/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.runtime.aggregate

import java.util
import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.function.{Function => JFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, BundleFunction, ExecutionContext}
import org.apache.flink.util.Collector

class MiniBatchIncrementalGroupAggFunction(
  genPartialAggsHandler: GeneratedAggsHandleFunction,
  genFinalAggsHandler: GeneratedAggsHandleFunction,
  groupKeySelector: KeySelector[BaseRow, BaseRow])
  extends BundleFunction[BaseRow, BaseRow, BaseRow, BaseRow] {

  private var partialFunction: AggsHandleFunction = _
  private var finalFunction: AggsHandleFunction = _

  private var resultRow: JoinedRow = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    val classLoader = ctx.getRuntimeContext.getUserCodeClassLoader
    partialFunction = genPartialAggsHandler.newInstance(classLoader)
    partialFunction.open(ctx)
    finalFunction = genFinalAggsHandler.newInstance(classLoader)
    finalFunction.open(ctx)

    resultRow = new JoinedRow()
  }

  override def close(): Unit = {
    if (partialFunction != null) {
      partialFunction.close()
    }
    if (finalFunction != null) {
      finalFunction.close()
    }
  }

  override def addInput(value: BaseRow, input: BaseRow): BaseRow = {
    val acc = if (value == null) {
      partialFunction.createAccumulators()
    } else {
      value
    }

    partialFunction.setAccumulators(acc)
    partialFunction.merge(input)
    partialFunction.getAccumulators
  }


  override def finishBundle(
      buffer: util.Map[BaseRow, BaseRow],
      out: Collector[BaseRow]): Unit = {

    // groupKey -> shuffleKey -> Accumulators
    val finalAggBuffer = new JHashMap[BaseRow, JMap[BaseRow, BaseRow]]()
    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val currentAcc = entry.getValue
      val shuffleKey = entry.getKey
      val groupKey = groupKeySelector.getKey(shuffleKey)
      // use compute to avoid additional put
      val accMap = finalAggBuffer.computeIfAbsent(
        groupKey,
        new JFunction[BaseRow, JMap[BaseRow, BaseRow]]() {
          override def apply(t: BaseRow): JMap[BaseRow, BaseRow] = {
            new JHashMap[BaseRow, BaseRow]()
          }
      })
      accMap.put(shuffleKey, currentAcc)
    }

    val finalAggBufferIter = finalAggBuffer.entrySet().iterator()
    while (finalAggBufferIter.hasNext) {
      val entry = finalAggBufferIter.next()
      val groupKey = entry.getKey
      val accMap = entry.getValue
      val accIter = accMap.entrySet().iterator()
      // set accumulators to initial value
      finalFunction.resetAccumulators()
      while (accIter.hasNext) {
        val accEntry = accIter.next()
        val currentAcc = accEntry.getValue
        // partial agg key
        val shuffleKey = accEntry.getKey
        // set current key to make dataview know current key
        ctx.setCurrentKey(shuffleKey)
        finalFunction.merge(currentAcc)
      }
      val accResult = finalFunction.getAccumulators
      resultRow.replace(groupKey, accResult)
      out.collect(resultRow)
    }
    // for gc friendly
    finalAggBuffer.clear()
  }
}
