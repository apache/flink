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

import java.util.{Map => JMap}
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, BundleFunction, ExecutionContext}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * Class of Aggregate Function used for the local groupby (without window) aggregate in
  * miniBatch mode.
  *
  * @param genAggsHandler  the generated aggregate handler
  */
class MiniBatchLocalGroupAggFunction(genAggsHandler: GeneratedAggsHandleFunction)
  extends BundleFunction[BaseRow, BaseRow, BaseRow, BaseRow]
  with Logging {

  protected var function: AggsHandleFunction = _

  protected var resultRow: JoinedRow = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling AggsHandleFunction: ${genAggsHandler.name} \n\n " +
                s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    resultRow = new JoinedRow()
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }

  // value is accumulator, accumulate input to value
  override def addInput(value: BaseRow, input: BaseRow): BaseRow = {
    val acc = if (value == null) {
      function.createAccumulators()
    } else {
      value
    }
    function.setAccumulators(acc)
    if (BaseRowUtil.isAccumulateMsg(input)) {
      function.accumulate(input)
    } else {
      function.retract(input)
    }
    function.getAccumulators
  }

  // value is accumulator
  override def mergeValue(value1: BaseRow, value2: BaseRow): BaseRow = {
    if (value1 == null) {
      value2
    } else {
      function.setAccumulators(value1)
      function.merge(value2)
      function.getAccumulators
    }
  }

  override def finishBundle(
      buffer: JMap[BaseRow, BaseRow],
      out: Collector[BaseRow]): Unit = {
    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue
      resultRow.replace(key, value)
      out.collect(resultRow)
    }
    buffer.clear()
  }
}
