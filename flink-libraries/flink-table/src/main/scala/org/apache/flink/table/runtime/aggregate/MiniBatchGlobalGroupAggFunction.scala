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
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.types.{DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.codegen.{EqualiserCodeGenerator, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.util.{BaseRowUtil, BinaryRowUtil}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, BundleFunction, ExecutionContext}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * Class of Aggregate Function used for the global groupby (without window) aggregate
  * in miniBatch mode.
  *
  * @param accTypes        the accumulator types
  * @param aggValueTypes   the aggregate value types
  * @param inputCountIndex None when the input not contains retraction.
  *                        Some when input contains retraction and
  *                        the index represents the count1 agg index.
  * @param generateRetraction whether this operator will generate retraction
  */
class MiniBatchGlobalGroupAggFunction(
    genLocalAggsHandler: GeneratedAggsHandleFunction,
    genGlobalAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Array[InternalType],
    aggValueTypes: Array[InternalType],
    inputCountIndex: Option[Int],
    generateRetraction: Boolean,
    groupWithoutKey: Boolean)
  extends BundleFunction[BaseRow, BaseRow, BaseRow, BaseRow]
  with Logging {

  protected var localAgg: AggsHandleFunction = _
  protected var globalAgg: AggsHandleFunction = _

  // stores the accumulators
  protected var accState: KeyedValueState[BaseRow, BaseRow] = _

  private val inputCounter: InputCounter = InputCounter(inputCountIndex)

  protected var newAggValue: BaseRow = _
  protected var prevAggValue: BaseRow = _

  protected var resultRow: JoinedRow = _

  @transient
  private var equaliser: RecordEqualiser = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling AggsHandleFunction: ${genLocalAggsHandler.name} \n\n " +
                s"Code:\n${genLocalAggsHandler.code}")
    localAgg = genLocalAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    localAgg.open(ctx)

    LOG.debug(s"Compiling AggsHandleFunction: ${genGlobalAggsHandler.name} \n\n " +
                s"Code:\n${genGlobalAggsHandler.code}")
    globalAgg = genGlobalAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    globalAgg.open(ctx)

    // serialize as GenericRow, deserialize as BinaryRow
    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val accDesc = new ValueStateDescriptor("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accDesc)

    val generator = new EqualiserCodeGenerator(aggValueTypes)
    val generatedEqualiser = generator.generateRecordEqualiser("GroupAggValueEqualiser")
    equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)

    resultRow = new JoinedRow()
  }

  override def close(): Unit = {
    if (localAgg != null) {
      localAgg.close()
    }
    if (globalAgg != null) {
      globalAgg.close()
    }
  }

  // value is accumulator, but input is <key, accumulator>, so need to project input to acc
  override def addInput(value: BaseRow, input: BaseRow): BaseRow = {
    val acc = if (value == null) {
      localAgg.createAccumulators()
    } else {
      value
    }

    localAgg.setAccumulators(acc)
    localAgg.merge(input)
    localAgg.getAccumulators
  }

  override def finishBundle(
    buffer: JMap[BaseRow, BaseRow],
    out: Collector[BaseRow]): Unit = {

    // batch get to cache
    val accMap = accState.getAll(buffer.keySet())

    val iter = buffer.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val currentAcc = entry.getValue
      val currentKey = entry.getKey
      // set current key to make dataview know current key
      ctx.setCurrentKey(currentKey)

      var firstRow = false

      // get acc from cache
      var acc = accMap.get(currentKey)
      if (acc == null) {
        acc = globalAgg.createAccumulators()
        firstRow = true
      }
      // set accumulator first
      globalAgg.setAccumulators(acc)
      // get previous aggregate result
      prevAggValue = globalAgg.getValue

      // merge currentAcc to acc
      globalAgg.merge(currentAcc)
      // get current aggregate result
      newAggValue = globalAgg.getValue
      // get new accumulator
      acc = globalAgg.getAccumulators

      if (!inputCounter.countIsZero(acc)) {
        // we aggregated at least one record for this key

        // update new acc to the cache, batch put to state later
        entry.setValue(acc)

        // if this was not the first row and we have to emit retractions
        if (!firstRow) {
          if (!equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
            // new row is not same with prev row
            if (generateRetraction) {
              out.collect(prevResultRow(currentKey))
            }
            out.collect(newResultRow(currentKey))
          } else {
            // new row is same with prev row, no need to output
          }
        } else {
          // this is the first, output new result
          out.collect(newResultRow(currentKey))
        }

      } else {
        // we retracted the last record for this key
        // sent out a delete message
        if (!firstRow) {
          out.collect(prevResultRow(currentKey))
        }
        // and clear all state, remove directly not removeAll because removeAll calls remove
        accState.remove(currentKey)
        // remove the entry, in order to avoid batch put to state later
        iter.remove()
        // cleanup dataview under current key
        globalAgg.cleanup()
      }
    }

    // batch put to state
    if (!buffer.isEmpty) {
      accState.putAll(buffer)
    }
  }

  protected def newResultRow(key: BaseRow): BaseRow = {
    resultRow.replace(key, newAggValue)
    BaseRowUtil.setAccumulate(resultRow)
    resultRow
  }

  protected def prevResultRow(key: BaseRow): BaseRow = {
    resultRow.replace(key, prevAggValue)
    BaseRowUtil.setRetract(resultRow)
    resultRow
  }

  override def endInput(out: Collector[BaseRow]): Unit = {
    // output default value if grouping without key and it's an empty group
    if (groupWithoutKey) {
      ctx.setCurrentKey(BinaryRowUtil.EMPTY_ROW)
      val accumulators = accState.get(BinaryRowUtil.EMPTY_ROW)
      if (inputCounter.countIsZero(accumulators)) {
        globalAgg.setAccumulators(globalAgg.createAccumulators)
        newAggValue = globalAgg.getValue
        out.collect(newResultRow(BinaryRowUtil.EMPTY_ROW))
      }
      // no need to update acc state, because this is the end
    }
  }
}
