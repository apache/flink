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

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.codegen.{EqualiserCodeGenerator, GeneratedAggsHandleFunction}
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext, ProcessFunction}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.Logging
import org.apache.flink.table.dataformat.util.BaseRowUtil.isAccumulateMsg
import org.apache.flink.table.dataformat.util.{BaseRowUtil, BinaryRowUtil}
import org.apache.flink.util.Collector

/**
  * Class of Aggregate Function used for the groupby (without window) aggregate
  * @param genAggsHandler  the generated aggregate handler
  * @param accTypes        the accumulator types
  * @param aggValueTypes   the aggregate value types
  * @param inputCountIndex None when the input not contains retraction.
  *                        Some when input contains retraction and
  *                        the index represents the count1 agg index.
  * @param generateRetraction whether this operator will generate retraction
  */
class GroupAggFunction(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Array[InternalType],
    aggValueTypes: Array[InternalType],
    inputCountIndex: Option[Int],
    generateRetraction: Boolean,
    groupWithoutKey: Boolean,
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  protected var function: AggsHandleFunction = _

  // stores the accumulators
  protected var accState: KeyedValueState[BaseRow, BaseRow] = _

  private val inputCounter: InputCounter = InputCounter(inputCountIndex)

  protected var newAggValue: BaseRow = _
  protected var prevAggValue: BaseRow = _
  protected var firstRow: Boolean = _

  protected var resultRow: JoinedRow = _

  @transient
  private var equaliser: RecordEqualiser = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling AggsHandleFunction: ${genAggsHandler.name} \n\n " +
      s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    // serialize as GenericRow, deserialize as BinaryRow
    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val accDesc = new ValueStateDescriptor("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(accDesc)

    val generator = new EqualiserCodeGenerator(aggValueTypes)
    val generatedEqualiser = generator.generateRecordEqualiser("GroupAggValueEqualiser")
    equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)

    initCleanupTimeState("GroupAggregateCleanupTime")

    resultRow = new JoinedRow()
  }

  override def processElement(
      input: BaseRow,
      ctx: ProcessFunction.Context,
      out: Collector[BaseRow]): Unit = {
    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val currentKey = executionContext.currentKey()

    var accumulators = accState.get(currentKey)
    if (null == accumulators) {
      firstRow = true
      accumulators = function.createAccumulators()
    } else {
      firstRow = false
    }

    // set accumulators to handler first
    function.setAccumulators(accumulators)
    // get previous aggregate result
    prevAggValue = function.getValue

    // update aggregate result and set to the newRow
    if (isAccumulateMsg(input)) {
      // accumulate input
      function.accumulate(input)
    } else {
      // retract input
      function.retract(input)
    }
    // get current aggregate result
    newAggValue = function.getValue

    // get accumulator
    accumulators = function.getAccumulators

    if (!inputCounter.countIsZero(accumulators)) {
      // we aggregated at least one record for this key

      // update the state
      accState.put(currentKey, accumulators)

      // if this was not the first row and we have to emit retractions
      if (!firstRow) {
        if (!stateCleaningEnabled && equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
          // newRow is the same as before and state cleaning is not enabled.
          // We do not emit retraction and acc message.
          // If state cleaning is enabled, we have to emit messages to prevent too early
          // state eviction of downstream operators.
          return
        } else {
          // retract previous result
          if (generateRetraction) {
            out.collect(prevResultRow(currentKey))
          }
        }
      }
      // emit the new result
      out.collect(newResultRow(currentKey))

    } else {
      // we retracted the last record for this key
      // sent out a delete message
      if (!firstRow) {
        out.collect(prevResultRow(currentKey))
      }
      // and clear all state
      accState.remove(currentKey)
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

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction.OnTimerContext,
      out: Collector[BaseRow]): Unit = {
    if (needToCleanupState(timestamp)) {
      cleanupState(accState)
      function.cleanup()
    }
  }

  override def endInput(out: Collector[BaseRow]): Unit = {
    // output default value if grouping without key and it's an empty group
    if (groupWithoutKey) {
      executionContext.setCurrentKey(BinaryRowUtil.EMPTY_ROW)
      val accumulators = accState.get(BinaryRowUtil.EMPTY_ROW)
      if (inputCounter.countIsZero(accumulators)) {
        function.setAccumulators(function.createAccumulators)
        newAggValue = function.getValue
        out.collect(newResultRow(BinaryRowUtil.EMPTY_ROW))
      }
      // no need to update acc state, because this is the end
    }
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }
}
