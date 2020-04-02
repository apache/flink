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

import java.lang.{Long => JLong}

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Aggregate Function used for the groupby (without window) aggregate
  *
  * @param genAggregations      Generated aggregate helper function
  * @param aggregationStateType The row type info of aggregation
  */
class GroupAggProcessFunction[K](
    private val genAggregations: GeneratedAggregationsFunction,
    private val aggregationStateType: RowTypeInfo,
    private val generateRetraction: Boolean,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[K, CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations]
    with Logging {

  private var function: GeneratedAggregations = _

  private var newRow: CRow = _
  private var prevRow: CRow = _
  private var firstRow: Boolean = _
  // stores the accumulators
  private var state: ValueState[Row] = _
  // counts the number of added and retracted input records
  private var cntState: ValueState[JLong] = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
      s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()
    function.open(getRuntimeContext)

    newRow = new CRow(function.createOutputRow(), true)
    prevRow = new CRow(function.createOutputRow(), false)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("GroupAggregateState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
    val inputCntDescriptor: ValueStateDescriptor[JLong] =
      new ValueStateDescriptor[JLong]("GroupAggregateInputCounter", Types.LONG)
    cntState = getRuntimeContext.getState(inputCntDescriptor)

    initCleanupTimeState("GroupAggregateCleanupTime")
  }

  override def processElement(
      inputC: CRow,
      ctx: KeyedProcessFunction[K, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    processCleanupTimer(ctx, currentTime)

    val input = inputC.row

    // get accumulators and input counter
    var accumulators = state.value()
    var inputCnt = cntState.value()

    if (null == accumulators) {
      // Don't create a new accumulator for a retraction message. This
      // might happen if the retraction message is the first message for the
      // key or after a state clean up.
      if (!inputC.change) {
        return
      }
      // first accumulate message
      firstRow = true
      accumulators = function.createAccumulators()
    } else {
      firstRow = false
    }

    if (null == inputCnt) {
      inputCnt = 0L
    }

    // Set group keys value to the final output
    function.setForwardedFields(input, newRow.row)
    function.setForwardedFields(input, prevRow.row)

    // Set previous aggregate result to the prevRow
    function.setAggregationResults(accumulators, prevRow.row)

    // update aggregate result and set to the newRow
    if (inputC.change) {
      inputCnt += 1
      // accumulate input
      function.accumulate(accumulators, input)
      function.setAggregationResults(accumulators, newRow.row)
    } else {
      inputCnt -= 1
      // retract input
      function.retract(accumulators, input)
      function.setAggregationResults(accumulators, newRow.row)
    }

    if (inputCnt != 0) {
      // we aggregated at least one record for this key

      // update the state
      state.update(accumulators)
      cntState.update(inputCnt)

      // if this was not the first row
      if (!firstRow) {
        if (prevRow.row.equals(newRow.row) && !stateCleaningEnabled) {
          // newRow is the same as before and state cleaning is not enabled.
          // We emit nothing
          // If state cleaning is enabled, we have to emit messages to prevent too early
          // state eviction of downstream operators.
          return
        } else {
          // retract previous result
          if (generateRetraction) {
            out.collect(prevRow)
          }
        }
      }
      // emit the new result
      out.collect(newRow)

    } else {
      // we retracted the last record for this key
      // sent out a delete message
      out.collect(prevRow)
      // and clear all state
      state.clear()
      cntState.clear()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[K, CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled) {
      cleanupState(state, cntState)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    if (function != null) {
      function.close()
    }
  }
}
