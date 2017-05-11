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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.table.runtime.types.CRow

/**
  * Aggregate Function used for the groupby (without window) aggregate
  *
  * @param genAggregations      Generated aggregate helper function
  * @param aggregationStateType The row type info of aggregation
  */
class GroupAggProcessFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val aggregationStateType: RowTypeInfo,
    private val generateRetraction: Boolean,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations] {

  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
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
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val input = inputC.row

    // get accumulators and input counter
    var accumulators = state.value()
    var inputCnt = cntState.value()

    if (null == accumulators) {
      firstRow = true
      accumulators = function.createAccumulators()
      inputCnt = 0L
    } else {
      firstRow = false
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

      // if this was not the first row and we have to emit retractions
      if (generateRetraction && !firstRow) {
        if (prevRow.row.equals(newRow.row)) {
          // newRow is the same as before. Do not emit retraction and acc messages
          return
        } else {
          // retract previous result
          out.collect(prevRow)
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
      ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {
    cleanupStateOnTimer(timestamp, state, cntState)
  }

}
