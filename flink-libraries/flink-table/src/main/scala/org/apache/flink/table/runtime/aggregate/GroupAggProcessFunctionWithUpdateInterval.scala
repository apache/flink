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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
  * Aggregate Function used for the groupby (without window) aggregate
  * with update interval config.
  *
  * @param genAggregations      Generated aggregate helper function
  * @param aggregationStateType The row type info of aggregation
  * @param outputRowType The row type info of output.
  */
class GroupAggProcessFunctionWithUpdateInterval(
    private val genAggregations: GeneratedAggregationsFunction,
    private val aggregationStateType: RowTypeInfo,
    private val outputRowType: RowTypeInfo,
    private val generateRetraction: Boolean,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
with Compiler[GeneratedAggregations] {

  protected val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  protected var function: GeneratedAggregations = _

  private var emitRow: Row = _
  protected var newRow: CRow = _
  protected var prevRow: CRow = _

  private var typeSerializer: TypeSerializer[Row] = _

  // stores the accumulators
  protected var state: ValueState[Row] = _

  // counts the number of added and retracted input records
  protected var cntState: ValueState[JLong] = _

  // stores the last emit row
  private var preEmitState: ValueState[Row] = _

  // stores the current emit row
  private var emitState: ValueState[Row] = _

  // stores the emit time
  private var emitTimerState: ValueState[JLong] = _


  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    emitRow = function.createOutputRow
    newRow = new CRow(function.createOutputRow, true)
    prevRow = new CRow(function.createOutputRow, false)
    typeSerializer = outputRowType.createSerializer(new ExecutionConfig())

    state = getRuntimeContext.getState(
      new ValueStateDescriptor[Row]("GroupAggregateState", aggregationStateType))
    cntState = getRuntimeContext.getState(
      new ValueStateDescriptor[JLong]("GroupAggregateInputCounter", Types.LONG))
    preEmitState = getRuntimeContext.getState(
      new ValueStateDescriptor[Row]("GroupAggregatePreEmitState", outputRowType))
    emitState = getRuntimeContext.getState(
      new ValueStateDescriptor[Row]("GroupAggregateEmitState", outputRowType))
    emitTimerState = getRuntimeContext.getState(
      new ValueStateDescriptor[JLong]("emitTimeState", Types.LONG))

    initCleanupTimeState("GroupAggregateWithUpdateIntervalCleanupTime")
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
    var accumulators = state.value
    var inputCnt = cntState.value

    if (null == accumulators) {
      accumulators = function.createAccumulators()
      inputCnt = 0L
    }

    // update aggregate result and set to the newRow
    if (inputC.change) {
      inputCnt += 1
      // accumulate input
      function.accumulate(accumulators, input)
    } else {
      inputCnt -= 1
      // retract input
      function.retract(accumulators, input)
    }

    state.update(accumulators)
    cntState.update(inputCnt)

    var triggerTimer = emitTimerState.value

    if (null == triggerTimer) {
      triggerTimer = 0L
    }

    if (0 == triggerTimer || currentTime >= triggerTimer) {

      // set group keys value to the final output
      function.setForwardedFields(input, emitRow)
      // set previous aggregate result to the prevRow
      function.setAggregationResults(accumulators, emitRow)
      // store emit row
      emitState.update(typeSerializer.copy(emitRow))

      // emit the first row for each key
      if (0 == triggerTimer) {
        ctx.timerService().registerProcessingTimeTimer(currentTime)
      }

      val newTimer = if (currentTime > triggerTimer) {
        currentTime + queryConfig.getUnboundedAggregateUpdateInterval
      } else {
        triggerTimer + queryConfig.getUnboundedAggregateUpdateInterval
      }

      emitTimerState.update(newTimer)

      ctx.timerService().registerProcessingTimeTimer(newTimer)
    }

  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(state, cntState, preEmitState, emitTimerState, emitState)
    } else {
      newRow.row = emitState.value
      val preEmit = preEmitState.value

      if (null != preEmit) {
        prevRow.row = preEmit
      }

      val inputCnt = cntState.value
      if (!prevRow.row.equals(newRow.row)) {
        // emit the data
        if (inputCnt != 0) {
          // we aggregated at least one record for this key
          // if this was not the first row and we have to emit retractions
          if (generateRetraction && null != preEmit) {
            // retract previous result
            out.collect(prevRow)
          }
          // emit the new result
          out.collect(newRow)
          // update preState
          preEmitState.update(typeSerializer.copy(newRow.row))
        } else {
          if (null != preEmit) {
            // we retracted the last record for this key
            // sent out a delete message
            out.collect(prevRow)
          }

          // and clear all state
          cleanupState(state, cntState, preEmitState, emitTimerState, emitState)
        }
      }
    }
  }
}
