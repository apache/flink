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
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Table Aggregate Function used for the groupby (without window) aggregate
  *
  * @param genAggregations      Generated table aggregate helper function
  * @param aggregationStateType The row type info of table aggregation
  */
class TableAggProcessFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val aggregationStateType: RowTypeInfo,
    private val generateRetraction: Boolean,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Compiler[GeneratedTableAggregations]
    with Logging {

  private var function: GeneratedTableAggregations = _
  // stores the accumulators
  private var state: ValueState[Row] = _
  // counts the number of added and retracted input records
  private var cntState: ValueState[JLong] = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling TableAggregateHelper: $genAggregations.name \n\n " +
      s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating TableAggregateHelper.")
    function = clazz.newInstance()
    function.open(getRuntimeContext)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("TableAggregateState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
    val inputCntDescriptor: ValueStateDescriptor[JLong] =
      new ValueStateDescriptor[JLong]("TableAggregateInputCounter", Types.LONG)
    cntState = getRuntimeContext.getState(inputCntDescriptor)

    initCleanupTimeState("TableAggregateCleanupTime")
  }

  override def processElement(
      inputC: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
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
      accumulators = function.createAccumulators()
    }

    if (null == inputCnt) {
      inputCnt = 0L
    }

    if (inputC.change) {
      inputCnt += 1
      // accumulate input
      function.accumulate(accumulators, input)
    } else {
      inputCnt -= 1
      // retract input
      function.retract(accumulators, input)
    }

    function.emit(accumulators, out)

    if (inputCnt == 0) {
      // and clear all state
      state.clear()
      cntState.clear()
    } else {
      // update the state
      state.update(accumulators)
      cntState.update(inputCnt)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled) {
      cleanupState(state, cntState)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    function.close()
  }
}
