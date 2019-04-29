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
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Aggregate Function used for the groupby (without window) table aggregate.
  *
  * @param genTableAggregations Generated aggregate helper function
  * @param aggregationStateType The row type info of aggregation
  */
class GroupTableAggProcessFunction[K](
    private val genTableAggregations: GeneratedAggregationsFunction,
    private val aggregationStateType: RowTypeInfo,
    private val generateRetraction: Boolean,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[K, CRow, CRow](queryConfig)
    with Compiler[GeneratedTableAggregations]
    with Logging {

  private var function: GeneratedTableAggregations = _

  private var firstRow: Boolean = _
  // stores the accumulators
  private var state: ValueState[Row] = _
  // counts the number of added and retracted input records
  private var cntState: ValueState[JLong] = _

  private var appendKeyCollector: AppendKeyCRowCollector = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling TableAggregateHelper: ${genTableAggregations.name} \n\n " +
      s"Code:\n${genTableAggregations.code}")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genTableAggregations.name,
      genTableAggregations.code)
    LOG.debug("Instantiating TableAggregateHelper.")
    function = clazz.newInstance()
    function.open(getRuntimeContext)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("GroupTableAggregateState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
    val inputCntDescriptor: ValueStateDescriptor[JLong] =
      new ValueStateDescriptor[JLong]("GroupTableAggregateInputCounter", Types.LONG)
    cntState = getRuntimeContext.getState(inputCntDescriptor)

    appendKeyCollector = new AppendKeyCRowCollector
    appendKeyCollector.setResultRow(function.createOutputRow())

    initCleanupTimeState("GroupTableAggregateCleanupTime")
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

    // Set group keys value to the final output
    function.setForwardedFields(input, appendKeyCollector.getResultRow)

    appendKeyCollector.out = out
    if (!firstRow) {
      if (generateRetraction) {
        appendKeyCollector.setChange(false)
        function.emit(accumulators, appendKeyCollector)
        appendKeyCollector.setChange(true)
      }
    }

    if (null == inputCnt) {
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

    if (inputCnt != 0) {
      // we aggregated at least one record for this key

      // update the state
      state.update(accumulators)
      cntState.update(inputCnt)

      // emit the new result
      function.emit(accumulators, appendKeyCollector)

    } else {
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
    function.close()
  }
}

/**
  * The collector is used to assemble group key and table function output.
  */
class AppendKeyCRowCollector() extends CRowWrappingCollector {

  var resultRow: Row = _

  def setResultRow(row: Row): Unit = {
    resultRow = row
  }

  def getResultRow: Row = {
    resultRow
  }

  override def collect(record: Row): Unit = {
    var i = 0
    val offset = resultRow.getArity - record.getArity
    while (i < record.getArity) {
      resultRow.setField(i + offset, record.getField(i))
      i += 1
    }
    super.collect(resultRow)
  }
}
