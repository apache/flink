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
package org.apache.flink.table.runtime

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Function used to convert upsert to retractions.
  *
  * @param rowTypeInfo the output row type info.
  * @param queryConfig the configuration for the query.
  */
class UpsertToRetractionProcessFunction(
    private val rowTypeInfo: RowTypeInfo,
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Logging {

  @transient private var prevRow: CRow = _
  // stores the accumulators
  @transient private var state: ValueState[Row] = _

  override def open(config: Configuration) {

    prevRow = new CRow(new Row(rowTypeInfo.getArity), false)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("UpsertToRetractionState", rowTypeInfo)
    state = getRuntimeContext.getState(stateDescriptor)

    initCleanupTimeState("UpsertToRetractionCleanupTime")
    LOG.info("Init UpsertToRetractionProcessFunction.")
  }

  override def processElement(
      inputC: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    processCleanupTimer(ctx, currentTime)

    val pre = state.value()
    val current = inputC.row

    if (inputC.change) {
      // ignore same record
      if (!stateCleaningEnabled && pre != null && pre.equals(current)) {
        return
      }
      state.update(current)
      // retract prevRow
      if (pre != null) {
        prevRow.row = pre
        out.collect(prevRow)
      }
      // output currentRow
      out.collect(inputC)
    } else {
      state.clear()
      if (pre != null) {
        prevRow.row = pre
        out.collect(prevRow)
      } else {
        // else input is a delete row we ignore it, since delete on nothing means nothing.
      }
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
      out: Collector[CRow]): Unit = {

    if (stateCleaningEnabled) {
      cleanupState(state)
    }
  }
}
