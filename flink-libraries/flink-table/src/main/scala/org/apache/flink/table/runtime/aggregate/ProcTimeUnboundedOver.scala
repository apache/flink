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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.CRow
import org.slf4j.LoggerFactory

/**
  * Process Function for processing-time unbounded OVER window
  *
  * @param genAggregations Generated aggregate helper function
  * @param aggregationStateType     row type info of aggregation
  */
class ProcTimeUnboundedOver(
    genAggregations: GeneratedAggregationsFunction,
    aggregationStateType: RowTypeInfo,
    queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[CRow, CRow](queryConfig)
    with Compiler[GeneratedAggregations] {

  private var output: CRow = _
  private var state: ValueState[Row] = _
  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: ${genAggregations.name} \n\n" +
                s"Code:\n${genAggregations.code}")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = new CRow(function.createOutputRow(), true)
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)

    initCleanupTimeState("ProcTimeUnboundedPartitionedOverCleanupTime")
  }

  override def processElement(
    inputC: CRow,
    ctx: ProcessFunction[CRow, CRow]#Context,
    out: Collector[CRow]): Unit = {

    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    val input = inputC.row

    var accumulators = state.value()

    if (null == accumulators) {
      accumulators = function.createAccumulators()
    }

    function.setForwardedFields(input, output.row)

    function.accumulate(accumulators, input)
    function.setAggregationResults(accumulators, output.row)

    state.update(accumulators)
    out.collect(output)
  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[CRow, CRow]#OnTimerContext,
    out: Collector[CRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(state)
    }
  }
}
