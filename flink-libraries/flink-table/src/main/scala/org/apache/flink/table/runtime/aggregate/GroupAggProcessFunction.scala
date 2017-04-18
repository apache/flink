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
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.slf4j.LoggerFactory
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
    private val generateRetraction: Boolean)
  extends ProcessFunction[CRow, CRow]
    with Compiler[GeneratedAggregations] {

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  private var newRow: CRow = _
  private var prevRow: CRow = _
  private var firstRow: Boolean = _
  private var state: ValueState[Row] = _

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
  }

  override def processElement(
      inputC: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    val input = inputC.row

    // get accumulators
    var accumulators = state.value()
    if (null == accumulators) {
      firstRow = true
      accumulators = function.createAccumulators()
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
      // accumulate input
      function.accumulate(accumulators, input)
      function.setAggregationResults(accumulators, newRow.row)
    } else {
      // retract input
      function.retract(accumulators, input)
      function.setAggregationResults(accumulators, newRow.row)
    }

    // update accumulators
    state.update(accumulators)

    // if previousRow is not null, do retraction process
    if (generateRetraction && !firstRow) {
      if (prevRow.row.equals(newRow.row)) {
        // ignore same newRow
        return
      } else {
        // retract previous row
        out.collect(prevRow)
      }
    }

    out.collect(newRow)
  }
}
