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

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.slf4j.LoggerFactory

/**
  * Process Function for non-partitioned processing-time unbounded OVER window
  *
  * @param genAggregations Generated aggregate helper function
  * @param aggregationStateType     row type info of aggregation
  */
class ProcTimeUnboundedNonPartitionedOver(
    genAggregations: GeneratedAggregationsFunction,
    aggregationStateType: CRowTypeInfo)
  extends ProcessFunction[CRow, CRow]
    with CheckpointedFunction
    with Compiler[GeneratedAggregations] {

  private var accumulators: CRow = _
  private var output: CRow = _
  private var state: ListState[CRow] = _
  val LOG = LoggerFactory.getLogger(this.getClass)

  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = function.createOutputRow()
    if (null == accumulators) {
      val it = state.get().iterator()
      if (it.hasNext) {
        accumulators = it.next()
      } else {
        accumulators = new CRow(function.createAccumulators(), true)
      }
    }
  }

  override def processElement(
      input: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    function.setForwardedFields(input, output)

    function.accumulate(accumulators.row, input)
    function.setAggregationResults(accumulators.row, output)

    out.collect(output)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    if (null != accumulators) {
      state.add(accumulators)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val accumulatorsDescriptor = new ListStateDescriptor[CRow]("overState", aggregationStateType)
    state = context.getOperatorStateStore.getOperatorState(accumulatorsDescriptor)
  }
}
