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
import org.apache.flink.table.codegen.GeneratedAggsHandleFunction
import org.apache.flink.table.dataformat.{BaseRow, JoinedRow}
import org.apache.flink.table.runtime.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.table.runtime.functions.{AggsHandleFunction, ExecutionContext}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * Process Function for processing-time unbounded OVER window
  *
  * @param genAggsHandler      Generated aggregate helper function
  * @param accTypes            accumulator types of aggregation
  */
class ProcTimeUnboundedOver(
    genAggsHandler: GeneratedAggsHandleFunction,
    accTypes: Seq[InternalType],
    tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with Logging {

  private var function: AggsHandleFunction = _
  private var accState: KeyedValueState[BaseRow, BaseRow] = _
  private var output: JoinedRow = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    LOG.debug(s"Compiling AggregateHelper: ${genAggsHandler.name} \n\n" +
        s"Code:\n${genAggsHandler.code}")
    function = genAggsHandler.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
    function.open(ctx)

    output = new JoinedRow()

    val accTypeInfo = new BaseRowTypeInfo(
      accTypes.map(TypeConverters.createExternalTypeInfoFromDataType): _*)
    val stateDescriptor = new ValueStateDescriptor("accState", accTypeInfo)
    accState = ctx.getKeyedValueState(stateDescriptor)

    initCleanupTimeState("ProcTimeUnboundedOverCleanupTime")
  }

  override def processElement(
      input: BaseRow,
      ctx: Context,
      out: Collector[BaseRow]): Unit = {

    val currentKey = executionContext.currentKey()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, ctx.timerService().currentProcessingTime())

    var accumulators = accState.get(currentKey)
    if (null == accumulators) {
      accumulators = function.createAccumulators()
    }
    // set accumulators in context first
    function.setAccumulators(accumulators)

    // accumulate input row
    function.accumulate(input)

    // update the value of accumulators for future incremental computation
    accumulators = function.getAccumulators
    accState.put(currentKey, accumulators)

    // prepare output row
    val aggValue = function.getValue
    output.replace(input, aggValue)
    out.collect(output)
  }

  override def onTimer(
      timestamp: Long,
      ctx: OnTimerContext,
      out: Collector[BaseRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(accState)
      function.cleanup()
    }
  }

  override def close(): Unit = {
    if (null != function) {
      function.close()
    }
  }
}
