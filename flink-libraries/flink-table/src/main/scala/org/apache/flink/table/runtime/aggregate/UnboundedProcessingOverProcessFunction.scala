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
import org.apache.flink.table.codegen.{AggregateHelperFunction, Compiler}
import org.slf4j.LoggerFactory

/**
  * Process Function for processing-time unbounded OVER window
  *
  * @param GeneratedAggregateHelper Generated aggregate helper function
  * @param forwardedFieldCount      count of forwarded fields.
  * @param aggregationStateType     row type info of aggregation
  */
class UnboundedProcessingOverProcessFunction(
    GeneratedAggregateHelper: AggregateHelperFunction,
    forwardedFieldCount: Int,
    aggregationStateType: RowTypeInfo)
  extends ProcessFunction[Row, Row]
    with Compiler[AggregateHelper] {

  private var output: Row = _
  private var state: ValueState[Row] = _
  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: AggregateHelper = _
  val NumOfAggregates = aggregationStateType.getFieldNames.length

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $GeneratedAggregateHelper.name \n\n " +
                s"Code:\n$GeneratedAggregateHelper.code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader,
                        GeneratedAggregateHelper.name,
                        GeneratedAggregateHelper.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = new Row(forwardedFieldCount + NumOfAggregates)
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    var accumulators = state.value()

    if (null == accumulators) {
      accumulators = function.createAccumulator()
    }

    function.forwardValueToOutput(input, output)

    function.accumulate(accumulators, input)
    function.setOutput(accumulators, output, forwardedFieldCount)

    state.update(accumulators)

    out.collect(output)
  }

}
