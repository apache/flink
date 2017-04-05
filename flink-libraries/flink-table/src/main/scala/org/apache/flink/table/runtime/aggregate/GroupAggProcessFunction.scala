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
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * Aggregate Function used for the groupby (without window) aggregate
  *
  * @param aggregates           the list of all
  *                             [[org.apache.flink.table.functions.AggregateFunction]] used for
  *                             this aggregation
  * @param aggFields            the position (in the input Row) of the input value for each
  *                             aggregate
  * @param groupings            the position (in the input Row) of the grouping keys
  * @param aggregationStateType the row type info of aggregation
  */
class GroupAggProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Array[Int]],
    private val groupings: Array[Int],
    private val aggregationStateType: RowTypeInfo)
  extends ProcessFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var state: ValueState[Row] = _

  override def open(config: Configuration) {
    output = new Row(groupings.length + aggregates.length)
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("GroupAggregateState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
      input: Row,
      ctx: ProcessFunction[Row, Row]#Context,
      out: Collector[Row]): Unit = {

    var i = 0

    var accumulators = state.value()

    if (null == accumulators) {
      accumulators = new Row(aggregates.length)
      i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    // Set group keys value to the final output
    i = 0
    while (i < groupings.length) {
      output.setField(i, input.getField(groupings(i)))
      i += 1
    }

    // Set aggregate result to the final output
    i = 0
    while (i < aggregates.length) {
      val index = groupings.length + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)(0)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }
    state.update(accumulators)

    out.collect(output)
  }

}
