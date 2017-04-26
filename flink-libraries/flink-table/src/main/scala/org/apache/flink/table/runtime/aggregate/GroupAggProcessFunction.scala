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
import org.apache.flink.table.runtime.types.CRow

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
    private val aggregationStateType: RowTypeInfo,
    private val generateRetraction: Boolean)
  extends ProcessFunction[CRow, CRow] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var newRow: CRow = _
  private var prevRow: CRow = _
  private var firstRow: Boolean = _
  private var state: ValueState[Row] = _

  override def open(config: Configuration) {
    newRow = new CRow(new Row(groupings.length + aggregates.length), true)
    prevRow = new CRow(new Row(groupings.length + aggregates.length), false)
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("GroupAggregateState", aggregationStateType)
    state = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
      input: CRow,
      ctx: ProcessFunction[CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    var i = 0

    var accumulators = state.value()

    if (null == accumulators) {
      firstRow = true
      accumulators = new Row(aggregates.length)
      i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    } else {
      firstRow = false
    }

    // Set group keys value to the newRow and prevRow
    i = 0
    while (i < groupings.length) {
      newRow.row.setField(i, input.row.getField(groupings(i)))
      prevRow.row.setField(i, input.row.getField(groupings(i)))
      i += 1
    }

    // Set previous aggregate result to the prevRow
    // Set current aggregate result to the newRow
    if (input.change) {
      // accumulate input
      i = 0
      while (i < aggregates.length) {
        val index = groupings.length + i
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        prevRow.row.setField(index, aggregates(i).getValue(accumulator))
        aggregates(i).accumulate(accumulator, input.row.getField(aggFields(i)(0)))
        newRow.row.setField(index, aggregates(i).getValue(accumulator))
        i += 1
      }
    } else {
      // retract input
      i = 0
      while (i < aggregates.length) {
        val index = groupings.length + i
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        prevRow.row.setField(index, aggregates(i).getValue(accumulator))
        aggregates(i).retract(accumulator, input.row.getField(aggFields(i)(0)))
        newRow.row.setField(index, aggregates(i).getValue(accumulator))
        i += 1
      }
    }

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
