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
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Process Function used for the aggregate in
  * [[org.apache.flink.streaming.api.datastream.DataStream]]
  *
  * @param aggregates the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                   used for this aggregation
  * @param aggFields  the position (in the input Row) of the input value for each aggregate
  */
class UnboundedNonPartitionedProcessingOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Array[Int]],
    private val forwardedFieldCount: Int,
    private val aggregationStateType: RowTypeInfo)
  extends ProcessFunction[Row, Row] with CheckpointedFunction{

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var accumulators: Row = _
  private var output: Row = _
  private var state: ListState[Row] = null

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    if (null == accumulators) {
      val it = state.get().iterator()
      if (it.hasNext) {
        accumulators = it.next()
      } else {
        accumulators = new Row(aggregates.length)
        var i = 0
        while (i < aggregates.length) {
          accumulators.setField(i, aggregates(i).createAccumulator())
          i += 1
        }
      }
    }
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    var i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)(0)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }

    out.collect(output)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    if (null != accumulators) {
      state.add(accumulators)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val accumulatorsDescriptor = new ListStateDescriptor[Row]("overState", aggregationStateType)
    state = context.getOperatorStateStore.getOperatorState(accumulatorsDescriptor)
  }
}
