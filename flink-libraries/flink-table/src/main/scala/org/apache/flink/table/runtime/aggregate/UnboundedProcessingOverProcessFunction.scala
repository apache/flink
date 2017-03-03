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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction.{Context, OnTimerContext}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.RichProcessFunction
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

class UnboundedProcessingOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val intermediateRowType: RowTypeInfo,
    private val returnType: TypeInformation[Row])
  extends RichProcessFunction[Row, Row]{

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var state: ValueState[Row] = _
  private val aggregateWithIndex: Array[(AggregateFunction[_], Int)] = aggregates.zipWithIndex

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    val stateSerializer: TypeSerializer[Row] =
      intermediateRowType.createSerializer(getRuntimeContext.getExecutionConfig)
    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", stateSerializer)
    state = getRuntimeContext.getState(stateDescriptor)
  }

  override def processElement(
    input: Row,
    ctx: Context,
    out: Collector[Row]): Unit = {

    var accumulators = state.value()

    if (null == accumulators) {
      accumulators = new Row(aggregates.length)
      aggregateWithIndex.foreach { case (agg, i) =>
        accumulators.setField(i, agg.createAccumulator())
      }
    }

    for (i <- 0 until forwardedFieldCount) {
      output.setField(i, input.getField(i))
    }

    for (i <- 0 until aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator];
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
      accumulators.setField(i, accumulator)
    }
    state.update(accumulators)

    out.collect(output)
  }

  override def onTimer(
    timestamp: Long,
    ctx: OnTimerContext,
    out: Collector[Row]): Unit = ??? // Implement this method if following is needed to be supported
}
