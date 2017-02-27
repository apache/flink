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

import java.util.{ArrayList => JArrayList, List => JList}
import org.apache.flink.api.common.functions.{AggregateFunction => ApiAggFunction}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * Aggregate Function used for the aggregate operator in
  * [[org.apache.flink.streaming.api.datastream.WindowedStream]]
  *
  * @param aggregates       the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                         used for this aggregation
  * @param aggFieldsIndex   the position (in the input Row) of the input value for each aggregate
  * @param aggregateMapping the list of the mapping of (the position of this aggregate result in the
  *                         output row => the index of the aggregate) for all the aggregates
  * @param groupKeysIndex   the position (in the input Row) of grouping keys
  * @param groupKeysMapping the list of mapping of (the position of the grouping key in the
  *                         output row => the index of grouping key) for all the grouping keys
  * @param finalRowArity    the arity of the final row
  */
class AggregateAggFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFieldsIndex: Array[Int],
    private val aggregateMapping: Array[(Int, Int)],
    private val groupKeysIndex: Array[Int],
    private val groupKeysMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends ApiAggFunction[Row, Row, Row] {

  override def createAccumulator(): Row = {
    val accumulatorRow: Row = new Row(groupKeysIndex.length + aggregates.length)

    for (i <- aggregates.indices) {
      accumulatorRow.setField(groupKeysIndex.length + i, aggregates(i).createAccumulator())
    }
    accumulatorRow
  }

  override def add(value: Row, accumulatorRow: Row) = {
    for (i <- groupKeysIndex.indices) {
      accumulatorRow.setField(i, value.getField(groupKeysIndex(i)))
    }

    for (i <- aggregates.indices) {
      val accumulator =
        accumulatorRow.getField(i + groupKeysIndex.length).asInstanceOf[Accumulator]
      val v = value.getField(aggFieldsIndex(i))
      aggregates(i).accumulate(accumulator, v)
    }
  }

  override def getResult(accumulatorRow: Row): Row = {
    val output = new Row(finalRowArity)

    groupKeysMapping.foreach {
      case (after, previous) =>
        output.setField(after, accumulatorRow.getField(previous))
    }

    aggregateMapping.foreach {
      case (after, previous) =>
        val accumulator =
          accumulatorRow.getField(previous + groupKeysIndex.length).asInstanceOf[Accumulator]
        output.setField(after, aggregates(previous).getValue(accumulator))
    }
    output
  }

  override def merge(aAccumulatorRow: Row, bAccumulatorRow: Row): Row = {
    for (i <- aggregates.indices) {
      val aAccum =
        aAccumulatorRow.getField(i + groupKeysIndex.length).asInstanceOf[Accumulator]
      val bAccum =
        bAccumulatorRow.getField(i + groupKeysIndex.length).asInstanceOf[Accumulator]
      val accumulators: JList[Accumulator] = new JArrayList[Accumulator]()
      accumulators.add(aAccum)
      accumulators.add(bAccum)
      aAccumulatorRow.setField(i + groupKeysIndex.length, aggregates(i).merge(accumulators))
    }
    aAccumulatorRow
  }
}
