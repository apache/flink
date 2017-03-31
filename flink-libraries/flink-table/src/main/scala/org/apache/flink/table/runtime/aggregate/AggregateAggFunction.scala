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
import org.apache.flink.api.common.functions.{AggregateFunction => DataStreamAggFunc}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * Aggregate Function used for the aggregate operator in
  * [[org.apache.flink.streaming.api.datastream.WindowedStream]]
  *
  * @param aggregates       the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                         used for this aggregation
  * @param aggFields   the position (in the input Row) of the input value for each aggregate
  */
class AggregateAggFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Array[Int]])
  extends DataStreamAggFunc[Row, Row, Row] {

  override def createAccumulator(): Row = {
    val accumulatorRow: Row = new Row(aggregates.length)
    var i = 0
    while (i < aggregates.length) {
      accumulatorRow.setField(i, aggregates(i).createAccumulator())
      i += 1
    }
    accumulatorRow
  }

  override def add(value: Row, accumulatorRow: Row): Unit = {

    var i = 0
    while (i < aggregates.length) {
      val acc = accumulatorRow.getField(i).asInstanceOf[Accumulator]
      val v = value.getField(aggFields(i)(0))
      aggregates(i).accumulate(acc, v)
      i += 1
    }
  }

  override def getResult(accumulatorRow: Row): Row = {
    val output = new Row(aggFields.length)

    var i = 0
    while (i < aggregates.length) {
      val acc = accumulatorRow.getField(i).asInstanceOf[Accumulator]
      output.setField(i, aggregates(i).getValue(acc))
      i += 1
    }
    output
  }

  override def merge(aAccumulatorRow: Row, bAccumulatorRow: Row): Row = {

    var i = 0
    while (i < aggregates.length) {
      val aAcc = aAccumulatorRow.getField(i).asInstanceOf[Accumulator]
      val bAcc = bAccumulatorRow.getField(i).asInstanceOf[Accumulator]
      val accumulators: JList[Accumulator] = new JArrayList[Accumulator]()
      accumulators.add(aAcc)
      accumulators.add(bAcc)
      aAccumulatorRow.setField(i, aggregates(i).merge(accumulators))
      i += 1
    }
    aAccumulatorRow
  }
}
