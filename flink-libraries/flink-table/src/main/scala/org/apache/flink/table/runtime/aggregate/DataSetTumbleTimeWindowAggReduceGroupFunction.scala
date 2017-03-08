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

import java.lang.Iterable
import java.util.{ArrayList => JArrayList}

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]]. It is used for tumbling time-window
  * on batch.
  *
  * @param windowSize       Tumbling time window size
  * @param windowStartPos   The relative window-start field position to the last field of output row
  * @param windowEndPos     The relative window-end field position to the last field of output row
  * @param aggregates       The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity    The output row field count
  */
class DataSetTumbleTimeWindowAggReduceGroupFunction(
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(groupKeysMapping)

  private var collector: TimeWindowPropertyCollector = _
  protected var aggregateBuffer: Row = _
  private var output: Row = _
  private val accumStartPos: Int = groupKeysMapping.length
  private val rowtimePos: Int = accumStartPos + aggregates.length
  private val intermediateRowArity: Int = rowtimePos + 1


  val accumulatorList: Array[JArrayList[Accumulator]] = Array.fill(aggregates.length) {
    new JArrayList[Accumulator](2)
  }

  override def open(config: Configuration) {
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(windowStartPos, windowEndPos)

    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var last: Row = null
    val iterator = records.iterator()

    // reset first accumulator in merge list
    var i = 0
    while (i < aggregates.length) {
      aggregates(i).resetAccumulator(accumulatorList(i).get(0))
      i += 1
    }

    while (iterator.hasNext) {
      val record = iterator.next()

      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(groupKeysMapping.length + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
        i += 1
      }

      last = record
    }

    // set group keys value to final output.
    i = 0
    while (i < groupKeysMapping.length) {
      val (after, previous) = groupKeysMapping(i)
      output.setField(after, last.getField(previous))
      i += 1
    }

    // get final aggregate value and set to output.
    i = 0
    while (i < aggregateMapping.length) {
      val (after, previous) = aggregateMapping(i)
      val agg = aggregates(previous)
      val result = agg.getValue(accumulatorList(previous).get(0))
      output.setField(after, result)
      i += 1
    }

    // get window start timestamp
    val startTs: Long = last.getField(rowtimePos).asInstanceOf[Long]

    // set collector and window
    collector.wrappedCollector = out
    collector.windowStart = startTs
    collector.windowEnd = startTs + windowSize

    collector.collect(output)
  }

}
