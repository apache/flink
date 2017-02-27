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

  private var collector: TimeWindowPropertyCollector = _
  protected var aggregateBuffer: Row = _
  private var output: Row = _
  private val accumStartPos: Int = groupKeysMapping.length
  private val rowtimePos: Int = accumStartPos + aggregates.length
  private val intermediateRowArity: Int = rowtimePos + 1
  protected val maxMergeLen = 16
  val accumulatorList = Array.fill(aggregates.length) {
    new JArrayList[Accumulator]()
  }

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(windowStartPos, windowEndPos)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var last: Row = null
    accumulatorList.foreach(_.clear())

    val iterator = records.iterator()

    var count: Int = 0
    while (iterator.hasNext) {
      val record = iterator.next()
      count += 1
      // per each aggregator, collect its accumulators to a list
      for (i <- aggregates.indices) {
        accumulatorList(i).add(record.getField(accumStartPos + i).asInstanceOf[Accumulator])
      }
      // if the number of buffered accumulators is bigger than maxMergeLen, merge them into one
      // accumulator
      if (count > maxMergeLen) {
        count = 0
        for (i <- aggregates.indices) {
          val agg = aggregates(i)
          val accumulator = agg.merge(accumulatorList(i))
          accumulatorList(i).clear()
          accumulatorList(i).add(accumulator)
        }
      }
      last = record
    }

    // set group keys value to final output.
    groupKeysMapping.foreach {
      case (after, previous) =>
        output.setField(after, last.getField(previous))
    }

    // get final aggregate value and set to output.
    aggregateMapping.foreach {
      case (after, previous) => {
        val agg = aggregates(previous)
        val accumulator = agg.merge(accumulatorList(previous))
        val result = agg.getValue(accumulator)
        output.setField(after, result)
      }
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
