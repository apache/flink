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
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  *
  * It is used for sliding on batch for both time and count-windows.
  *
  * @param aggregates aggregate functions.
  * @param groupKeysMapping index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity output row field count
  * @param finalRowWindowStartPos relative window-start position to last field of output row
  * @param finalRowWindowEndPos relative window-end position to last field of output row
  * @param windowSize size of the window, used to determine window-end for output row
  */
class DataSetSlideWindowAggReduceGroupFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    windowSize: Long)
  extends RichGroupReduceFunction[Row, Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(groupKeysMapping)

  private var collector: TimeWindowPropertyCollector = _
  private var output: Row = _
  private val accumulatorStartPos: Int = groupKeysMapping.length
  protected val windowStartPos: Int = accumulatorStartPos + aggregates.length

  val accumulatorList: Array[JArrayList[Accumulator]] = Array.fill(aggregates.length) {
    new JArrayList[Accumulator](2)
  }

  override def open(config: Configuration) {
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(finalRowWindowStartPos, finalRowWindowEndPos)

    // init lists with two empty accumulators
    var i = 0
    while (i < aggregates.length) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
      i += 1
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // reset first accumulator
    var i = 0
    while (i < aggregates.length) {
      aggregates(i).resetAccumulator(accumulatorList(i).get(0))
      i += 1
    }

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()

      // accumulate
      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(accumulatorStartPos + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
        i += 1
      }

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys value to final output
        i = 0
        while (i < groupKeysMapping.length) {
          val mapping = groupKeysMapping(i)
          output.setField(mapping._1, record.getField(mapping._2))
          i += 1
        }

        // get final aggregate value and set to output.
        i = 0
        while (i < aggregateMapping.length) {
          val mapping = aggregateMapping(i)
          val agg = aggregates(i)
          val result = agg.getValue(accumulatorList(mapping._2).get(0))
          output.setField(mapping._1, result)
          i += 1
        }

        // adds TimeWindow properties to output then emit output
        if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
          collector.wrappedCollector = out
          collector.windowStart = record.getField(windowStartPos).asInstanceOf[Long]
          collector.windowEnd = collector.windowStart + windowSize

          collector.collect(output)
        } else {
          out.collect(output)
        }
      }
    }
  }
}
