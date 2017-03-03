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
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]]. It is used for Session time-window
  * on batch.
  *
  * Note:
  *
  * This can handle two input types (depending if input is combined or not):
  *
  *  1. when partial aggregate is not supported, the input data structure of reduce is
  * |groupKey1|groupKey2|sum1|count1|sum2|count2|rowTime|
  *  2. when partial aggregate is supported, the input data structure of reduce is
  * |groupKey1|groupKey2|sum1|count1|sum2|count2|windowStart|windowEnd|
  *
  * @param aggregates             The aggregate functions.
  * @param groupKeysMapping       The index mapping of group keys between intermediate aggregate Row
  *                               and output Row.
  * @param aggregateMapping       The index mapping between aggregate function list and
  *                               aggregated value index in output Row.
  * @param finalRowArity          The output row field count.
  * @param finalRowWindowStartPos The relative window-start field position.
  * @param finalRowWindowEndPos   The relative window-end field position.
  * @param gap                    Session time window gap.
  */
class DataSetSessionWindowAggregateReduceGroupFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    gap: Long,
    isInputCombined: Boolean)
  extends RichGroupReduceFunction[Row, Row] {

  private var aggregateBuffer: Row = _
  private var output: Row = _
  private var collector: TimeWindowPropertyCollector = _
  private var accumStartPos: Int = groupKeysMapping.length
  private var intermediateRowArity: Int = accumStartPos + aggregates.length + 2
  private var intermediateRowWindowStartPos = intermediateRowArity - 2
  private var intermediateRowWindowEndPos = intermediateRowArity - 1
  private val maxMergeLen = 16
  val accumulatorList = Array.fill(aggregates.length) {
    new JArrayList[Accumulator]()
  }

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(finalRowWindowStartPos, finalRowWindowEndPos)
  }

  /**
    * For grouped intermediate aggregate Rows, divide window according to the window-start
    * and window-end, merge data (within a unified window) into an aggregate buffer, calculate
    * aggregated values output from aggregate buffer, and then set them into output
    * Row based on the mapping relationship between intermediate aggregate data and output data.
    *
    * @param records Grouped intermediate aggregate Rows iterator.
    * @param out     The collector to hand results to.
    *
    */
  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null
    accumulatorList.foreach(_.clear())

    val iterator = records.iterator()

    var count: Int = 0
    while (iterator.hasNext) {
      val record = iterator.next()
      count += 1
      currentRowTime = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (null == windowEnd ||
        (null != windowEnd && currentRowTime > windowEnd)) {

        // calculate the current window and open a new window
        if (null != windowEnd) {
          // evaluate and emit the current window's result.
          doEvaluateAndCollect(out, accumulatorList, windowStart, windowEnd)

          // clear the accumulator list for all aggregate
          accumulatorList.foreach(_.clear())
          count = 0
        } else {
          // set group keys value to final output.
          groupKeysMapping.foreach {
            case (after, previous) =>
              output.setField(after, record.getField(previous))
          }
        }

        windowStart = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      }

      // collect the accumulators for each aggregate
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

      windowEnd = if (isInputCombined) {
        // partial aggregate is supported
        record.getField(intermediateRowWindowEndPos).asInstanceOf[Long]
      } else {
        // partial aggregate is not supported, window-start equal rowtime + gap
        currentRowTime + gap
      }
    }
    // evaluate and emit the current window's result.
    doEvaluateAndCollect(out, accumulatorList, windowStart, windowEnd)
  }

  /**
    * Evaluate and emit the data of the current window.
    *
    * @param out             the collection of the aggregate results
    * @param accumulatorList an array (indexed by aggregate index) of the accumulator lists for
    *                        each aggregate
    * @param windowStart     the window's start attribute value is the min (rowtime) of all rows
    *                        in the window.
    * @param windowEnd       the window's end property value is max (rowtime) + gap for all rows
    *                        in the window.
    */
  def doEvaluateAndCollect(
      out: Collector[Row],
      accumulatorList: Array[JArrayList[Accumulator]],
      windowStart: Long,
      windowEnd: Long): Unit = {

    // merge the accumulators and then get value for the final output
    aggregateMapping.foreach {
      case (after, previous) =>
        val agg = aggregates(previous)
        val accum = agg.merge(accumulatorList(previous))
        output.setField(after, agg.getValue(accum))
    }

    // adds TimeWindow properties to output then emit output
    if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
      collector.wrappedCollector = out
      collector.windowStart = windowStart
      collector.windowEnd = windowEnd

      collector.collect(output)
    } else {
      out.collect(output)
    }
  }

}
