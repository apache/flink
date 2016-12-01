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
package org.apache.flink.api.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, Preconditions}

import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
/**
  * This wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  *
  * @param aggregates       The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  */
class DataSetSessionWindowAggregateReduceGroupFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowArity: Int,
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    gap:Long)
  extends RichGroupReduceFunction[Row, Row] {

  private var aggregateBuffer: Row = _
  private var output: Row = _
  private var collector: TimeWindowPropertyCollector = _
  private var intermediateRowWindowStartPos = 0
  private var intermediateRowWindowEndPos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    intermediateRowWindowStartPos = intermediateRowArity - 2
    intermediateRowWindowEndPos = intermediateRowArity - 1
    output = new Row(finalRowArity)
    if (finalRowWindowStartPos.isDefined  || finalRowWindowEndPos.isDefined) {
      collector = new TimeWindowPropertyCollector(finalRowWindowStartPos, finalRowWindowEndPos)
    }
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

    var last: Row = null
    var head: Row = null
    var lastWindowEnd: Option[Long] = None
    var currentWindowStart: Option[Long] = None

    records.foreach(
      (record) => {
        currentWindowStart =
          Some(record.productElement(intermediateRowWindowStartPos).asInstanceOf[Long])
        // initial traversal or new window open
        if (lastWindowEnd.isEmpty ||
          (lastWindowEnd.isDefined && currentWindowStart.get > lastWindowEnd.get)) {

          // calculate the current window and open a new window
          if (lastWindowEnd.isDefined) {

            // evaluate and emit the current window's result.
            doEvaluateAndCollect(out, last, head)
          }
          // initiate intermediate aggregate value.
          aggregates.foreach(_.initiate(aggregateBuffer))
          head = record
        }

        aggregates.foreach(_.merge(record, aggregateBuffer))
        last = record
        lastWindowEnd = Some(getWindowEnd(last))
      })

    doEvaluateAndCollect(out, last, head)

  }

  def doEvaluateAndCollect(
    out: Collector[Row],
    last: Row,
    head: Row): Unit = {
    // set group keys value to final output.
    groupKeysMapping.foreach {
      case (after, previous) =>
        output.setField(after, last.productElement(previous))
    }

    // evaluate final aggregate value and set to output.
    aggregateMapping.foreach {
      case (after, previous) =>
        output.setField(after, aggregates(previous).evaluate(aggregateBuffer))
    }

    // adds TimeWindow properties to output then emit output
    if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
      val start =
        head.productElement(intermediateRowWindowStartPos).asInstanceOf[Long]
      val end = getWindowEnd(last)

      collector.wrappedCollector = out
      collector.timeWindow = new TimeWindow(start, end)

      collector.collect(output)
    } else {
      out.collect(output)
    }
  }

  def getWindowEnd(record: Row): Long = {

    // when partial aggregate is not supported, the input data structure of reduce is
    // |groupKey1|groupKey2|sum1|count1|sum2|count2|rowTime|
    if (record.productArity == intermediateRowWindowEndPos) {
      //session window end is row-time + gap
      record.productElement(intermediateRowWindowStartPos).asInstanceOf[Long] + gap
    }
    // when partial aggregate is supported, the input data structure of reduce is
    // |groupKey1|groupKey2|sum1|count1|sum2|count2|windowStart|windowEnd|
    else {
      record.productElement(intermediateRowWindowEndPos).asInstanceOf[Long]
    }
  }

}
