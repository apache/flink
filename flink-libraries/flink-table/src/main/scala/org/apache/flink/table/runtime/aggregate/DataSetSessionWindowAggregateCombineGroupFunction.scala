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

import org.apache.flink.api.common.functions.RichGroupCombineFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.util.{Collector, Preconditions}

/**
  * This wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * @param aggregates          The aggregate functions.
  * @param groupingKeys        The indexes of the grouping fields.
  * @param gap                 Session time window gap.
  * @param intermediateRowType Intermediate row data type.
  */
class DataSetSessionWindowAggregateCombineGroupFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupingKeys: Array[Int],
    gap: Long,
    @transient intermediateRowType: TypeInformation[Row])
  extends RichGroupCombineFunction[Row, Row] with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var accumStartPos: Int = groupingKeys.length
  private var rowTimeFieldPos = accumStartPos + aggregates.length
  private val maxMergeLen = 16
  val accumulatorList = Array.fill(aggregates.length) {
    new JArrayList[Accumulator]()
  }

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupingKeys)
    aggregateBuffer = new Row(rowTimeFieldPos + 2)
  }

  /**
    * For sub-grouped intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records Sub-grouped intermediate aggregate Rows.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null
    accumulatorList.foreach(_.clear())

    val iterator = records.iterator()


    var count: Int = 0
    while (iterator.hasNext) {
      val record = iterator.next()
      count += 1
      currentRowTime = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (windowEnd == null || (windowEnd != null && (currentRowTime > windowEnd))) {

        // calculate the current window and open a new window.
        if (windowEnd != null) {
          // emit the current window's merged data
          doCollect(out, accumulatorList, windowStart, windowEnd)

          // clear the accumulator list for all aggregate
          accumulatorList.foreach(_.clear())
          count = 0
        } else {
          // set group keys to aggregateBuffer.
          for (i <- groupingKeys.indices) {
            aggregateBuffer.setField(i, record.getField(i))
          }
        }

        windowStart = record.getField(rowTimeFieldPos).asInstanceOf[Long]
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

      // the current rowtime is the last rowtime of the next calculation.
      windowEnd = currentRowTime + gap
    }
    // emit the merged data of the current window.
    doCollect(out, accumulatorList, windowStart, windowEnd)
  }

  /**
    * Emit the merged data of the current window.
    *
    * @param out             the collection of the aggregate results
    * @param accumulatorList an array (indexed by aggregate index) of the accumulator lists for
    *                        each aggregate
    * @param windowStart     the window's start attribute value is the min (rowtime)
    *                        of all rows in the window.
    * @param windowEnd       the window's end property value is max (rowtime) + gap
    *                        for all rows in the window.
    */
  def doCollect(
      out: Collector[Row],
      accumulatorList: Array[JArrayList[Accumulator]],
      windowStart: Long,
      windowEnd: Long): Unit = {

    // merge the accumulators into one accumulator
    for (i <- aggregates.indices) {
      aggregateBuffer.setField(accumStartPos + i, aggregates(i).merge(accumulatorList(i)))
    }

    // intermediate Row WindowStartPos is rowtime pos.
    aggregateBuffer.setField(rowTimeFieldPos, windowStart)

    // intermediate Row WindowEndPos is rowtime pos + 1.
    aggregateBuffer.setField(rowTimeFieldPos + 1, windowEnd)

    out.collect(aggregateBuffer)
  }

  override def getProducedType: TypeInformation[Row] = {
    intermediateRowType
  }
}
