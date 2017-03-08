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

import org.apache.flink.api.common.functions.{AbstractRichFunction, GroupCombineFunction, MapPartitionFunction}
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
class DataSetSessionWindowAggregatePreProcessor(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupingKeys: Array[Int],
    gap: Long,
    @transient intermediateRowType: TypeInformation[Row])
  extends AbstractRichFunction
  with MapPartitionFunction[Row,Row]
  with GroupCombineFunction[Row,Row]
  with ResultTypeQueryable[Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(groupingKeys)

  private var aggregateBuffer: Row = _
  private val accumStartPos: Int = groupingKeys.length
  private val rowTimeFieldPos = accumStartPos + aggregates.length

  val accumulatorList: Array[JArrayList[Accumulator]] = Array.fill(aggregates.length) {
    new JArrayList[Accumulator](2)
  }

  override def open(config: Configuration) {
    aggregateBuffer = new Row(rowTimeFieldPos + 2)

    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
    }
  }

  /**
    * For sub-grouped intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Sub-grouped intermediate aggregate Rows.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {
    preProcessing(records, out)
  }

  /**
    * Divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Intermediate aggregate Rows.
    * @return Pre partition intermediate aggregate Row.
    *
    */
  override def mapPartition(records: Iterable[Row], out: Collector[Row]): Unit = {
    preProcessing(records, out)
  }

  /**
    * Intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records Intermediate aggregate Rows.
    * @return PreProcessing intermediate aggregate Row.
    *
    */
  private def preProcessing(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null

    // reset first accumulator in merge list
    var i = 0
    while (i < aggregates.length) {
      aggregates(i).resetAccumulator(accumulatorList(i).get(0))
      i += 1
    }

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (windowEnd == null || (windowEnd != null && (currentRowTime > windowEnd))) {

        // calculate the current window and open a new window.
        if (windowEnd != null) {
          // emit the current window's merged data
          doCollect(out, accumulatorList, windowStart, windowEnd)

          // reset first value of accumulator list
          i = 0
          while (i < aggregates.length) {
            aggregates(i).resetAccumulator(accumulatorList(i).get(0))
            i += 1
          }
        } else {
          // set group keys to aggregateBuffer.
          i = 0
          while (i < groupingKeys.length) {
            aggregateBuffer.setField(i, record.getField(i))
            i += 1
          }
        }

        windowStart = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      }

      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(accumStartPos + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
        i += 1
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
    var i = 0
    while (i < aggregates.length) {
      aggregateBuffer.setField(accumStartPos + i, accumulatorList(i).get(0))
      i += 1
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
