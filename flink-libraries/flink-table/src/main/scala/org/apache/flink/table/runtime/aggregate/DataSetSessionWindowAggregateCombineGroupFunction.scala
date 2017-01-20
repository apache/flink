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

import org.apache.flink.api.common.functions.RichGroupCombineFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, Preconditions}

/**
  * This wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * @param aggregates The aggregate functions.
  * @param groupingKeys
  * @param intermediateRowArity The intermediate row field count.
  * @param gap  Session time window gap.
  * @param intermediateRowType Intermediate row data type.
  */
class DataSetSessionWindowAggregateCombineGroupFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    groupingKeys: Array[Int],
    intermediateRowArity: Int,
    gap: Long,
    @transient intermediateRowType: TypeInformation[Row])
  extends RichGroupCombineFunction[Row,Row] with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var rowTimeFieldPos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupingKeys)
    aggregateBuffer = new Row(intermediateRowArity)
    rowTimeFieldPos = intermediateRowArity - 2
  }

  /**
    * For sub-grouped intermediate aggregate Rows, divide window based on the row-time
    * (current'row-time - previous’row-time > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Sub-grouped intermediate aggregate Rows .
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (null == windowEnd ||
        (null != windowEnd && (currentRowTime > windowEnd))) {

        // calculate the current window and open a new window.
        if (null != windowEnd) {
          // emit the current window's merged data
          doCollect(out, windowStart, windowEnd)
        } else {
          // set group keys to aggregateBuffer.
          for (i <- groupingKeys.indices) {
            aggregateBuffer.setField(i, record.getField(i))
          }
        }

        // initiate intermediate aggregate value.
        aggregates.foreach(_.initiate(aggregateBuffer))
        windowStart = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      }

      // merge intermediate aggregate value to the buffered value.
      aggregates.foreach(_.merge(record, aggregateBuffer))

      // the current row-time is the last row-time of the next calculation.
      windowEnd = currentRowTime + gap
    }
    // emit the merged data of the current window.
    doCollect(out, windowStart, windowEnd)
  }

  /**
    * Emit the merged data of the current window.
    * @param windowStart the window's start attribute value is the min (row-time)
    *                    of all rows in the window.
    * @param windowEnd the window's end property value is max (row-time) + gap
    *                  for all rows in the window.
    */
  def doCollect(
    out: Collector[Row],
    windowStart: Long,
    windowEnd: Long): Unit = {

    // intermediate Row WindowStartPos is row-time pos .
    aggregateBuffer.setField(rowTimeFieldPos, windowStart)
    // intermediate Row WindowEndPos is row-time pos + 1 .
    aggregateBuffer.setField(rowTimeFieldPos + 1, windowEnd)

    out.collect(aggregateBuffer)
  }

  override def getProducedType: TypeInformation[Row] = {
    intermediateRowType
  }
}
