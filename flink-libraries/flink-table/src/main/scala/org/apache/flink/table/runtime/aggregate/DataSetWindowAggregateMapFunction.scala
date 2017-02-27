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

import java.sql.Timestamp

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions


/**
  * This map function only works for windows on batch tables. The differences between this function
  * and [[org.apache.flink.table.runtime.aggregate.AggregateMapFunction]] is this function
  * append an (aligned) rowtime field to the end of the output row.
  */
class DataSetWindowAggregateMapFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val groupingKeys: Array[Int],
    private val timeFieldPos: Int, // time field position in input row
    private val tumbleTimeWindowSize: Option[Long],
    @transient private val returnType: TypeInformation[Row])
  extends RichMapFunction[Row, Row] with ResultTypeQueryable[Row] {

  private var output: Row = _
  // rowtime index in the buffer output row
  private var rowtimeIndex: Int = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(aggFields)
    Preconditions.checkArgument(aggregates.length == aggFields.length)
    // add one more arity to store rowtime
    val partialRowLength = groupingKeys.length + aggregates.length + 1
    // set rowtime to the last field of the output row
    rowtimeIndex = partialRowLength - 1
    output = new Row(partialRowLength)
  }

  override def map(input: Row): Row = {

    for (i <- aggregates.indices) {
      val agg = aggregates(i)
      val fieldValue = input.getField(aggFields(i))
      val accumulator = agg.createAccumulator()
      agg.accumulate(accumulator, fieldValue)
      output.setField(groupingKeys.length + i, accumulator)
    }

    for (i <- groupingKeys.indices) {
      output.setField(i, input.getField(groupingKeys(i)))
    }

    val timeField = input.getField(timeFieldPos)
    val rowtime = getTimestamp(timeField)
    if (tumbleTimeWindowSize.isDefined) {
      // in case of tumble time window, align rowtime to window start to represent the window
      output.setField(
        rowtimeIndex,
        TimeWindow.getWindowStartWithOffset(rowtime, 0L, tumbleTimeWindowSize.get))
    } else {
      // for session window and slide window
      output.setField(rowtimeIndex, rowtime)
    }

    output
  }

  private def getTimestamp(timeField: Any): Long = {
    timeField match {
      case b: Byte => b.toLong
      case t: Character => t.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toLong
      case d: Double => d.toLong
      case s: String => s.toLong
      case t: Timestamp => t.getTime
      case _ =>
        throw new RuntimeException(
          s"Window time field doesn't support ${timeField.getClass} type currently")
    }
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}

