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

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


/**
  * It is used for sliding windows on batch for time-windows. It takes a prepared input row,
  * aligns the window start, and replicates or omits records for different panes of a sliding
  * window. It is used for non-partial aggregations.
  *
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideTimeWindowAggFlatMapFunction(
    private val timeFieldPos: Int,
    private val windowSize: Long,
    private val windowSlide: Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichFlatMapFunction[Row, Row]
  with ResultTypeQueryable[Row] {

  override def flatMap(record: Row, out: Collector[Row]): Unit = {
    val windowStart = record.getField(timeFieldPos).asInstanceOf[Long]

    // adopted from SlidingEventTimeWindows.assignWindows
    var start: Long = TimeWindow.getWindowStartWithOffset(windowStart, 0, windowSlide)

    // adopted from SlidingEventTimeWindows.assignWindows
    while (start > windowStart - windowSize) {
      record.setField(timeFieldPos, start)
      out.collect(record)
      start -= windowSlide
    }
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
