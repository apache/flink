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

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

/**
  * Computes the final aggregate value from incrementally computed aggreagtes.
  *
  * @param numGroupingKey the number of grouping keys
  * @param numAggregates the number of aggregates
  * @param windowStartOffset the offset of the window start property
  * @param windowEndOffset   the offset of the window end property
  * @param windowRowtimeOffset the offset of the window rowtime property
  * @param finalRowArity  The arity of the final output row.
  */
class IncrementalAggregateTimeWindowFunction(
    private val numGroupingKey: Int,
    private val numAggregates: Int,
    private val windowStartOffset: Option[Int],
    private val windowEndOffset: Option[Int],
    private val windowRowtimeOffset: Option[Int],
    private val finalRowArity: Int)
  extends IncrementalAggregateWindowFunction[TimeWindow](
    numGroupingKey,
    numAggregates,
    finalRowArity) {

  private var collector: CRowTimeWindowPropertyCollector = _

  override def open(parameters: Configuration): Unit = {
    collector = new CRowTimeWindowPropertyCollector(
      windowStartOffset,
      windowEndOffset,
      windowRowtimeOffset)
    super.open(parameters)
  }

  override def apply(
      key: Row,
      window: TimeWindow,
      records: Iterable[Row],
      out: Collector[CRow]): Unit = {

    // set collector and window
    collector.wrappedCollector = out
    collector.windowStart = window.getStart
    collector.windowEnd = window.getEnd

    super.apply(key, window, records, collector)
  }
}
