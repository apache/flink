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

import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  *
  * Computes the final aggregate value from incrementally computed aggreagtes.
  *
  * @param aggregates   The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity  The arity of the final output row.
  */
class IncrementalAggregateAllTimeWindowFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val finalRowArity: Int,
    private val windowStartPos: Option[Int],
    private val windowEndPos: Option[Int])
  extends IncrementalAggregateAllWindowFunction[TimeWindow](
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    finalRowArity) {

  private var collector: TimeWindowPropertyCollector = _

  override def open(parameters: Configuration): Unit = {
    collector = new TimeWindowPropertyCollector(windowStartPos, windowEndPos)
    super.open(parameters)
  }

  override def apply(
    window: TimeWindow,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    // set collector and window
    collector.wrappedCollector = out
    collector.windowStart = window.getStart
    collector.windowEnd = window.getEnd

    super.apply(window, records, collector)
  }
}
