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

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Adds TimeWindow properties to specified fields of a row before it emits the row to a wrapped
  * collector.
  */
class TimeWindowPropertyCollector(windowStartOffset: Option[Int], windowEndOffset: Option[Int])
    extends Collector[Row] {

  var wrappedCollector: Collector[Row] = _
  var timeWindow: TimeWindow = _

  override def collect(record: Row): Unit = {

    val lastFieldPos = record.getArity - 1

    if (windowStartOffset.isDefined) {
      record.setField(
        lastFieldPos + windowStartOffset.get,
        SqlFunctions.internalToTimestamp(timeWindow.getStart))
    }
    if (windowEndOffset.isDefined) {
      record.setField(
        lastFieldPos + windowEndOffset.get,
        SqlFunctions.internalToTimestamp(timeWindow.getEnd))
    }
    wrappedCollector.collect(record)
  }

  override def close(): Unit = wrappedCollector.close()
}
