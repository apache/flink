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
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Adds TimeWindow properties to specified fields of a row before it emits the row to a wrapped
  * collector.
  */
abstract class TimeWindowPropertyCollector[T](
    windowStartOffset: Option[Int],
    windowEndOffset: Option[Int],
    windowRowtimeOffset: Option[Int])
  extends Collector[T] {

  var wrappedCollector: Collector[T] = _
  var output: Row = _
  var windowStart:Long = _
  var windowEnd:Long = _

  def getRow(record: T): Row

  def setRowtimeAttribute(pos: Int): Unit

  override def collect(record: T): Unit = {

    output = getRow(record)
    val lastFieldPos = output.getArity - 1

    if (windowStartOffset.isDefined) {
      output.setField(
        lastFieldPos + windowStartOffset.get,
        SqlFunctions.internalToTimestamp(windowStart))
    }
    if (windowEndOffset.isDefined) {
      output.setField(
        lastFieldPos + windowEndOffset.get,
        SqlFunctions.internalToTimestamp(windowEnd))
    }

    if (windowRowtimeOffset.isDefined) {
      setRowtimeAttribute(lastFieldPos + windowRowtimeOffset.get)
    }

    wrappedCollector.collect(record)
  }

  override def close(): Unit = wrappedCollector.close()
}

final class DataSetTimeWindowPropertyCollector(
    startOffset: Option[Int],
    endOffset: Option[Int],
    rowtimeOffset: Option[Int])
  extends TimeWindowPropertyCollector[Row](startOffset, endOffset, rowtimeOffset) {

  override def getRow(record: Row): Row = record

  override def setRowtimeAttribute(pos: Int): Unit = {
    output.setField(pos, SqlFunctions.internalToTimestamp(windowEnd - 1))
  }
}

final class DataStreamTimeWindowPropertyCollector(
    startOffset: Option[Int],
    endOffset: Option[Int],
    rowtimeOffset: Option[Int])
  extends TimeWindowPropertyCollector[CRow](startOffset, endOffset, rowtimeOffset) {

  override def getRow(record: CRow): Row = record.row

  override def setRowtimeAttribute(pos: Int): Unit = {
    output.setField(pos, windowEnd - 1)
  }
}
