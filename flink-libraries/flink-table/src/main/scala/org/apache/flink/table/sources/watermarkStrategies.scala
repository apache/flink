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

package org.apache.flink.table.sources

import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.types.Row

/**
  * Provides a strategy to generate watermarks for a rowtime attribute.
  *
  * A watermark strategy is either a [[PeriodicWatermarkAssigner]] or
  * [[PunctuatedWatermarkAssigner]].
  *
  */
sealed abstract class WatermarkStrategy extends Serializable

/** A periodic watermark assigner. */
abstract class PeriodicWatermarkAssigner extends WatermarkStrategy {

  /**
    * Updates the assigner with the next timestamp.
    *
    * @param timestamp The next timestamp to update the assigner.
    */
  def nextTimestamp(timestamp: Long): Unit

  /**
    * Returns the current watermark.
    *
    * @return The current watermark.
    */
  def getWatermark: Watermark
}

/** A punctuated watermark assigner. */
abstract class PunctuatedWatermarkAssigner extends WatermarkStrategy {

  /**
    * Returns the watermark for the current row or null if no watermark should be generated.
    *
    * @param row The current row.
    * @param timestamp The value of the timestamp attribute for the row.
    * @return The watermark for this row or null if no watermark should be generated.
    */
  def getWatermark(row: Row, timestamp: Long): Watermark
}

/**
  * A watermark assigner for ascending rowtime attributes.
  *
  * Emits a watermark of the maximum observed timestamp so far minus 1.
  * Rows that have a timestamp equal to the max timestamp are not late.
  */
class AscendingWatermarks extends PeriodicWatermarkAssigner {

  var maxTimestamp: Long = Long.MinValue + 1

  override def nextTimestamp(timestamp: Long): Unit = {
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
  }

  override def getWatermark: Watermark = new Watermark(maxTimestamp - 1)
}

/**
  * A watermark assigner for rowtime attributes which are out-of-order by a bounded time interval.
  *
  * Emits watermarks which are the maximum observed timestamp minus the specified delay.
  *
  * @param delay The delay by which watermarks are behind the maximum observed timestamp.
  */
class BoundedOutOfOrderWatermarks(val delay: Long) extends PeriodicWatermarkAssigner {

  var maxTimestamp: Long = Long.MinValue + delay

  override def nextTimestamp(timestamp: Long): Unit = {
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
  }

  override def getWatermark: Watermark = new Watermark(maxTimestamp - delay)
}
