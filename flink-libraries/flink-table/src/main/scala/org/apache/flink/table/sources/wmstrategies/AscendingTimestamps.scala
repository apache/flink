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

package org.apache.flink.table.sources.wmstrategies

import org.apache.flink.streaming.api.watermark.Watermark

/**
  * A watermark strategy for ascending rowtime attributes.
  *
  * Emits a watermark of the maximum observed timestamp so far minus 1.
  * Rows that have a timestamp equal to the max timestamp are not late.
  */
final class AscendingTimestamps extends PeriodicWatermarkAssigner {

  var maxTimestamp: Long = Long.MinValue + 1

  override def nextTimestamp(timestamp: Long): Unit = {
    if (timestamp > maxTimestamp) {
      maxTimestamp = timestamp
    }
  }

  override def getWatermark: Watermark = new Watermark(maxTimestamp - 1)

  override def equals(obj: Any): Boolean = obj match {
    case _: AscendingTimestamps => true
    case _ => false
  }

  override def hashCode(): Int = {
    classOf[AscendingTimestamps].hashCode()
  }
}
