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
package org.apache.flink.table.runtime.operators

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.operators.co.LegacyKeyedCoProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * A [[LegacyKeyedCoProcessOperator]] that supports holding back watermarks with a static delay.
  */
class KeyedCoProcessOperatorWithWatermarkDelay[KEY, IN1, IN2, OUT](
    private val flatMapper: CoProcessFunction[IN1, IN2, OUT],
    private val watermarkDelay: Long = 0L)
  extends LegacyKeyedCoProcessOperator[KEY, IN1, IN2, OUT](flatMapper) {

  /** emits watermark without delay */
  def emitWithoutDelay(mark: Watermark): Unit = output.emitWatermark(mark)

  /** emits watermark with delay */
  def emitWithDelay(mark: Watermark): Unit = {
    output.emitWatermark(new Watermark(mark.getTimestamp - watermarkDelay))
  }

  if (watermarkDelay < 0) {
    throw new IllegalArgumentException("The watermark delay should be non-negative.")
  }

  // choose watermark emitter
  val emitter: Watermark => Unit = if (watermarkDelay == 0) {
    emitWithoutDelay
  } else {
    emitWithDelay
  }

  @throws[Exception]
  override def processWatermark(mark: Watermark) {
    if (timeServiceManager != null) timeServiceManager.advanceWatermark(mark)

    emitter(mark)
  }

}
