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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

/**
  * A custom operator that adjusts the watermarks.
  *
  * @param offset can be positive or negative and according offset adjusts the watermarks.
  */
class AdjustWatermark[IN](offset: Long)
  extends AbstractStreamOperator[IN] with OneInputStreamOperator[IN, IN] {

  override def processWatermark(mark: Watermark): Unit = {
    super.processWatermark(new Watermark(mark.getTimestamp - offset))
  }

  override def processElement(element: StreamRecord[IN]): Unit = {
    output.collect(element)
  }

}

object AdjustWatermark {
  def of[IN](lateDataTimeOffset: Long): AdjustWatermark[IN] = {
    new AdjustWatermark[IN](lateDataTimeOffset)
  }
}
