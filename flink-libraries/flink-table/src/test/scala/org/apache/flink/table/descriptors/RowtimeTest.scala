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

package org.apache.flink.table.descriptors

import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.RowtimeTest.CustomAssigner
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.types.Row
import org.junit.Test

class RowtimeTest extends DescriptorTestBase {

  @Test
  def testRowtime(): Unit = {
    val desc = Rowtime()
      .timestampsFromField("otherField")
      .watermarksPeriodicBounding(1000L)
    val expected = Seq(
      "rowtime.0.version" -> "1",
      "rowtime.0.timestamps.type" -> "from-field",
      "rowtime.0.timestamps.from" -> "otherField",
      "rowtime.0.watermarks.type" -> "periodic-bounding",
      "rowtime.0.watermarks.delay" -> "1000"
    )
    verifyProperties(desc, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWatermarkType(): Unit = {
    verifyInvalidProperty("rowtime.0.watermarks.type", "xxx")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingWatermarkClass(): Unit = {
    verifyMissingProperty("rowtime.0.watermarks.class")
  }

  override def descriptor(): Descriptor = {
    Rowtime()
      .timestampsFromSource()
      .watermarksFromStrategy(new CustomAssigner())
  }

  override def validator(): DescriptorValidator = {
    new RowtimeValidator("rowtime.0.")
  }
}

object RowtimeTest {

  class CustomAssigner extends PunctuatedWatermarkAssigner() {
    override def getWatermark(row: Row, timestamp: Long): Watermark =
      throw new UnsupportedOperationException()
  }
}
