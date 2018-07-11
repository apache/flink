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

import java.util

import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.RowtimeTest.CustomAssigner
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.types.Row
import org.junit.Test

import scala.collection.JavaConverters._

class RowtimeTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidWatermarkType(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "rowtime.watermarks.type", "xxx")
  }

  @Test(expected = classOf[ValidationException])
  def testMissingWatermarkClass(): Unit = {
    removePropertyAndVerify(descriptors().get(1), "rowtime.watermarks.class")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedSourceWatermarks(): Unit = {
    addPropertyAndVerify(descriptors().get(0), "rowtime.watermarks.type", "from-source")
  }

  // ----------------------------------------------------------------------------------------------

  override def descriptors(): util.List[Descriptor] = {
    val desc1 = Rowtime()
      .timestampsFromField("otherField")
      .watermarksPeriodicBounded(1000L)

    val desc2 = Rowtime()
      .timestampsFromSource()
      .watermarksFromStrategy(new CustomAssigner())

    util.Arrays.asList(desc1, desc2)
  }

  override def validator(): DescriptorValidator = {
    new RowtimeValidator(supportsSourceTimestamps = true, supportsSourceWatermarks = false)
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val props1 = Map(
      "rowtime.timestamps.type" -> "from-field",
      "rowtime.timestamps.from" -> "otherField",
      "rowtime.watermarks.type" -> "periodic-bounded",
      "rowtime.watermarks.delay" -> "1000"
    )

    val props2 = Map(
      "rowtime.timestamps.type" -> "from-source",
      "rowtime.watermarks.type" -> "custom",
      "rowtime.watermarks.class" -> "org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner",
      "rowtime.watermarks.serialized" -> ("rO0ABXNyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaX" +
        "B0b3JzLlJvd3RpbWVUZXN0JEN1c3RvbUFzc2lnbmVyeDcuDvfbu0kCAAB4cgBHb3JnLmFwYWNoZS5mbGluay" +
        "50YWJsZS5zb3VyY2VzLndtc3RyYXRlZ2llcy5QdW5jdHVhdGVkV2F0ZXJtYXJrQXNzaWduZXKBUc57oaWu9A" +
        "IAAHhyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnNvdXJjZXMud21zdHJhdGVnaWVzLldhdGVybWFya1N0cm" +
        "F0ZWd5mB_uSxDZ8-MCAAB4cA")
    )

    util.Arrays.asList(props1.asJava, props2.asJava)
  }
}

object RowtimeTest {

  class CustomAssigner extends PunctuatedWatermarkAssigner() {
    override def getWatermark(row: Row, timestamp: Long): Watermark =
      throw new UnsupportedOperationException()
  }
}
