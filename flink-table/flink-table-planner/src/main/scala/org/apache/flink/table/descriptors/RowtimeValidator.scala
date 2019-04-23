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

import org.apache.flink.table.descriptors.Rowtime._
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp, TimestampExtractor}
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, BoundedOutOfOrderTimestamps, PreserveWatermarks, WatermarkStrategy}
import org.apache.flink.table.util.JavaScalaConversionUtil.toJava
import org.apache.flink.table.utils.EncodingUtils

import scala.collection.JavaConverters._

/**
  * Validator for [[Rowtime]].
  */
class RowtimeValidator(
    supportsSourceTimestamps: Boolean,
    supportsSourceWatermarks: Boolean,
    prefix: String = "")
  extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    val timestampExistingField = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_FROM, false, 1)
    }

    val timestampCustom = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_CLASS, false, 1)
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_SERIALIZED, false, 1)
    }

    val timestampsValidation = if (supportsSourceTimestamps) {
      Map(
        ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD -> toJava(timestampExistingField),
        ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE -> DescriptorProperties.noValidation(),
        ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM -> toJava(timestampCustom))
    } else {
      Map(
        ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD -> toJava(timestampExistingField),
        ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM -> toJava(timestampCustom))
    }

    properties.validateEnum(
      prefix + ROWTIME_TIMESTAMPS_TYPE,
      false,
      timestampsValidation.asJava
    )

    val watermarkPeriodicBounded = (_: String) => {
      properties.validateLong(
        prefix + ROWTIME_WATERMARKS_DELAY, false, 0)
    }

    val watermarkCustom = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_WATERMARKS_CLASS, false, 1)
      properties.validateString(
        prefix + ROWTIME_WATERMARKS_SERIALIZED, false, 1)
    }

    val watermarksValidation = if (supportsSourceWatermarks) {
      Map(
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING -> DescriptorProperties.noValidation(),
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED -> toJava(watermarkPeriodicBounded),
        ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE -> DescriptorProperties.noValidation(),
        ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM -> toJava(watermarkCustom))
    } else {
      Map(
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING -> DescriptorProperties.noValidation(),
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED -> toJava(watermarkPeriodicBounded),
        ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM -> toJava(watermarkCustom))
    }

    properties.validateEnum(
      prefix + ROWTIME_WATERMARKS_TYPE,
      false,
      watermarksValidation.asJava
    )
  }
}

object RowtimeValidator {

  // utilities

  def getRowtimeComponents(properties: DescriptorProperties, prefix: String)
    : Option[(TimestampExtractor, WatermarkStrategy)] = {

    // create timestamp extractor
    val t = properties.getOptionalString(prefix + ROWTIME_TIMESTAMPS_TYPE)
    if (!t.isPresent) {
      return None
    }
    val extractor: TimestampExtractor = t.get() match {

      case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD =>
        val field = properties.getString(prefix + ROWTIME_TIMESTAMPS_FROM)
        new ExistingField(field)

      case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE =>
        StreamRecordTimestamp.INSTANCE

      case ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM =>
        val clazz = properties.getClass(
          prefix + ROWTIME_TIMESTAMPS_CLASS,
          classOf[TimestampExtractor])
        EncodingUtils.decodeStringToObject(
          properties.getString(prefix + ROWTIME_TIMESTAMPS_SERIALIZED),
          clazz)
    }

    // create watermark strategy
    val s = properties.getString(prefix + ROWTIME_WATERMARKS_TYPE)
    val strategy: WatermarkStrategy = s match {

      case ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING =>
        new AscendingTimestamps()

      case ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED =>
        val delay = properties.getLong(prefix + ROWTIME_WATERMARKS_DELAY)
        new BoundedOutOfOrderTimestamps(delay)

      case ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE =>
        PreserveWatermarks.INSTANCE

      case ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM =>
        val clazz = properties.getClass(
          prefix + ROWTIME_WATERMARKS_CLASS,
          classOf[WatermarkStrategy])
        EncodingUtils.decodeStringToObject(
          properties.getString(prefix + ROWTIME_WATERMARKS_SERIALIZED),
          clazz)
    }

    Some((extractor, strategy))
  }
}
