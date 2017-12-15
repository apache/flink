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

import org.apache.flink.table.descriptors.DescriptorProperties.serialize
import org.apache.flink.table.descriptors.RowtimeValidator._
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp, TimestampExtractor}
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, BoundedOutOfOrderTimestamps, PreserveWatermarks, WatermarkStrategy}

/**
  * Validator for [[Rowtime]].
  */
class RowtimeValidator(val prefix: String = "") extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateInt(prefix + ROWTIME_VERSION, isOptional = true, 0, Integer.MAX_VALUE)

    val noValidation = () => {}

    val timestampExistingField = () => {
      properties.validateString(prefix + TIMESTAMPS_FROM, isOptional = false, minLen = 1)
    }

    val timestampCustom = () => {
      properties.validateString(prefix + TIMESTAMPS_CLASS, isOptional = false, minLen = 1)
      properties.validateString(prefix + TIMESTAMPS_SERIALIZED, isOptional = false, minLen = 1)
    }

    properties.validateEnum(
      prefix + TIMESTAMPS_TYPE,
      isOptional = false,
      Map(
        TIMESTAMPS_TYPE_VALUE_FROM_FIELD -> timestampExistingField,
        TIMESTAMPS_TYPE_VALUE_FROM_SOURCE -> noValidation,
        TIMESTAMPS_TYPE_VALUE_CUSTOM -> timestampCustom
      )
    )

    val watermarkPeriodicBounding = () => {
      properties.validateLong(prefix + WATERMARKS_DELAY, isOptional = false, min = 0)
    }

    val watermarkCustom = () => {
      properties.validateString(prefix + WATERMARKS_CLASS, isOptional = false, minLen = 1)
      properties.validateString(prefix + WATERMARKS_SERIALIZED, isOptional = false, minLen = 1)
    }

    properties.validateEnum(
      prefix + WATERMARKS_TYPE,
      isOptional = false,
      Map(
        WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING -> noValidation,
        WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDING -> watermarkPeriodicBounding,
        WATERMARKS_TYPE_VALUE_FROM_SOURCE -> noValidation,
        WATERMARKS_TYPE_VALUE_CUSTOM -> watermarkCustom
      )
    )
  }
}

object RowtimeValidator {

  val ROWTIME = "rowtime"

  // per rowtime properties

  val ROWTIME_VERSION = "version"
  val TIMESTAMPS_TYPE = "timestamps.type"
  val TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field"
  val TIMESTAMPS_TYPE_VALUE_FROM_SOURCE = "from-source"
  val TIMESTAMPS_TYPE_VALUE_CUSTOM = "custom"
  val TIMESTAMPS_FROM = "timestamps.from"
  val TIMESTAMPS_CLASS = "timestamps.class"
  val TIMESTAMPS_SERIALIZED = "timestamps.serialized"

  val WATERMARKS_TYPE = "watermarks.type"
  val WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING = "periodic-ascending"
  val WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDING = "periodic-bounding"
  val WATERMARKS_TYPE_VALUE_FROM_SOURCE = "from-source"
  val WATERMARKS_TYPE_VALUE_CUSTOM = "custom"
  val WATERMARKS_CLASS = "watermarks.class"
  val WATERMARKS_SERIALIZED = "watermarks.serialized"
  val WATERMARKS_DELAY = "watermarks.delay"

  // utilities

  def normalizeTimestampExtractor(extractor: TimestampExtractor): Map[String, String] =
    extractor match {
        case existing: ExistingField =>
          Map(
            TIMESTAMPS_TYPE -> TIMESTAMPS_TYPE_VALUE_FROM_FIELD,
            TIMESTAMPS_FROM -> existing.getArgumentFields.apply(0))
        case _: StreamRecordTimestamp =>
          Map(TIMESTAMPS_TYPE -> TIMESTAMPS_TYPE_VALUE_FROM_SOURCE)
        case _: TimestampExtractor =>
          Map(
            TIMESTAMPS_TYPE -> TIMESTAMPS_TYPE_VALUE_CUSTOM,
            TIMESTAMPS_CLASS -> extractor.getClass.getName,
            TIMESTAMPS_SERIALIZED -> serialize(extractor))
    }

  def normalizeWatermarkStrategy(strategy: WatermarkStrategy): Map[String, String] =
    strategy match {
      case _: AscendingTimestamps =>
        Map(WATERMARKS_TYPE -> WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING)
      case bounding: BoundedOutOfOrderTimestamps =>
        Map(
          WATERMARKS_TYPE -> WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDING,
          WATERMARKS_DELAY -> bounding.delay.toString)
      case _: PreserveWatermarks =>
        Map(WATERMARKS_TYPE -> WATERMARKS_TYPE_VALUE_FROM_SOURCE)
      case _: WatermarkStrategy =>
        Map(
          WATERMARKS_TYPE -> WATERMARKS_TYPE_VALUE_CUSTOM,
          WATERMARKS_CLASS -> strategy.getClass.getName,
          WATERMARKS_SERIALIZED -> serialize(strategy))
    }
}
