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

import org.apache.flink.table.descriptors.DescriptorProperties.{serialize, toJava}
import org.apache.flink.table.descriptors.RowtimeValidator._
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp, TimestampExtractor}
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, BoundedOutOfOrderTimestamps, PreserveWatermarks, WatermarkStrategy}

import scala.collection.JavaConverters._

/**
  * Validator for [[Rowtime]].
  */
class RowtimeValidator(val prefix: String = "") extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    val timestampExistingField = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_FROM, isOptional = false, minLen = 1)
    }

    val timestampCustom = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_CLASS, isOptional = false, minLen = 1)
      properties.validateString(
        prefix + ROWTIME_TIMESTAMPS_SERIALIZED, isOptional = false, minLen = 1)
    }

    properties.validateEnum(
      prefix + ROWTIME_TIMESTAMPS_TYPE,
      isOptional = false,
      Map(
        ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD -> toJava(timestampExistingField),
        ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE -> properties.noValidation(),
        ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM -> toJava(timestampCustom)
      ).asJava
    )

    val watermarkPeriodicBounded = (_: String) => {
      properties.validateLong(
        prefix + ROWTIME_WATERMARKS_DELAY, isOptional = false, min = 0)
    }

    val watermarkCustom = (_: String) => {
      properties.validateString(
        prefix + ROWTIME_WATERMARKS_CLASS, isOptional = false, minLen = 1)
      properties.validateString(
        prefix + ROWTIME_WATERMARKS_SERIALIZED, isOptional = false, minLen = 1)
    }

    properties.validateEnum(
      prefix + ROWTIME_WATERMARKS_TYPE,
      isOptional = false,
      Map(
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING -> properties.noValidation(),
        ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED -> toJava(watermarkPeriodicBounded),
        ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE -> properties.noValidation(),
        ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM -> toJava(watermarkCustom)
      ).asJava
    )
  }
}

object RowtimeValidator {

  val ROWTIME = "rowtime"
  val ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type"
  val ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field"
  val ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE = "from-source"
  val ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM = "custom"
  val ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from"
  val ROWTIME_TIMESTAMPS_CLASS = "rowtime.timestamps.class"
  val ROWTIME_TIMESTAMPS_SERIALIZED = "rowtime.timestamps.serialized"

  val ROWTIME_WATERMARKS_TYPE = "rowtime.watermarks.type"
  val ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING = "periodic-ascending"
  val ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED = "periodic-bounded"
  val ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE = "from-source"
  val ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM = "custom"
  val ROWTIME_WATERMARKS_CLASS = "rowtime.watermarks.class"
  val ROWTIME_WATERMARKS_SERIALIZED = "rowtime.watermarks.serialized"
  val ROWTIME_WATERMARKS_DELAY = "rowtime.watermarks.delay"

  // utilities

  def normalizeTimestampExtractor(extractor: TimestampExtractor): Map[String, String] =
    extractor match {

        case existing: ExistingField =>
          Map(
            ROWTIME_TIMESTAMPS_TYPE -> ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD,
            ROWTIME_TIMESTAMPS_FROM -> existing.getArgumentFields.apply(0))

        case _: StreamRecordTimestamp =>
          Map(ROWTIME_TIMESTAMPS_TYPE -> ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE)

        case _: TimestampExtractor =>
          Map(
            ROWTIME_TIMESTAMPS_TYPE -> ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM,
            ROWTIME_TIMESTAMPS_CLASS -> extractor.getClass.getName,
            ROWTIME_TIMESTAMPS_SERIALIZED -> serialize(extractor))
    }

  def normalizeWatermarkStrategy(strategy: WatermarkStrategy): Map[String, String] =
    strategy match {

      case _: AscendingTimestamps =>
        Map(ROWTIME_WATERMARKS_TYPE -> ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING)

      case bounded: BoundedOutOfOrderTimestamps =>
        Map(
          ROWTIME_WATERMARKS_TYPE -> ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED,
          ROWTIME_WATERMARKS_DELAY -> bounded.delay.toString)

      case _: PreserveWatermarks =>
        Map(ROWTIME_WATERMARKS_TYPE -> ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE)

      case _: WatermarkStrategy =>
        Map(
          ROWTIME_WATERMARKS_TYPE -> ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM,
          ROWTIME_WATERMARKS_CLASS -> strategy.getClass.getName,
          ROWTIME_WATERMARKS_SERIALIZED -> serialize(strategy))
    }

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
        new StreamRecordTimestamp

      case ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM =>
        val clazz = properties.getClass(
          ROWTIME_TIMESTAMPS_CLASS,
          classOf[TimestampExtractor])
        DescriptorProperties.deserialize(
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
        DescriptorProperties.deserialize(
          properties.getString(prefix + ROWTIME_WATERMARKS_SERIALIZED),
          clazz)
    }

    Some((extractor, strategy))
  }
}
