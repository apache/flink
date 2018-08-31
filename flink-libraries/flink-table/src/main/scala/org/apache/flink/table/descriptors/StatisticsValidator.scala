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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.DescriptorProperties.toScala
import org.apache.flink.table.descriptors.StatisticsValidator.{STATISTICS_COLUMNS, STATISTICS_PROPERTY_VERSION, STATISTICS_ROW_COUNT, validateColumnStats}
import org.apache.flink.table.plan.stats.ColumnStats

import scala.collection.mutable

/**
  * Validator for [[FormatDescriptor]].
  */
class StatisticsValidator extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateInt(STATISTICS_PROPERTY_VERSION, isOptional = true, 0, Integer.MAX_VALUE)
    properties.validateLong(STATISTICS_ROW_COUNT, isOptional = true, min = 0)
    validateColumnStats(properties, STATISTICS_COLUMNS)
  }
}

object StatisticsValidator {

  val STATISTICS_PROPERTY_VERSION = "statistics.property-version"
  val STATISTICS_ROW_COUNT = "statistics.row-count"
  val STATISTICS_COLUMNS = "statistics.columns"

  // per column properties

  val NAME = "name"
  val DISTINCT_COUNT = "distinct-count"
  val NULL_COUNT = "null-count"
  val AVG_LENGTH = "avg-length"
  val MAX_LENGTH = "max-length"
  val MAX_VALUE = "max-value"
  val MIN_VALUE = "min-value"

  // utilities

  def normalizeColumnStats(columnStats: ColumnStats): Map[String, String] = {
    val stats = mutable.HashMap[String, String]()
    if (columnStats.ndv != null) {
      stats += DISTINCT_COUNT -> columnStats.ndv.toString
    }
    if (columnStats.nullCount != null) {
      stats += NULL_COUNT -> columnStats.nullCount.toString
    }
    if (columnStats.avgLen != null) {
      stats += AVG_LENGTH -> columnStats.avgLen.toString
    }
    if (columnStats.maxLen != null) {
      stats += MAX_LENGTH -> columnStats.maxLen.toString
    }
    if (columnStats.max != null) {
      stats += MAX_VALUE -> columnStats.max.toString
    }
    if (columnStats.min != null) {
      stats += MIN_VALUE -> columnStats.min.toString
    }
    stats.toMap
  }

  def validateColumnStats(properties: DescriptorProperties, key: String): Unit = {

    // filter for number of columns
    val columnCount = properties.getIndexedProperty(key, NAME).size

    for (i <- 0 until columnCount) {
      properties.validateString(s"$key.$i.$NAME", isOptional = false, minLen = 1)
      properties.validateLong(s"$key.$i.$DISTINCT_COUNT", isOptional = true, min = 0L)
      properties.validateLong(s"$key.$i.$NULL_COUNT", isOptional = true, min = 0L)
      properties.validateDouble(s"$key.$i.$AVG_LENGTH", isOptional = true, min = 0.0)
      properties.validateInt(s"$key.$i.$MAX_LENGTH", isOptional = true, min = 0)
      properties.validateDouble(s"$key.$i.$MAX_VALUE", isOptional = true, min = 0.0)
      properties.validateDouble(s"$key.$i.$MIN_VALUE", isOptional = true, min = 0.0)
    }
  }

  def readColumnStats(properties: DescriptorProperties, key: String): Map[String, ColumnStats] = {

    // filter for number of columns
    val columnCount = properties.getIndexedProperty(key, NAME).size

    val stats = for (i <- 0 until columnCount) yield {
      val name = toScala(properties.getOptionalString(s"$key.$i.$NAME")).getOrElse(
        throw new ValidationException(s"Could not find name of property '$key.$i.$NAME'."))

      val stats = ColumnStats(
        properties.getOptionalLong(s"$key.$i.$DISTINCT_COUNT").orElse(null),
        properties.getOptionalLong(s"$key.$i.$NULL_COUNT").orElse(null),
        properties.getOptionalDouble(s"$key.$i.$AVG_LENGTH").orElse(null),
        properties.getOptionalInt(s"$key.$i.$MAX_LENGTH").orElse(null),
        properties.getOptionalDouble(s"$key.$i.$MAX_VALUE").orElse(null),
        properties.getOptionalDouble(s"$key.$i.$MIN_VALUE").orElse(null)
      )

      name -> stats
    }

    stats.toMap
  }
}
