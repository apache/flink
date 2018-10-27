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

import org.apache.flink.table.descriptors.StatisticsValidator._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Statistics descriptor for describing table stats.
  */
class Statistics extends Descriptor {

  private var rowCount: Option[Long] = None
  private val columnStats: mutable.LinkedHashMap[String, mutable.Map[String, String]] =
    mutable.LinkedHashMap[String, mutable.Map[String, String]]()

  /**
    * Sets the statistics from a [[TableStats]] instance.
    *
    * This method overwrites all existing statistics.
    *
    * @param tableStats the table statistics
    */
  def tableStats(tableStats: TableStats): Statistics = {
    rowCount(tableStats.rowCount)
    columnStats.clear()
    tableStats.colStats.asScala.foreach { case (col, stats) =>
        columnStats(col, stats)
    }
    this
  }

  /**
    * Sets statistics for the overall row count. Required.
    *
    * @param rowCount the expected number of rows
    */
  def rowCount(rowCount: Long): Statistics = {
    this.rowCount = Some(rowCount)
    this
  }

  /**
    * Sets statistics for a column. Overwrites all existing statistics for this column.
    *
    * @param columnName the column name
    * @param columnStats expected statistics for the column
    */
  def columnStats(columnName: String, columnStats: ColumnStats): Statistics = {
    val map = mutable.Map(normalizeColumnStats(columnStats).toSeq: _*)
    this.columnStats.put(columnName, map)
    this
  }

  /**
    * Sets the number of distinct values statistic for the given column.
    */
  def columnDistinctCount(columnName: String, ndv: Long): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(DISTINCT_COUNT, ndv.toString)
    this
  }

  /**
    * Sets the number of null values statistic for the given column.
    */
  def columnNullCount(columnName: String, nullCount: Long): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(NULL_COUNT, nullCount.toString)
    this
  }

  /**
    * Sets the average length statistic for the given column.
    */
  def columnAvgLength(columnName: String, avgLen: Double): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(AVG_LENGTH, avgLen.toString)
    this
  }

  /**
    * Sets the maximum length statistic for the given column.
    */
  def columnMaxLength(columnName: String, maxLen: Integer): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(MAX_LENGTH, maxLen.toString)
    this
  }

  /**
    * Sets the maximum value statistic for the given column.
    */
  def columnMaxValue(columnName: String, max: Number): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(MAX_VALUE, max.toString)
    this
  }

  /**
    * Sets the minimum value statistic for the given column.
    */
  def columnMinValue(columnName: String, min: Number): Statistics = {
    this.columnStats
      .getOrElseUpdate(columnName, mutable.HashMap())
      .put(MIN_VALUE, min.toString)
    this
  }

  /**
    * Converts this descriptor into a set of properties.
    */
  final override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()

    properties.putInt(STATISTICS_PROPERTY_VERSION, 1)
    rowCount.foreach(rc => properties.putLong(STATISTICS_ROW_COUNT, rc))
    val namedStats = columnStats.map { case (name, stats) =>
      // name should not be part of the properties key
      (stats + (NAME -> name)).toMap.asJava
    }.toList.asJava
    properties.putIndexedVariableProperties(STATISTICS_COLUMNS, namedStats)

    properties.asMap()
  }
}

/**
  * Statistics descriptor for describing table stats.
  */
object Statistics {

  /**
    * Statistics descriptor for describing table stats.
    *
    * @deprecated Use `new Statistics()`.
    */
  @deprecated
  def apply(): Statistics = new Statistics()
}
