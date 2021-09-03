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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.catalog.{ResolvedSchema, UniqueConstraint}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.plan.`trait`.{RelModifiedMonotonicity, RelWindowProperties}

import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.{RelCollation, RelDistribution, RelReferentialConstraint}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet

import javax.annotation.Nullable

import java.util
import java.util.{HashSet, Optional, Set}

import scala.collection.JavaConversions._

/**
  * The class provides statistics for a [[org.apache.calcite.schema.Table]].
  */
class FlinkStatistic(
    tableStats: TableStats,
    uniqueKeys: util.Set[_ <: util.Set[String]] = null,
    relModifiedMonotonicity: RelModifiedMonotonicity = null,
    relWindowProperties: RelWindowProperties = null)
  extends Statistic {

  require(tableStats != null, "tableStats should not be null")
  require(uniqueKeys == null || !uniqueKeys.exists(keys => keys == null || keys.isEmpty),
    "uniqueKeys contains invalid elements!")

  /**
    * Returns the table statistics.
    *
    * @return The table statistics
    */
  def getTableStats: TableStats = tableStats

  /**
    * Returns the stats of the specified the column.
    *
    * @param columnName The name of the column for which the stats are requested.
    * @return The stats of the specified column.
    */
  def getColumnStats(columnName: String): ColumnStats = {
    if (tableStats != TableStats.UNKNOWN && tableStats.getColumnStats != null) {
      tableStats.getColumnStats.get(columnName)
    } else {
      null
    }
  }

  /**
    * Returns the table uniqueKeys.
    * @return
    */
  def getUniqueKeys: util.Set[_ <: util.Set[String]] = uniqueKeys

  /**
    * Returns the modified monotonicity of the table
    */
  def getRelModifiedMonotonicity: RelModifiedMonotonicity = relModifiedMonotonicity

  /**
   * Returns the window properties of the table
   */
  def getRelWindowProperties: RelWindowProperties = relWindowProperties

  /**
    * Returns the number of rows of the table.
    *
    * @return The number of rows of the table.
    */
  override def getRowCount: java.lang.Double = {
    if (tableStats != TableStats.UNKNOWN) {
      val rowCount = tableStats.getRowCount.toDouble
      // rowCount requires non-negative number
      if (rowCount >= 0) {
        rowCount
      } else {
        null
      }
    } else {
      null
    }
  }

  override def getCollations: util.List[RelCollation] = util.Collections.emptyList()

  /**
    * Returns whether the given columns are a key or a superset of a unique key
    * of this table.
    *
    * Note: Do not call this method!
    * Use [[org.apache.calcite.rel.metadata.RelMetadataQuery]].areRowsUnique if need.
    * Because columns in original uniqueKey may not exist in RowType after project pushDown, however
    * the RowType cannot be available here.
    *
    * @param columns Ordinals of key columns
    * @return if bit mask represents a unique column set; false if not (or
    *         if no metadata is available).
    */
  override def isKey(columns: ImmutableBitSet): Boolean = false

  override def getDistribution: RelDistribution = null

  override def getReferentialConstraints: util.List[RelReferentialConstraint] =
    util.Collections.emptyList()

  override def toString: String = {
    val builder = new StringBuilder
    if (tableStats != TableStats.UNKNOWN) {
      builder.append(s"TableStats: " +
        s"{rowCount: ${tableStats.getRowCount}, " +
        s"columnStats: ${tableStats.getColumnStats}}, ")
    }
    if (uniqueKeys != null) {
      builder.append(s"uniqueKeys: $uniqueKeys, ")
    }
    if (relModifiedMonotonicity != null) {
      builder.append(relModifiedMonotonicity.toString).append(", ")
    }
    if (relWindowProperties != null) {
      builder.append(relWindowProperties.toString).append(", ")
    }

    if (builder.nonEmpty && builder.length() > 2) {
      // delete `, ` if build is not empty
      builder.delete(builder.length() - 2, builder.length())
    }
    builder.toString()
  }

  override def getKeys: util.List[ImmutableBitSet] = ImmutableList.of()
}

/**
  * Methods to create FlinkStatistic.
  */
object FlinkStatistic {

  /** Represents a FlinkStatistic that knows nothing about a table */
  val UNKNOWN: FlinkStatistic = new FlinkStatistic(TableStats.UNKNOWN)

  class Builder {

    private var tableStats: TableStats = TableStats.UNKNOWN
    private var uniqueKeys: util.Set[_ <: util.Set[String]] = _
    private var relModifiedMonotonicity: RelModifiedMonotonicity = _
    private var windowProperties: RelWindowProperties = _

    def tableStats(tableStats: TableStats): Builder = {
      if (tableStats != null) {
        this.tableStats = tableStats
      } else {
        this.tableStats = TableStats.UNKNOWN
      }
      this
    }

    def uniqueKeys(@Nullable uniqueKeys: util.Set[_ <: util.Set[String]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def uniqueKeys(@Nullable uniqueConstraint: UniqueConstraint): Builder = {
      val uniqueKeySet = if (uniqueConstraint == null) {
        null
      } else {
        val uniqueKey = new util.HashSet[String](uniqueConstraint.getColumns)
        val uniqueKeySet = new util.HashSet[util.Set[String]]
        uniqueKeySet.add(uniqueKey)
        uniqueKeySet
      }
      uniqueKeys(uniqueKeySet)
    }

    def relModifiedMonotonicity(monotonicity: RelModifiedMonotonicity): Builder = {
      this.relModifiedMonotonicity = monotonicity
      this
    }

    def relWindowProperties(windowProperties: RelWindowProperties): Builder = {
      this.windowProperties = windowProperties
      this
    }

    def statistic(statistic: FlinkStatistic): Builder = {
      require(statistic != null, "input statistic cannot be null!")
      this.tableStats = statistic.getTableStats
      this.uniqueKeys = statistic.getUniqueKeys
      this.relModifiedMonotonicity = statistic.getRelModifiedMonotonicity
      this
    }

    def build(): FlinkStatistic = {
      if (tableStats == TableStats.UNKNOWN &&
        uniqueKeys == null &&
        relModifiedMonotonicity == null &&
        windowProperties == null) {
        UNKNOWN
      } else {
        new FlinkStatistic(tableStats, uniqueKeys, relModifiedMonotonicity)
      }
    }
  }

  /**
    * Return a new builder that builds a [[FlinkStatistic]].
    *
    * @return a new builder to build a [[FlinkStatistic]]
    */
  def builder(): Builder = new Builder
}
