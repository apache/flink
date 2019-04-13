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

package org.apache.flink.table.plan.stats

import org.apache.calcite.rel.{RelCollation, RelDistribution, RelReferentialConstraint}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet

import java.lang.Double
import java.util

import scala.collection.JavaConversions._

/**
  * The class provides statistics for a [[org.apache.flink.table.plan.schema.FlinkTable]].
  */
class FlinkStatistic(
    tableStats: TableStats,
    uniqueKeys: util.Set[_ <: util.Set[String]] = null)
  extends Statistic {

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
    if (tableStats != null && tableStats.getColumnStats != null) {
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
    * Returns the number of rows of the table.
    *
    * @return The number of rows of the table.
    */
  override def getRowCount: Double = {
    if (tableStats != null) {
      tableStats.getRowCount.toDouble
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
}

/**
  * Methods to create FlinkStatistic.
  */
object FlinkStatistic {

  /** Represents a FlinkStatistic that knows nothing about a table */
  val UNKNOWN: FlinkStatistic = new FlinkStatistic(null)

  class Builder {

    private var tableStats: TableStats = _
    private var uniqueKeys: util.Set[_ <: util.Set[String]] = _

    def tableStats(tableStats: TableStats): Builder = {
      this.tableStats = tableStats
      this
    }

    def uniqueKeys(uniqueKeys: util.Set[_ <: util.Set[String]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def statistic(statistic: FlinkStatistic): Builder = {
      require(statistic != null, "input statistic cannot be null!")
      this.tableStats = statistic.getTableStats
      this.uniqueKeys = statistic.getUniqueKeys
      this
    }

    def build(): FlinkStatistic = {
      if (tableStats == null && uniqueKeys == null) {
        UNKNOWN
      } else {
        new FlinkStatistic(tableStats, uniqueKeys)
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
