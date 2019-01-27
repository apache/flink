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

import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity

import org.apache.calcite.rel.{RelCollation, RelDistribution, RelReferentialConstraint}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet

import java.lang.Double
import java.util

import scala.collection.JavaConversions._

/**
  * The class provides statistics for a [[org.apache.flink.table.plan.schema.FlinkTable]].
  *
  * @param tableStats The table statistics.
  * @param uniqueKeys unique keys of table.
  *                   null means miss this information;
  *                   empty set means does not exist unique key of the table;
  *                   non-empty set means set of the unique keys.
  * @param skewInfo statistics of skewedColNames and skewedColValues.
  */
class FlinkStatistic private(tableStats: Option[TableStats],
    uniqueKeys: util.Set[_ <: util.Set[String]] = null,
    skewInfo: util.Map[String, util.List[AnyRef]] = null,
    monotonicity: RelModifiedMonotonicity = null)
  extends Statistic {

  require(uniqueKeys == null || !uniqueKeys.exists(keys => keys == null || keys.isEmpty),
    "uniqueKeys contains invalid elements!")

  /**
    * Returns the table statistics.
    *
    * @return The table statistics
    */
  def getTableStats: TableStats = tableStats.orNull

  /**
    * Returns the stats of the specified the column.
    *
    * @param columnName The name of the column for which the stats are requested.
    * @return The stats of the specified column.
    */
  def getColumnStats(columnName: String): ColumnStats = tableStats match {
    case Some(tStats) if tStats.colStats != null => tStats.colStats.get(columnName)
    case _ => null
  }

  /**
    * Returns the table uniqueKeys.
    * @return
    */
  def getUniqueKeys: util.Set[_ <: util.Set[String]] = uniqueKeys

  /**
    * Returns the modified monotonicity of the table
    */
  def getRelModifiedMonotonicity: RelModifiedMonotonicity = monotonicity

  /**
    * Returns the skew info.
    * @return
    */
  def getSkewInfo: util.Map[String, util.List[AnyRef]] = skewInfo

  /**
    * Returns the number of rows of the table.
    *
    * @return The number of rows of the table.
    */
  override def getRowCount: Double = tableStats match {
    case Some(tStats) if tStats.rowCount != null => tStats.rowCount.toDouble
    case _ => null
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
  val UNKNOWN: FlinkStatistic = new FlinkStatistic(None)

  class Builder {

    private var tableStats: TableStats = null
    private var uniqueKeys: util.Set[_ <: util.Set[String]] = null
    private var skewInfo: util.Map[String, util.List[AnyRef]] = null
    private var monotonicity: RelModifiedMonotonicity = null

    def tableStats(tableStats: TableStats): Builder = {
      this.tableStats = tableStats
      this
    }

    def uniqueKeys(uniqueKeys: util.Set[_ <: util.Set[String]]): Builder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def skewInfo(skewInfo: util.Map[String, util.List[AnyRef]]): Builder = {
      this.skewInfo = skewInfo
      this
    }

    def monotonicity(monotonicity: RelModifiedMonotonicity): Builder = {
      this.monotonicity = monotonicity
      this
    }

    def statistic(statistic: FlinkStatistic): Builder = {
      this.tableStats = statistic.getTableStats
      this.uniqueKeys = statistic.getUniqueKeys
      this.skewInfo = statistic.getSkewInfo
      this.monotonicity = statistic.getRelModifiedMonotonicity
      this
    }

    def build(): FlinkStatistic = {
      if (tableStats == null && uniqueKeys == null && skewInfo == null && monotonicity == null) {
        UNKNOWN
      } else {
        new FlinkStatistic(Option(tableStats), uniqueKeys, skewInfo, monotonicity)
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
