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

import java.lang.Double
import java.util
import java.util.{Collections, List}

import org.apache.calcite.rel.{RelCollation, RelDistribution, RelReferentialConstraint}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.schema.TableSourceTable

/**
  * The class provides statistics for a [[TableSourceTable]].
  *
  * @param tableStats The table statistics.
  */
class FlinkStatistic(tableStats: Option[TableStats]) extends Statistic {

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
    case Some(tStats) => tStats.getColumnStats.get(columnName)
    case None => null
  }

  /**
    * Returns the number of rows of the table.
    *
    * @return The number of rows of the table.
    */
  override def getRowCount: Double = tableStats match {
    case Some(tStats) => tStats.getRowCount.toDouble
    case None => null
  }

  override def getCollations: List[RelCollation] = Collections.emptyList()

  override def isKey(columns: ImmutableBitSet): Boolean = false

  override def getDistribution: RelDistribution = null

  override def getReferentialConstraints: util.List[RelReferentialConstraint] =
    Collections.emptyList()
}

/**
  * Methods to create FlinkStatistic.
  */
object FlinkStatistic {

  /** Represents a FlinkStatistic that knows nothing about a table */
  val UNKNOWN: FlinkStatistic = new FlinkStatistic(None)

  /**
    * Returns a FlinkStatistic with given table statistics.
    *
    * @param tableStats The table statistics.
    * @return The generated FlinkStatistic
    */
  def of(tableStats: TableStats): FlinkStatistic = new FlinkStatistic(Option(tableStats))

}
