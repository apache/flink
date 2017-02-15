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
import java.util.{Collections, List}

import org.apache.calcite.rel.{RelCollation, RelDistribution}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet

/**
  * The class is responsible for provide statistics for flink table.
  *
  * @param tableStats
  */
class FlinkStatistics(tableStats: Option[TableStats]) extends Statistic {

  /**
    * Get table stats
    *
    * @return table stats
    */
  def getTableStats: TableStats = tableStats.getOrElse(null)

  /**
    * Get stats of specified column
    *
    * @param columnName which column to get stats
    * @return stats of specified column
    */
  def getColumnStats(columnName: String): ColumnStats = tableStats match {
    case Some(tStats) => tStats.colStats.get(columnName)
    case None => null
  }

  /**
    * Get number of rows in the table
    *
    * @return number of rows in the table
    */
  override def getRowCount: Double = tableStats match {
    case Some(tStats) => tStats.rowCount.toDouble
    case None => null
  }

  override def getCollations: List[RelCollation] = Collections.emptyList()

  override def isKey(columns: ImmutableBitSet): Boolean = false

  override def getDistribution: RelDistribution = null

}

/**
  * Utility functions regarding FlinkStatistic
  */
object FlinkStatistics {

  /** Represents a FlinkStatistic that knows nothing about a table */
  val UNKNOWN: FlinkStatistics = new FlinkStatistics(None)

  /**
    * Returns a Flinkstatistic with a given tableStats.
    *
    * @param tableStats
    * @return
    */
  def of(tableStats: TableStats): FlinkStatistics = new FlinkStatistics(Some(tableStats))

}
