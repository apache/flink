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

import java.lang.Long
import java.util

/**
  * Table statistics
  *
  * @param rowCount cardinality of table, the value is null if not available
  * @param colStats statistics of table columns, the value is empty map if not available.
  *                 default value is empty map.
  */
case class TableStats(
    rowCount: Long,
    colStats: util.Map[String, ColumnStats] = new util.HashMap()) {

  require(rowCount == null || rowCount >= 0L, "rowCount cannot be negative!")
  require(colStats != null, "column statistic cannot be null!")

  override def toString: String = {
    val rowCountStr = if (rowCount != null) s"rowCount=$rowCount" else ""
    val colStatsStr = if (!colStats.isEmpty) s"colStats=$colStats" else ""

    s"TableStats{${Seq(rowCountStr, colStatsStr).mkString(", ")}}"
  }

}

object TableStats {

  val UNKNOWN = new TableStats(null)

  def builder(): Builder = new Builder()

  class Builder {
    private var rowCount: Long = _
    private var colStats: util.Map[String, ColumnStats] = new util.HashMap()

    def rowCount(rowCount: Long): Builder = {
      this.rowCount = rowCount
      this
    }

    def colStats(colStats: util.Map[String, ColumnStats]): Builder = {
      this.colStats = colStats
      this
    }

    def tableStats(stats: TableStats): Builder = {
      require(stats != null, "input TableStats cannot be null!")
      this.rowCount = stats.rowCount
      this.colStats = stats.colStats
      this
    }

    def build(): TableStats = {
      new TableStats(rowCount, colStats)
    }
  }

}
