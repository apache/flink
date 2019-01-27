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

package org.apache.flink.table.plan.schema

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource

import java.util

import scala.collection.mutable.ArrayBuffer

/** Abstract class which define the interfaces required to convert a [[TableSource]] to
  * a Calcite Table */
abstract class TableSourceTable(
    val tableSource: TableSource,
    val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends FlinkTable {

  /**
    * Returns statistics of current table.
    * Note: If there is no available tableStats yet, try to fetch the table Stats by calling
    * getTableStats method of tableSource.
    *
    * @return statistics of current table
    */
  override def getStatistic: FlinkStatistic = {
    // Currently, we could get more exact TableStats by AnalyzeStatistic#generateTableStats
    // and update it by TableEnvironment#alterTableStats.
    // So the default tableStats should be prior to the stats from TableSource.
    val stats = if (statistic != null && statistic.getTableStats != null) {
      statistic.getTableStats
    } else {
      tableSource.getTableStats
    }
    val statisticBuilder = FlinkStatistic.builder.statistic(statistic).tableStats(stats)
    val primaryKeys = tableSource.getTableSchema.getPrimaryKeys
    val uniqueKeys = tableSource.getTableSchema.getUniqueKeys
    if (primaryKeys.nonEmpty || uniqueKeys.nonEmpty) {
      val keyBuffer = new ArrayBuffer[util.Set[String]]()
      if (!primaryKeys.isEmpty) {
        keyBuffer.append(ImmutableSet.copyOf(primaryKeys))
      }
      uniqueKeys.foreach {
        case uniqueKey: Array[String] => keyBuffer.append(ImmutableSet.copyOf(uniqueKey))
      }
      statisticBuilder.uniqueKeys(ImmutableSet.copyOf(keyBuffer.toArray))
    }
    statisticBuilder.build()
  }

  /**
   * replace table source with the given one, and create a new table source table.
   * @param tableSource tableSource to replace.
   * @return new TableSourceTable
   */
  def replaceTableSource(tableSource: TableSource): TableSourceTable
}
