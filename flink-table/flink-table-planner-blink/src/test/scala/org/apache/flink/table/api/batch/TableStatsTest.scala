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

package org.apache.flink.table.api.batch

import org.apache.flink.table.api.{DataTypes, PlannerConfigOptions, TableConfigOptions, TableSchema}
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.{TableTestBase, TestTableSource}
import org.junit.Test

class TableStatsTest extends TableTestBase {

  @Test
  def testTableStatsWithSmallRightTable(): Unit = {
    val util = batchTestUtil()
    util.tableEnv.registerTableSource("x", createTableX(new TableStats(10000000L)))
    util.tableEnv.registerTableSource("y", createTableY(new TableStats(100L)))
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    util.tableEnv.getConfig.getConf.setLong(
      PlannerConfigOptions.SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD, 5000)
    val sqlQuery =
      """
        |SELECT * FROM x, y WHERE a = d AND c LIKE 'He%'
      """.stripMargin
    // right table should be broadcast
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTableStatsWithSmallLeftTable(): Unit = {
    val util = batchTestUtil()
    util.tableEnv.registerTableSource("x", createTableX(new TableStats(100L)))
    util.tableEnv.registerTableSource("y", createTableY(new TableStats(10000000L)))
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    util.tableEnv.getConfig.getConf.setLong(
      PlannerConfigOptions.SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD, 5000)
    val sqlQuery =
      """
        |SELECT * FROM x, y WHERE a = d AND c LIKE 'He%'
      """.stripMargin
    // left table should be broadcast
    util.verifyPlan(sqlQuery)
  }

  private def createTableX(tableStats: TableStats): TableSource[_] = {
    val schema = TableSchema.builder()
      .field("a", DataTypes.INT())    // 4
      .field("b", DataTypes.BIGINT()) // 8
      .field("c", DataTypes.STRING()) // 12
      .build()
    new TestTableSource(true, schema, Option(tableStats))
  }

  private def createTableY(tableStats: TableStats): TableSource[_] = {
    val schema = TableSchema.builder()
      .field("d", DataTypes.INT())
      .field("e", DataTypes.BIGINT())
      .field("f", DataTypes.STRING())
      .build()
    new TestTableSource(true, schema, Option(tableStats))
  }
}
