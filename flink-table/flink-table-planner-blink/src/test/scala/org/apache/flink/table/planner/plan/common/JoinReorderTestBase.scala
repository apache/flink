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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.{Before, Test}

import scala.collection.JavaConversions._

abstract class JoinReorderTestBase extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil

  protected def getTableTestUtil: TableTestUtil

  @Before
  def setup(): Unit = {
    val types = Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)

    util.addTableSource("T1", types, Array("a1", "b1", "c1"), FlinkStatistic.builder()
      .tableStats(new TableStats(1000000L, Map(
        "a1" -> new ColumnStats(1000000L, 0L, 4.0, 4, null, null),
        "b1" -> new ColumnStats(10L, 0L, 8.0, 8, null, null)
      ))).build())

    util.addTableSource("T2", types, Array("a2", "b2", "c2"), FlinkStatistic.builder()
      .tableStats(new TableStats(10000L, Map(
        "a2" -> new ColumnStats(100L, 0L, 4.0, 4, null, null),
        "b2" -> new ColumnStats(5000L, 0L, 8.0, 8, null, null)
      ))).build())

    util.addTableSource("T3", types, Array("a3", "b3", "c3"), FlinkStatistic.builder()
      .tableStats(new TableStats(10L, Map(
        "a3" -> new ColumnStats(5L, 0L, 4.0, 4, null, null),
        "b3" -> new ColumnStats(2L, 0L, 8.0, 8, null, null)
      ))).build())

    util.addTableSource("T4", types, Array("a4", "b4", "c4"), FlinkStatistic.builder()
      .tableStats(new TableStats(100L, Map(
        "a4" -> new ColumnStats(100L, 0L, 4.0, 4, null, null),
        "b4" -> new ColumnStats(20L, 0L, 8.0, 8, null, null)
      ))).build())

    util.addTableSource("T5", types, Array("a5", "b5", "c5"), FlinkStatistic.builder()
      .tableStats(new TableStats(500000L, Map(
        "a5" -> new ColumnStats(200000L, 0L, 4.0, 4, null, null),
        "b5" -> new ColumnStats(200L, 0L, 8.0, 8, null, null)
      ))).build())

    util.getTableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true)
  }

  @Test
  def testStarJoinCondition1(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a1 = a3 AND a1 = a4 AND a1 = a5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testStarJoinCondition2(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b1 = b3 AND b1 = b4 AND b1 = b5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testBushyJoinCondition1(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a2 = a3 AND a1 = a4 AND a3 = a5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testBushyJoinCondition2(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b2 = b3 AND b1 = b4 AND b3 = b5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testWithoutColumnStats(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE c1 = c2 AND c1 = c3 AND c2 = c4 AND c1 = c5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testJoinWithProject(): Unit = {
    val sql =
      s"""
         |WITH V1 AS (SELECT b1, a1, a2, c2 FROM T1 JOIN T2 ON a1 = a2),
         |     V2 AS (SELECT a3, b1, a1, c2, c3 FROM V1 JOIN T3 ON a2 = a3),
         |     V3 AS (SELECT a3, b1, a1, c2, c3, a4, b4 FROM T4 JOIN V2 ON a1 = a4)
         |
         |SELECT * FROM V3, T5 where a4 = a5
         """.stripMargin
    // can not reorder now
    util.verifyPlan(sql)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val sql =
      s"""
         |WITH V1 AS (SELECT * FROM T1 JOIN T2 ON a1 = a2 WHERE b1 * b2 > 10),
         |     V2 AS (SELECT * FROM V1 JOIN T3 ON a2 = a3 WHERE b1 * b3 < 2000),
         |     V3 AS (SELECT * FROM T4 JOIN V2 ON a3 = a4 WHERE b2 + b4 > 100)
         |
         |SELECT * FROM V3, T5 WHERE a4 = a5 AND b5 < 15
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testInnerAndLeftOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3 can reorder
    util.verifyPlan(sql)
  }

  @Test
  def testInnerAndRightOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T3, T4, T5 can reorder
    util.verifyPlan(sql)
  }

  @Test
  def testInnerAndFullOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   FULL OUTER JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testAllLeftOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   LEFT OUTER JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a2 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can not reorder
    util.verifyPlan(sql)
  }

  @Test
  def testAllRightOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   RIGHT OUTER JOIN T3 ON a2 = a3
         |   RIGHT OUTER JOIN T4 ON a1 = a4
         |   RIGHT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can not reorder
    util.verifyPlan(sql)
  }

  @Test
  def testAllFullOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   FULL OUTER JOIN T2 ON a1 = a2
         |   FULL OUTER JOIN T3 ON a1 = a3
         |   FULL OUTER JOIN T4 ON a1 = a4
         |   FULL OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can not reorder
    util.verifyPlan(sql)
  }
}
