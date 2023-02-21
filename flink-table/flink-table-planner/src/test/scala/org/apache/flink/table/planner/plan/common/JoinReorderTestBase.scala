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
import org.apache.flink.table.planner.plan.rules.logical.JoinDeriveNullFilterRule
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.{Before, Test}
import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConversions._

/**
 * Ths base plan test for join reorder. This class will test
 * [[org.apache.flink.table.planner.plan.rules.logical.FlinkBushyJoinReorderRule]] and
 * [[org.apache.calcite.rel.rules.LoptOptimizeJoinRule]] together by changing the factor
 * isBushyJoinReorder.
 */
abstract class JoinReorderTestBase(isBushyJoinReorder: Boolean) extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil

  protected def getTableTestUtil: TableTestUtil

  @Before
  def setup(): Unit = {
    val types = Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)

    util.addTableSource(
      "T1",
      types,
      Array("a1", "b1", "c1"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            100000L,
            Map(
              "a1" -> new ColumnStats(1000000L, 0L, 4.0, 4, null, null),
              "b1" -> new ColumnStats(50L, 0L, 8.0, 8, null, null)
            )))
        .build()
    )

    util.addTableSource(
      "T2",
      types,
      Array("a2", "b2", "c2"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            10000L,
            Map(
              "a2" -> new ColumnStats(100L, 0L, 4.0, 4, null, null),
              "b2" -> new ColumnStats(500000L, 0L, 8.0, 8, null, null)
            )))
        .build()
    )

    util.addTableSource(
      "T3",
      types,
      Array("a3", "b3", "c3"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            1000L,
            Map(
              "a3" -> new ColumnStats(5L, 0L, 4.0, 4, null, null),
              "b3" -> new ColumnStats(50L, 0L, 8.0, 8, null, null)
            )))
        .build()
    )

    util.addTableSource(
      "T4",
      types,
      Array("a4", "b4", "c4"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            100L,
            Map(
              "a4" -> new ColumnStats(100L, 0L, 4.0, 4, null, null),
              "b4" -> new ColumnStats(500000L, 0L, 8.0, 8, null, null)
            )))
        .build()
    )

    util.addTableSource(
      "T5",
      types,
      Array("a5", "b5", "c5"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            500000L,
            Map(
              "a5" -> new ColumnStats(200000L, 0L, 4.0, 4, null, null),
              "b5" -> new ColumnStats(200L, 0L, 8.0, 8, null, null)
            )))
        .build()
    )

    util.getTableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, Boolean.box(true))

    if (!isBushyJoinReorder) {
      util.getTableEnv.getConfig
        .set(
          OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD,
          Integer.valueOf(3))
    } else {
      util.getTableEnv.getConfig
        .set(
          OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD,
          Integer.valueOf(1000))
    }

  }

  @Test
  def testStarJoinCondition1(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a1 = a3 AND a1 = a4 AND a1 = a5
         """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testStarJoinCondition2(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b1 = b3 AND b1 = b4 AND b1 = b5
         """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testBushyJoinCondition1(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a2 = a3 AND a1 = a4 AND a3 = a5
         """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testBushyJoinCondition2(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b2 = b3 AND b1 = b4 AND b3 = b5
         """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testWithoutColumnStats(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE c1 = c2 AND c1 = c3 AND c2 = c4 AND c1 = c5
         """.stripMargin
    util.verifyRelPlan(sql)
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
    util.verifyRelPlan(sql)
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
    util.verifyRelPlan(sql)
  }

  @Test
  def testAllInnerJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a2 = a4
         |   JOIN T5 ON a1 = a5
         """.stripMargin
    // can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerAndLeftOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a1 = a5
         """.stripMargin
    // T1, T2, T3 T4 T5 can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerAndRightOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a2 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3, T4, T5 can reorder
    util.verifyRelPlan(sql)
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
    util.verifyRelPlan(sql)
  }

  @Test
  def testAllLeftOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   LEFT OUTER JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a1 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a1 = a5
         """.stripMargin
    // can reorder. Left outer join will be converted to one multi
    // set by FlinkJoinToMultiJoinRule.
    util.verifyRelPlan(sql)
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
    // can not reorder.
    util.verifyRelPlan(sql)
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
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a1 = a3
         |   JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3, T4, T5 can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testLeftOuterJoinInnerJoinLeftOuterJoinInnerJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   LEFT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a1 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3, T4, T5 can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinRightOuterJoinInnerJoinRightOuterJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   RIGHT OUTER JOIN T3 ON a1 = a3
         |   JOIN T4 ON a1 = a4
         |   RIGHT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // T1 and T2 can not reorder, but MJ(T1, T2), T3, T4 can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testRightOuterJoinInnerJoinRightOuterJoinInnerJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a1 = a3
         |   RIGHT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3 can reorder, and MJ(T1, T2, T3), T4, T5 can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinSemiJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   WHERE a1 IN (SELECT a5 FROM T5)
         """.stripMargin
    // can not reorder. Semi join will support join order in future.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinAntiJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   WHERE NOT EXISTS (SELECT a5 FROM T5 WHERE a1 = a5)
         """.stripMargin
    // can not reorder
    util.verifyRelPlan(sql)
  }

  @Test
  def testCrossJoin(): Unit = {
    val sql = "SELECT * FROM T1, T2, T3, T4, T5"
    // All table can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinCrossJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1,
         |   (SELECT * FROM T2 JOIN T3 ON a2 = a3) tab1, T4
         """.stripMargin
    // All table can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinLeftOuterJoinCrossJoin(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1,
         |   (SELECT * FROM T2 LEFT JOIN T3 ON a2 = a3 JOIN T4 ON a2 = a4) tab1, T5
         """.stripMargin
    // All table can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testInnerJoinWithBushyTypeJoinCondition(): Unit = {
    // This case is to test whether can build a bushy join tree.
    // If variable isBushyJoinReorder is true, it can be built to
    // a bushy join tree. Otherwise the join reorder tree is not bushy
    // join tree.
    val sql =
      s"""
         |SELECT * FROM
         |(SELECT * FROM T1 JOIN T2 ON T1.b1 = T2.b2) tab1 JOIN
         |(SELECT * FROM T3 JOIN T4 ON T3.b3 = T4.b4) tab2
         |ON tab1.b2 = tab2.b4
         """.stripMargin
    // All table can reorder.
    util.verifyRelPlan(sql)
  }

  @Test
  def testDeriveNullFilterAfterJoinReorder(): Unit = {
    val types = Array[TypeInformation[_]](Types.INT, Types.LONG)
    val builderA = ColumnStats.Builder
      .builder()
      .setNdv(200000L)
      .setNullCount(50000L)
      .setAvgLen(4.0)
      .setMaxLen(4)
    val builderB = ColumnStats.Builder
      .builder()
      .setNdv(100000L)
      .setNullCount(0L)
      .setAvgLen(8.0)
      .setMaxLen(8)

    util.addTableSource(
      "T6",
      types,
      Array("a6", "b6"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            500000L,
            Map(
              "a6" -> builderA.build(),
              "b6" -> builderB.build()
            )))
        .build())

    util.addTableSource(
      "T7",
      types,
      Array("a7", "b7"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            500000L,
            Map(
              "a7" -> builderA.build(),
              "b7" -> builderB.build()
            )))
        .build())

    util.addTableSource(
      "T8",
      types,
      Array("a8", "b8"),
      FlinkStatistic
        .builder()
        .tableStats(
          new TableStats(
            500000L,
            Map(
              "a8" -> builderA.build(),
              "b8" -> builderB.build()
            )))
        .build())

    util.getTableEnv.getConfig
      .set(JoinDeriveNullFilterRule.TABLE_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD, Long.box(10000))
    val sql =
      s"""
         |SELECT * FROM T6
         |   INNER JOIN T7 ON b6 = b7
         |   INNER JOIN T8 ON a6 = a8
         |""".stripMargin
    util.verifyRelPlan(sql)
  }
}

object JoinReorderTestBase {
  @Parameterized.Parameters(name = "isBushyJoinReorder={0}")
  def parameters(): util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
