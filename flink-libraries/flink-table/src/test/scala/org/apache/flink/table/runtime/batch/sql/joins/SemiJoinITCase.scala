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

package org.apache.flink.table.runtime.batch.sql.joins

import java.util

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.batch.sql.joins.JoinType._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection.Seq

@RunWith(classOf[Parameterized])
class SemiJoinITCase(
    expectedJoinType: JoinType,
    buildLeft: Boolean) extends BatchTestBase with JoinITCaseBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection(
      "leftT", SemiJoinITCase.leftT, INT_DOUBLE, "a, b", SemiJoinITCase.nullablesOfLeftT)
    registerCollection(
      "rightT", SemiJoinITCase.rightT, INT_DOUBLE, "c, d", SemiJoinITCase.nullablesOfRightT)
    registerCollection("rightUniqueKeyT", SemiJoinITCase.rightUniqueKeyT, INT_DOUBLE, "c, d")
    disableOtherJoinOpForJoin(tEnv, expectedJoinType)

    if (buildLeft) {
      tEnv.alterTableStats("leftT", Some(TableStats(2L)))
      tEnv.alterTableStats("rightT", Some(TableStats(200L)))
    } else {
      tEnv.alterTableStats("leftT", Some(TableStats(200L)))
      tEnv.alterTableStats("rightT", Some(TableStats(2L)))
    }
  }

  @Test
  def testSingleConditionLeftSemi(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)",
      Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null)))
  }

  @Test
  def testSemiWithDistinct(): Unit = {
    tEnv.getConfig.getConf.setDouble(
      TableConfigOptions.SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO, 0.8D)
    val stats = TableStats(7L, Map[String, ColumnStats](
      "c" -> ColumnStats(4L, 0L, 4.0D, 4, 99, 0),
      "c" -> ColumnStats(3L, 0L, 4.0D, 4, 99, 0)))
    tEnv.alterTableStats("rightT", Some(stats))
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)",
      Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null)))
  }

  @Test
  def testComposedConditionLeftSemi(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b < d)",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @Test
  def testSingleConditionLeftAnti(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)",
      Seq(row(1, 2.0), row(1, 2.0), row(null, null), row(null, 5.0)))
  }

  @Test
  def testSingleUniqueConditionLeftAnti(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE NOT EXISTS " +
        "(SELECT * FROM (SELECT DISTINCT c FROM rightT) WHERE a = c)",
      Seq(row(1, 2.0), row(1, 2.0), row(null, null), row(null, 5.0)))
  }

  @Test
  def testComposedConditionLeftAnti(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(6, null), row(null, 5.0), row(null, null)))
  }

  @Test
  def testComposedUniqueConditionLeftAnti(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightUniqueKeyT WHERE a = c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(null, null), row(null, 5.0), row(6, null)))
  }

  @Test
  def testSemiJoinTranspose(): Unit = {
    verifyPlanAndCheckResult("SELECT a, b FROM " +
      "(SELECT a, b, c FROM leftT, rightT WHERE a = c) lr " +
      "WHERE lr.a > 0 AND lr.c IN (SELECT c FROM rightUniqueKeyT WHERE d > 1)",
      Seq(row(2, 1.0), row(2, 1.0), row(2, 1.0), row(2, 1.0), row(3, 3.0))
    )
  }

  @Test
  def testFilterPushDownLeftSemi1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM (SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)) T WHERE T.b > 2",
      Seq(row(3, 3.0)))
  }

  @Test
  def testFilterPushDownLeftSemi2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM (SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT)) T WHERE T.b > 2",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testFilterPushDownLeftSemi3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
          "WHERE T.b > 2",
      Seq(row(3, 3.0)))
  }

  @Test
  def testJoinConditionPushDownLeftSemi1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b > 2)",
      Seq(row(3, 3.0)))
  }

  @Test
  def testJoinConditionPushDownLeftSemi2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE b > 2)",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftSemi3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)",
      Seq(row(3, 3.0)))
  }

  @Test
  def testFilterPushDownLeftAnti1(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE c < 3)) T " +
          "WHERE T.b > 2",
        Seq(row(3, 3.0)))
    }
  }

  @Test
  def testFilterPushDownLeftAnti2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM " +
            "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT where c > 10)) T " +
            "WHERE T.b > 2",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testFilterPushDownLeftAnti3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND c < 3)) T " +
          "WHERE T.b > 2",
      Seq(row(3, 3.0), row(null, 5.0)))
  }

  @Test
  def testFilterPushDownLeftAnti4(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
          "WHERE T.b > 2",
      Seq(row(null, 5.0)))
  }

  @Test
  def testJoinConditionPushDownLeftAnti1(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b > 2)",
        Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(null, null), row(6, null)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftAnti2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE b > 2)",
        Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(null, null), row(6, null)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftAnti3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND b > 1)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0),
        row(3, 3.0), row(null, null), row(6, null)))
  }

  @Test
  def testJoinConditionPushDownLeftAnti4(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0),
        row(null, null), row(null, 5.0), row(6, null)))
  }

  @Test
  def testInWithAggregate1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) FROM leftT WHERE b = d)",
      Seq(row(4, 1.0))
    )
  }

  @Test
  def testInWithAggregate2(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT t1 WHERE a IN (SELECT DISTINCT a FROM leftT t2 WHERE t1.b = t2.b)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(3, 3.0))
    )
  }

  @Test
  def testInWithAggregate3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE CAST(c/2 AS BIGINT) IN (SELECT COUNT(*) FROM leftT WHERE b = d)",
      Seq(row(2, 3.0), row(2, 3.0), row(4, 1.0))
    )
  }

  @Test
  def testInWithOver1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER " +
        "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
        "FROM leftT)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(6, null))
    )
  }

  @Test
  def testInWithOver2(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER" +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT GROUP BY a, b)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(6, null))
    )
  }

  @Test
  def testInWithOver3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER " +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT WHERE b = d)",
      Seq(row(4, 1.0))
    )
  }

  @Test
  def testInWithOver4(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER" +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT WHERE b = d GROUP BY a, b)",
      Seq()
    )
  }

  @Test
  def testExistsWithOver1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b = d)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(null, 5.0))
    )
  }

  @Test
  def testExistsWithOver2(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b > d)",
        Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
      )
    }
  }

  @Test
  def testExistsWithOver3(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b = d GROUP BY a)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(null, 5.0))
    )
  }

  @Test
  def testExistsWithOver4(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b>d GROUP BY a)",
        Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
      )
    }
  }

  @Test
  def testInWithNonEqualityCorrelationCondition1(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM rightT WHERE c IN (SELECT a FROM leftT WHERE b > d)",
      Seq(row(3, 2.0))
    )
  }

  @Test
  def testInWithNonEqualityCorrelationCondition2(): Unit = {
    verifyPlanAndCheckResult(
      "SELECT * FROM leftT WHERE a IN " +
          "(SELECT c FROM (SELECT MAX(c) AS c, d FROM rightT GROUP BY d) r WHERE leftT.b > r.d)",
      Seq(row(3, 3.0))
    )
  }

  @Test
  def testInWithNonEqualityCorrelationCondition3(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE a IN " +
            "(SELECT c FROM (SELECT MIN(c) OVER() AS c, d FROM rightT) r WHERE leftT.b <> r.d)",
        Seq(row(2, 1.0), row(2, 1.0))
      )
    }
  }

  @Test
  def testInWithNonEqualityCorrelationCondition4(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE a IN (SELECT c FROM " +
            "(SELECT MIN(c) OVER() AS c, d FROM rightT GROUP BY c, d) r WHERE leftT.b <> r.d)",
        Seq(row(2, 1.0), row(2, 1.0))
      )
    }
  }

  @Test
  def testExistsWithNonEqualityCorrelationCondition(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      verifyPlanAndCheckResult(
        "SELECT * FROM leftT WHERE EXISTS (SELECT c FROM rightT WHERE b > d)",
        Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(null, 5.0))
      )
    }
  }
}

object SemiJoinITCase {
  @Parameterized.Parameters(name = "{0}-{1}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array(BroadcastHashJoin, false),
      Array(HashJoin, false),
      Array(HashJoin, true),
      Array(SortMergeJoin, false),
      Array(NestedLoopJoin, false))
  }

  lazy val leftT = Seq(
    row(1, 2.0),
    row(1, 2.0),
    row(2, 1.0),
    row(2, 1.0),
    row(3, 3.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)
  )

  val nullablesOfLeftT = Seq(true, true)

  lazy val rightT = Seq(
    row(2, 3.0),
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)
  )

  val nullablesOfRightT = Seq(true, true)

  lazy val rightUniqueKeyT = Seq(
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, 5.0),
    row(6, null)
  )
}
