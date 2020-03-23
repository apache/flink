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

package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.planner.runtime.batch.sql.join.SemiJoinITCase.leftT
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Test}

import java.util

import scala.collection.Seq

@RunWith(classOf[Parameterized])
class SemiJoinITCase(expectedJoinType: JoinType) extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("leftT", leftT, INT_DOUBLE, "a, b")
    registerCollection("rightT", SemiJoinITCase.rightT, INT_DOUBLE, "c, d")
    registerCollection("rightUniqueKeyT", SemiJoinITCase.rightUniqueKeyT, INT_DOUBLE, "c, d")
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testSingleConditionLeftSemi(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)",
      Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null)))
  }

  @Test
  def testComposedConditionLeftSemi(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b < d)",
      Seq(row(2, 1.0), row(2, 1.0)))
  }

  @Test
  def testSingleConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)",
      Seq(row(1, 2.0), row(1, 2.0), row(null, null), row(null, 5.0)))
  }

  @Test
  def testSingleUniqueConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS " +
          "(SELECT * FROM (SELECT DISTINCT c FROM rightT) WHERE a = c)",
      Seq(row(1, 2.0), row(1, 2.0), row(null, null), row(null, 5.0)))
  }

  @Test
  def testComposedConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(6, null), row(null, 5.0), row(null, null)))
  }

  @Test
  def testComposedUniqueConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightUniqueKeyT WHERE a = c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(null, null), row(null, 5.0), row(6, null)))
  }

  @Test
  def testSemiJoinTranspose(): Unit = {
    checkResult("SELECT a, b FROM " +
        "(SELECT a, b, c FROM leftT, rightT WHERE a = c) lr " +
        "WHERE lr.a > 0 AND lr.c IN (SELECT c FROM rightUniqueKeyT WHERE d > 1)",
      Seq(row(2, 1.0), row(2, 1.0), row(2, 1.0), row(2, 1.0), row(3, 3.0))
    )
  }

  @Test
  def testFilterPushDownLeftSemi1(): Unit = {
    checkResult(
      "SELECT * FROM (SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)) T WHERE T.b > 2",
      Seq(row(3, 3.0)))
  }

  @Test
  def testFilterPushDownLeftSemi2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM (SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT)) T WHERE T.b > 2",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testFilterPushDownLeftSemi3(): Unit = {
    checkResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
          "WHERE T.b > 2",
      Seq(row(3, 3.0)))
  }

  @Test
  def testJoinConditionPushDownLeftSemi1(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b > 2)",
      Seq(row(3, 3.0)))
  }

  @Test
  def testJoinConditionPushDownLeftSemi2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE b > 2)",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftSemi3(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)",
      Seq(row(3, 3.0)))
  }

  @Test
  def testFilterPushDownLeftAnti1(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM " +
            "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE c < 3)) T " +
            "WHERE T.b > 2",
        Seq(row(3, 3.0)))
    }
  }

  @Test
  def testFilterPushDownLeftAnti2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM " +
            "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT where c > 10)) T " +
            "WHERE T.b > 2",
        Seq(row(3, 3.0), row(null, 5.0)))
    }
  }

  @Test
  def testFilterPushDownLeftAnti3(): Unit = {
    checkResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND c < 3)) T " +
          "WHERE T.b > 2",
      Seq(row(3, 3.0), row(null, 5.0)))
  }

  @Test
  def testFilterPushDownLeftAnti4(): Unit = {
    checkResult(
      "SELECT * FROM " +
          "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
          "WHERE T.b > 2",
      Seq(row(null, 5.0)))
  }

  @Test
  def testJoinConditionPushDownLeftAnti1(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b > 2)",
        Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(null, null), row(6, null)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftAnti2(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE b > 2)",
        Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(null, null), row(6, null)))
    }
  }

  @Test
  def testJoinConditionPushDownLeftAnti3(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND b > 1)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0),
        row(3, 3.0), row(null, null), row(6, null)))
  }

  @Test
  def testJoinConditionPushDownLeftAnti4(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0),
        row(null, null), row(null, 5.0), row(6, null)))
  }

  @Test
  def testInWithAggregate1(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) FROM leftT WHERE b = d)",
      Seq(row(4, 1.0))
    )
  }

  @Ignore // TODO not support same source until set lazy_from_source
  @Test
  def testInWithAggregate2(): Unit = {
    checkResult(
      "SELECT * FROM leftT t1 WHERE a IN (SELECT DISTINCT a FROM leftT t2 WHERE t1.b = t2.b)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0), row(3, 3.0))
    )
  }

  @Test
  def testInWithAggregate3(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE CAST(c/2 AS BIGINT) IN (SELECT COUNT(*) FROM leftT WHERE b = d)",
      Seq(row(2, 3.0), row(2, 3.0), row(4, 1.0))
    )
  }

  @Test
  def testInWithOver1(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER " +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(6, null))
    )
  }

  @Test
  def testInWithOver2(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER" +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT GROUP BY a, b)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(6, null))
    )
  }

  @Test
  def testInWithOver3(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER " +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT WHERE b = d)",
      Seq(row(4, 1.0))
    )
  }

  @Test
  def testInWithOver4(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT SUM(a) OVER" +
          "(PARTITION BY b ORDER BY a ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW) " +
          "FROM leftT WHERE b = d GROUP BY a, b)",
      Seq()
    )
  }

  @Test
  def testExistsWithOver1(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b = d)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(null, 5.0))
    )
  }

  @Test
  def testExistsWithOver2(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      checkResult(
        "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b > d)",
        Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
      )
    }
  }

  @Test
  def testExistsWithOver3(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b = d GROUP BY a)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0), row(null, 5.0))
    )
  }

  @Test
  def testExistsWithOver4(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      checkResult(
        "SELECT * FROM rightT WHERE EXISTS (SELECT SUM(a) OVER() FROM leftT WHERE b>d GROUP BY a)",
        Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
      )
    }
  }

  @Test
  def testInWithNonEqualityCorrelationCondition1(): Unit = {
    checkResult(
      "SELECT * FROM rightT WHERE c IN (SELECT a FROM leftT WHERE b > d)",
      Seq(row(3, 2.0))
    )
  }

  @Test
  def testInWithNonEqualityCorrelationCondition2(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE a IN " +
          "(SELECT c FROM (SELECT MAX(c) AS c, d FROM rightT GROUP BY d) r WHERE leftT.b > r.d)",
      Seq(row(3, 3.0))
    )
  }

  @Test
  def testInWithNonEqualityCorrelationCondition3(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE a IN " +
            "(SELECT c FROM (SELECT MIN(c) OVER() AS c, d FROM rightT) r WHERE leftT.b <> r.d)",
        Seq(row(2, 1.0), row(2, 1.0))
      )
    }
  }

  @Test
  def testInWithNonEqualityCorrelationCondition4(): Unit = {
    if (expectedJoinType eq NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE a IN (SELECT c FROM " +
            "(SELECT MIN(c) OVER() AS c, d FROM rightT GROUP BY c, d) r WHERE leftT.b <> r.d)",
        Seq(row(2, 1.0), row(2, 1.0))
      )
    }
  }

  @Test
  def testExistsWithNonEqualityCorrelationCondition(): Unit = {
    if (expectedJoinType eq JoinType.NestedLoopJoin) {
      checkResult(
        "SELECT * FROM leftT WHERE EXISTS (SELECT c FROM rightT WHERE b > d)",
        Seq(row(1, 2.0), row(1, 2.0), row(3, 3.0), row(null, 5.0))
      )
    }
  }

  @Test
  def testRewriteScalarQueryWithoutCorrelation1(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT)"
    ).foreach(checkResult(_, leftT))
  }

  @Test
  def testRewriteScalarQueryWithoutCorrelation2(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 5) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 5) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 5) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 5) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT WHERE c > 5)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT WHERE c > 5)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT WHERE c > 5)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT WHERE c > 5)"
    ).foreach(checkResult(_, leftT))
  }

  @Test
  def testRewriteScalarQueryWithoutCorrelation3(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 15) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 15) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 15) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE c > 15) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT WHERE c > 15)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT WHERE c > 15)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT WHERE c > 15)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT WHERE c > 15)"
    ).foreach(checkResult(_, Seq.empty))
  }

  @Test
  def testRewriteScalarQueryWithCorrelation1(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT WHERE a = c)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT WHERE a = c)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT WHERE a = c)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT WHERE a = c)"
    ).foreach(checkResult(_, Seq(row(2, 1.0), row(2, 1.0), row(3, 3.0), row(6, null))))
  }

  @Test
  def testRewriteScalarQueryWithCorrelation2(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 5)"
    ).foreach(checkResult(_, Seq(row(6, null))))
  }

  @Test
  def testRewriteScalarQueryWithCorrelation3(): Unit = {
    Seq(
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15) > 0",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15) > 0.9",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15) >= 1",
      "SELECT * FROM leftT WHERE (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15) >= 0.1",
      "SELECT * FROM leftT WHERE 0 < (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15)",
      "SELECT * FROM leftT WHERE 0.99 < (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15)",
      "SELECT * FROM leftT WHERE 1 <= (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15)",
      "SELECT * FROM leftT WHERE 0.01 <= (SELECT COUNT(*) FROM rightT WHERE a = c AND c > 15)"
    ).foreach(checkResult(_, Seq.empty))
  }
}

object SemiJoinITCase {
  @Parameterized.Parameters(name = "{0}-{1}")
  def parameters(): util.Collection[Any] = {
    util.Arrays.asList(BroadcastHashJoin, HashJoin, SortMergeJoin, NestedLoopJoin)
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

  lazy val rightT = Seq(
    row(2, 3.0),
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)
  )

  lazy val rightUniqueKeyT = Seq(
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, 5.0),
    row(6, null)
  )
}
