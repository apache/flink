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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinITCaseHelper
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

/** IT cases for operator fusion codegen. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class OperatorFusionCodegenITCase extends BatchTestBase {

  @Parameter var expectedJoinType: JoinType = _
  @BeforeEach
  override def before(): Unit = {
    super.before()

    registerCollection(
      "x",
      MultipleInputITCase.dataX,
      MultipleInputITCase.rowType,
      "a, b, c, nx",
      MultipleInputITCase.nullables)
    registerCollection(
      "y",
      MultipleInputITCase.dataY,
      MultipleInputITCase.rowType,
      "d, e, f, ny",
      MultipleInputITCase.nullables)
    registerCollection(
      "z",
      MultipleInputITCase.dataZ,
      MultipleInputITCase.rowType,
      "g, h, i, nz",
      MultipleInputITCase.nullables)
    registerCollection(
      "t",
      MultipleInputITCase.dataT,
      MultipleInputITCase.rowType,
      "a, b, c, nt",
      MultipleInputITCase.nullables)
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @TestTemplate
  def testBasicInnerHashJoin(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT * FROM
                                 |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |""".stripMargin)
  }

  @TestTemplate
  def testBasicInnerHashJoinWithCondition(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT * FROM
                                 |  (SELECT a, b FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d, e FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d WHERE T1.b > T2.e
                                 |""".stripMargin)
  }

  @TestTemplate
  def testBasicInnerHashJoinWithCondition2(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM
        |  (SELECT a, b FROM x INNER JOIN y ON x.a = y.d WHERE CHAR_LENGTH(x.c) > CHAR_LENGTH(y.f)) T1
        |  INNER JOIN
        |  (SELECT d, e FROM y INNER JOIN t ON y.d = t.a) T2
        |  ON T1.a = T2.d WHERE T1.b > T2.e
        |""".stripMargin)
  }

  @TestTemplate
  def testManyProbeOuterHashJoin(): Unit = {
    checkOpFusionCodegenResult(
      """
        |WITH
        |  T1 AS (
        |    SELECT a, ny, nz FROM x
        |      LEFT JOIN y ON x.a = y.ny
        |      LEFT JOIN z ON x.a = z.nz),
        |  T2 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN t ON T1.a = t.a
        |      INNER JOIN y ON T1.a = y.d),
        |  T3 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN y ON T1.a = y.d
        |      INNER JOIN t ON T1.a = t.a),
        |  T4 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T2 GROUP BY b),
        |  T5 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T3 GROUP BY b)
        |SELECT * FROM
        |  (SELECT t.b, sd, sy, sz FROM T4 LEFT JOIN t ON T4.b = t.b)
        |  UNION ALL
        |  (SELECT y.e, sd, sy, sz FROM T5 LEFT JOIN y ON T5.b = y.e)
        |""".stripMargin)
  }

  @TestTemplate
  def testProbeOuterHashJoinWithOnCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT a, ny, nz FROM x
        |  LEFT JOIN y ON x.a = y.ny
        |  LEFT JOIN z ON x.a = z.nz AND CHAR_LENGTH(x.c) > CHAR_LENGTH(z.i)
        |""".stripMargin)
  }

  @TestTemplate
  def testProbeOuterHashJoinWithWhereCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT a, ny, nz FROM x
        |  LEFT JOIN y ON x.a = y.ny
        |  LEFT JOIN z ON x.a = z.nz WHERE CHAR_LENGTH(x.c) > CHAR_LENGTH(y.f)
        |""".stripMargin)
  }

  @TestTemplate
  def testHashJoinWithNoPriorityConstraint(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM x
        |  INNER JOIN y ON x.a = y.d
        |  INNER JOIN t ON x.a = t.a
        |""".stripMargin
    )
  }

  @TestTemplate
  def testHashJoinWithOnlyProjection(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT * FROM (SELECT a, nx + ny AS nt FROM x
                                 |  JOIN y ON x.a = y.ny) t
                                 |JOIN z ON t.a = z.nz WHERE t.nt -10 > z.nz
                                 |""".stripMargin)
  }

  @TestTemplate
  def testHashJoinWithDeadlockCausedByExchangeInAncestor(): Unit = {
    checkOpFusionCodegenResult(
      """
        |WITH T1 AS (
        |  SELECT x1.*, x2.a AS k, (x1.b + x2.b) AS v
        |  FROM x x1 LEFT JOIN x x2 ON x1.a = x2.a WHERE x2.a > 0)
        |SELECT x.a, x.b, T1.* FROM x LEFT JOIN T1 ON x.a = T1.k WHERE x.a > 0 AND T1.v = 0
        |""".stripMargin
    )
  }

  @TestTemplate
  def testLeftSemiHashJoin(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM t WHERE t.a IN
        |(SELECT a FROM x INNER JOIN y ON x.a = y.d)
        |""".stripMargin
    )
  }

  @TestTemplate
  def testLeftSemiHashJoinWithCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM t WHERE t.a IN
        |(SELECT * FROM (SELECT a FROM x INNER JOIN y ON x.a = y.d) t2 WHERE t.b > t2.a )
        |""".stripMargin
    )
  }

  @TestTemplate
  def testLeftAntiHashJoin(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM x, y WHERE x.a = y.d
        |AND NOT EXISTS (SELECT * FROM z t1 WHERE x.b = t1.h AND t1.g >= 0)
        |AND NOT EXISTS (SELECT * FROM z t2 WHERE x.b = t2.h AND t2.h < 100)
        |""".stripMargin
    )
  }

  @TestTemplate
  def testLocalHashAggWithoutKey(): Unit = {
    // Due to the global hash agg is singleton when without key, so currently we can't cover the test case of
    // hash agg without key by MultipleInput, it can be covered by OperatorChain fusion codegen in future
    checkOpFusionCodegenResult("""
                                 |SELECT count(a) as cnt FROM
                                 |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |""".stripMargin)
  }

  @TestTemplate
  def testLocalHashAggWithoutKeyAndHashAggWithKey(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT count(distinct d) as cnt FROM
                                 |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |""".stripMargin)
  }

  @TestTemplate
  def testHashAggWithKey(): Unit = {
    checkOpFusionCodegenResult(
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  RIGHT JOIN
        |  (SELECT d FROM y) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    )
  }

  @TestTemplate
  def testHashAggWithKey2(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT a, count(b) as cnt FROM
                                 |  (SELECT a, b FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |  GROUP BY a
                                 |""".stripMargin)
  }

  @TestTemplate
  def testLocalHashAggWithKey(): Unit = {
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    checkOpFusionCodegenResult("""
                                 |SELECT a, count(b) as cnt, avg(b) as pj FROM
                                 |  (SELECT a, b FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |  GROUP BY a
                                 |""".stripMargin)
  }

  @TestTemplate
  def testGlobalHashAggWithKey(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
        |  INNER JOIN
        |  (SELECT a, SUM(b) AS cnt, AVG(b) AS pj FROM t GROUP BY a) T2
        |  ON T1.a = T2.a
        |""".stripMargin
    )
  }

  @TestTemplate
  def testGlobalHashAggWithKey2(): Unit = {
    checkOpFusionCodegenResult(
      """
        |WITH agg AS (SELECT a, SUM(b) AS cnt FROM t GROUP BY a)
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
        |  INNER JOIN
        |  (SELECT d FROM y INNER JOIN agg ON y.d = agg.a) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    )
  }

  @TestTemplate
  def testLocalAndGlobalHashAggInTwoSeperatedFusionOperator(): Unit = {
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    checkOpFusionCodegenResult(
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  RIGHT JOIN
        |  (SELECT d FROM y) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    )
  }

  @TestTemplate
  def testAdaptiveLocalHashAgg(): Unit = {
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    tEnv.getConfig.set(
      HashAggCodeGenerator.TABLE_EXEC_LOCAL_HASH_AGG_ADAPTIVE_SAMPLING_THRESHOLD,
      Long.box(1L))
    checkOpFusionCodegenResult(
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  RIGHT JOIN
        |  (SELECT d FROM y) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    )
  }

  @TestTemplate
  def testHashAggWithKeyAndFilterArgs(): Unit = {
    checkOpFusionCodegenResult(
      """
        |WITH T AS (SELECT a, b, c, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(DISTINCT b) AS cnt1, COUNT(DISTINCT c) as cnt2 FROM T GROUP BY a) T1
        |  RIGHT JOIN
        |  (SELECT d FROM y) T2
        |  ON T1.a = T2.d
        |""".stripMargin)
  }

  def checkOpFusionCodegenResult(sql: String): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, Boolean.box(false))
    val expected = executeQuery(sql)
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, Boolean.box(true))
    checkResult(sql, expected)
  }
}

object OperatorFusionCodegenITCase {
  @Parameters(name = "expectedJoinType={0}")
  def parameters(): util.Collection[Any] = {
    util.Arrays.asList(Array(BroadcastHashJoin), Array(HashJoin))
  }
}
