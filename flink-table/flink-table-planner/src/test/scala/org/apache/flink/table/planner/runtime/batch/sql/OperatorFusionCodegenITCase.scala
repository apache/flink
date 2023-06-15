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

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/** IT cases for operator fusion codegen. */
class OperatorFusionCodegenITCase extends BatchTestBase {

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
  }

  @Test
  def testBasicInnerHashJoin(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT * FROM
                                 |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d
                                 |""".stripMargin)
  }

  @Test
  def testBasicInnerHashJoinWithCondition(): Unit = {
    checkOpFusionCodegenResult("""
                                 |SELECT * FROM
                                 |  (SELECT a, b FROM x INNER JOIN y ON x.a = y.d) T1
                                 |  INNER JOIN
                                 |  (SELECT d, e FROM y INNER JOIN t ON y.d = t.a) T2
                                 |  ON T1.a = T2.d WHERE T1.b > T2.e
                                 |""".stripMargin)
  }

  @Test
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

  @Test
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

  @Test
  def testProbeOuterHashJoinWithOnCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT a, ny, nz FROM x
        |  LEFT JOIN y ON x.a = y.ny
        |  LEFT JOIN z ON x.a = z.nz AND CHAR_LENGTH(x.c) > CHAR_LENGTH(z.i)
        |""".stripMargin)
  }

  @Test
  def testProbeOuterHashJoinWithWhereCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT a, ny, nz FROM x
        |  LEFT JOIN y ON x.a = y.ny
        |  LEFT JOIN z ON x.a = z.nz WHERE CHAR_LENGTH(x.c) > CHAR_LENGTH(y.f)
        |""".stripMargin)
  }

  @Test
  def testHashJoinWithNoPriorityConstraint(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM x
        |  INNER JOIN y ON x.a = y.d
        |  INNER JOIN t ON x.a = t.a
        |""".stripMargin
    )
  }

  @Test
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

  @Test
  def testLeftSemiHashJoin(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM t WHERE t.a IN
        |(SELECT a FROM x INNER JOIN y ON x.a = y.d)
        |""".stripMargin
    )
  }

  @Test
  def testLeftSemiHashJoinWithCondition(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM t WHERE t.a IN
        |(SELECT * FROM (SELECT a FROM x INNER JOIN y ON x.a = y.d) t2 WHERE t.b > t2.a )
        |""".stripMargin
    )
  }

  @Test
  def testLeftAntiHashJoin(): Unit = {
    checkOpFusionCodegenResult(
      """
        |SELECT * FROM x, y WHERE x.a = y.d
        |AND NOT EXISTS (SELECT * FROM z t1 WHERE x.b = t1.h AND t1.g >= 0)
        |AND NOT EXISTS (SELECT * FROM z t2 WHERE x.b = t2.h AND t2.h < 100)
        |""".stripMargin
    )
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
