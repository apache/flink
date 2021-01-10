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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicUdf
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.{NonDeterministicTableFunc, StringSplit}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class SubplanReuseTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
  }

  @Test
  def testDisableSubplanReuse(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
    val sqlQuery =
      """
        |WITH r AS (
        | SELECT a, SUM(b) as b, SUM(e) as e FROM x, y WHERE a = d AND c > 100 GROUP BY a
        |)
        |SELECT r1.a, r1.b, r2.e FROM r r1, r r2 WHERE r1.b > 10 AND r2.e < 20 AND r1.a = r2.a
      """.stripMargin
    util.verifyRelPlanNotExpected(sqlQuery, "Reused")
  }

  @Test
  def testSubplanReuseWithDifferentRowType(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    // can not reuse because of different row-type
    val sqlQuery =
      """
        |WITH t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as BIGINT)),
        |     t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
        |SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b
      """.stripMargin
    util.verifyRelPlanNotExpected(sqlQuery, "Reused")
  }

  @Test
  def testEnableReuseTableSource(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true)
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDisableReuseTableSource(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    val sqlQuery =
      """
        |WITH t AS (SELECT * FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnCalc(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'test%')
        |(SELECT r.a, LOWER(c) AS c, y.e FROM r, y WHERE r.a = y.d)
        |UNION ALL
        |(SELECT r.a, LOWER(c) AS c, y.e FROM r, y WHERE r.a = y.d)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnCalcWithNonDeterministicProject(): Unit = {
    util.tableEnv.registerFunction("random_udf", new NonDeterministicUdf())

    val sqlQuery =
      """
        |(SELECT a, random_udf() FROM x WHERE a > 10)
        |UNION ALL
        |(SELECT a, random_udf() FROM x WHERE a > 10)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnCalcWithNonDeterministicUdf(): Unit = {
    util.tableEnv.registerFunction("random_udf", new NonDeterministicUdf())

    val sqlQuery =
      """
        |(SELECT a FROM x WHERE b > random_udf(a))
        |UNION ALL
        |(SELECT a FROM x WHERE b > random_udf(a))
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnExchange(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c FROM x WHERE c LIKE 'test%')
        |SELECT * FROM r, y WHERE a = d AND e > 10
        |UNION ALL
        |SELECT * FROM r, y WHERE a = d AND f <> ''
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnGroupAggregate(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnAggregateWithNonDeterministicAggCall(): Unit = {
    // FirstValueAggFunction and LastValueAggFunction are not deterministic
    util.addTemporarySystemFunction(
      "MyFirst",
      new FirstValueAggFunction(DataTypes.INT().getLogicalType))
    util.addTemporarySystemFunction(
      "MyLast",
      new FirstValueAggFunction(DataTypes.BIGINT().getLogicalType))

    val sqlQuery =
      """
        |WITH r AS (SELECT c, MyFirst(a) a, MyLast(b) b FROM x GROUP BY c)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnSort(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c ORDER BY a, b DESC)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.a > 1 AND r2.b < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnLimit(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |
        |SELECT a, b FROM r WHERE a > 10
        |UNION ALL
        |SELECT a, b * 2 AS b FROM r WHERE b < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnSortLimit(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c ORDER BY a, b DESC LIMIT 10)
        |
        |SELECT a, b FROM r WHERE a > 10
        |UNION ALL
        |SELECT a, b * 2 AS b FROM r WHERE b < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnJoin(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x FULL OUTER JOIN y ON ABS(a) = ABS(d) OR c = f
        |           WHERE b > 1 and e < 2)
        |
        |SELECT a, b FROM r
        |UNION ALL
        |SELECT a, b * 2 AS b FROM r
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnJoinNonDeterministicJoinCondition(): Unit = {
    util.tableEnv.registerFunction("random_udf", new NonDeterministicUdf)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x FULL OUTER JOIN y ON random_udf(a) = random_udf(d) OR c = f
        |           WHERE b > 1 and e < 2)
        |
        |SELECT a, b FROM r
        |UNION ALL
        |SELECT a, b * 2 AS b FROM r
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnOverWindow(): Unit = {
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, RANK() OVER (ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnOverWindowWithNonDeterministicAggCall(): Unit = {
    // FirstValueAggFunction is not deterministic
    util.addTemporarySystemFunction(
      "MyFirst",
      new FirstValueAggFunction(DataTypes.STRING().getLogicalType))
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, MyFirst(c) OVER (PARTITION BY c ORDER BY c DESC) FROM x)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b < 100 AND r2.b > 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnCorrelate(): Unit = {
    util.addFunction("str_split", new StringSplit())
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c, v FROM x, LATERAL TABLE(str_split(c, '-')) AS T(v))
        |SELECT * FROM r r1, r r2 WHERE r1.v = r2.v
      """.stripMargin
    // TODO the sub-plan of Correlate should be reused,
    // however the digests of Correlates are different
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseOnCorrelateWithNonDeterministicUDTF(): Unit = {
    util.addFunction("TableFun", new NonDeterministicTableFunc)

    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, c, s FROM x, LATERAL TABLE(TableFun(c)) AS T(s))
        |SELECT * FROM r r1, r r2 WHERE r1.c = r2.s
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuseWithDynamicFunction(): Unit = {
    val sqlQuery = util.tableEnv.sqlQuery(
      """
        |(SELECT a AS random FROM x ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT a AS random FROM x ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT a AS random FROM x ORDER BY rand() LIMIT 1)
      """.stripMargin)
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEnableReuseTableSourceOnNewSource(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true)
    testReuseOnNewSource()
  }

  @Test
  def testDisableReuseTableSourceOnNewSource(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    testReuseOnNewSource()
  }

  private def testReuseOnNewSource(): Unit = {
    util.addTable(
      s"""
         |create table newX(
         |  a int,
         |  b bigint,
         |  c varchar
         |) with (
         |  'connector' = 'values'
         |)
       """.stripMargin)
    util.addTable(
      s"""
         |create table newY(
         |  d int,
         |  e bigint,
         |  f varchar
         |) with (
         |  'connector' = 'values'
         |)
       """.stripMargin)
    val sqlQuery =
      """
        |WITH t AS (
        |  SELECT newX.a AS a, newX.b AS b, newY.d AS d, newY.e AS e
        |  FROM newX, newY
        |  WHERE newX.a = newY.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }
}
