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
package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.plan.stream.sql.join.TestTemporalTable
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.util.{Collection => JCollection}
import _root_.scala.collection.JavaConversions._
import org.assertj.core.api.Assertions.{assertThatExceptionOfType, assertThatThrownBy}
import org.assertj.core.api.Assumptions.assumeThat
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

/**
 * The physical plans for legacy [[org.apache.flink.table.sources.LookupableTableSource]] and new
 * [[org.apache.flink.table.connector.source.LookupTableSource]] should be identical.
 */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class LookupJoinTest(legacyTableSource: Boolean) extends TableTestBase {
  private val testUtil = batchTestUtil()

  @BeforeEach
  def before(): Unit = {
    testUtil.addDataStream[(Int, String, Long)]("T0", 'a, 'b, 'c)
    testUtil.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    testUtil.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)
    val myTable = testUtil.tableEnv.sqlQuery("SELECT *, PROCTIME() as proctime FROM T0")
    testUtil.tableEnv.createTemporaryView("MyTable", myTable)
    if (legacyTableSource) {
      TestTemporalTable.createTemporaryTable(testUtil.tableEnv, "LookupTable", isBounded = true)
    } else {
      testUtil.addTable("""
                          |CREATE TABLE LookupTable (
                          |  `id` INT,
                          |  `name` STRING,
                          |  `age` INT
                          |) WITH (
                          |  'connector' = 'values',
                          |  'bounded' = 'true'
                          |)
                          |""".stripMargin)

      testUtil.addTable("""
                          |CREATE TABLE LookupTableWithComputedColumn (
                          |  `id` INT,
                          |  `name` STRING,
                          |  `age` INT,
                          |  `nominal_age` as age + 1
                          |) WITH (
                          |  'connector' = 'values',
                          |  'bounded' = 'true'
                          |)
                          |""".stripMargin)
    }
  }

  @TestTemplate
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LookupTable T.proc AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // only support left or inner join
    // Calcite does not allow FOR SYSTEM_TIME AS OF non-nullable left table field to Right Join.
    // There is an exception:
    // java.lang.AssertionError
    //    at SqlToRelConverter.getCorrelationUse(SqlToRelConverter.java:2517)
    //    at SqlToRelConverter.createJoin(SqlToRelConverter.java:2426)
    //    at SqlToRelConverter.convertFrom(SqlToRelConverter.java:2071)
    //    at SqlToRelConverter.convertSelectImpl(SqlToRelConverter.java:646)
    //    at SqlToRelConverter.convertSelect(SqlToRelConverter.java:627)
    //    at SqlToRelConverter.convertQueryRecursive(SqlToRelConverter.java:3100)
    //    at SqlToRelConverter.convertQuery(SqlToRelConverter.java:563)
    //    at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.rel(FlinkPlannerImpl.scala:125)
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      null,
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 1",
      "Temporal table join requires an equality condition on fields of table " +
        "[default_catalog.default_database.LookupTable].",
      classOf[TableException]
    )
  }

  @TestTemplate
  def testNotDistinctFromInJoinCondition(): Unit = {

    // does not support join condition contains `IS NOT DISTINCT`
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a IS NOT  DISTINCT FROM D.id",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )

    // does not support join condition contains `IS NOT  DISTINCT` and similar syntax
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id OR (T.a IS NULL AND D.id IS NULL)",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )
  }

  @TestTemplate
  def testPythonUDFInJoinCondition(): Unit = {
    testUtil.addTemporarySystemFunction("pyFunc", new PythonScalarFunction("pyFunc"))
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |LEFT OUTER JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10 AND pyFunc(D.age, T.a) > 100
      """.stripMargin

    assertThatThrownBy(() => testUtil.verifyExecPlan(sql))
      .hasMessageContaining("Only inner join condition with equality predicates supports the " +
        "Python UDF taking the inputs from the left table and the right table at the same time, " +
        "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testLogicalPlan(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proctime
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT T.* FROM ($sql1) AS T
         |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin
    val programs = FlinkBatchProgram.buildProgram(testUtil.tableEnv.getConfig)
    programs.remove(FlinkBatchProgram.PHYSICAL)
    testUtil.replaceBatchProgram(programs)
    testUtil.verifyRelPlan(sql)
  }

  @TestTemplate
  def testLogicalPlanWithImplicitTypeCast(): Unit = {
    val programs = FlinkBatchProgram.buildProgram(testUtil.tableEnv.getConfig)
    programs.remove(FlinkBatchProgram.PHYSICAL)
    testUtil.replaceBatchProgram(programs)

    assertThatThrownBy(
      () =>
        testUtil.verifyRelPlan(
          "SELECT * FROM MyTable AS T JOIN LookupTable "
            + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id"))
      .hasMessageContaining("implicit type conversion between VARCHAR(2147483647) and INTEGER " +
        "is not supported on join's condition now")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proctime FROM MyTable WHERE c > 1000) AS T " +
      "JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
      """.stripMargin
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testAvoidAggregatePushDown(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proctime
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT T.* FROM ($sql1) AS T
         |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testJoinTemporalTableWithTrueCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON true
        |WHERE T.c > 1000
      """.stripMargin

    assertThatThrownBy(() => testUtil.verifyExplain(sql))
      .hasMessageContaining("Temporal table join requires an equality condition on fields of " +
        "table [default_catalog.default_database.LookupTable]")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    // Computed column do not support in legacyTableSource.
    assumeThat(legacyTableSource).isFalse
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id
      """.stripMargin
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    // Computed column do not support in legacyTableSource.
    assumeThat(legacyTableSource).isFalse
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id and D.nominal_age > 12
      """.stripMargin
    testUtil.verifyExecPlan(sql)
  }

  @TestTemplate
  def testReusing(): Unit = {
    testUtil.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proctime
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT * FROM ($sql1) AS T
         |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin
    val sql3 =
      s"""
         |SELECT id as a, b FROM ($sql2) AS T
       """.stripMargin
    val sql =
      s"""
         |SELECT count(T1.a), count(T1.id), sum(T2.a)
         |FROM ($sql2) AS T1, ($sql3) AS T2
         |WHERE T1.a = T2.a
         |GROUP BY T1.b, T2.b
      """.stripMargin

    testUtil.verifyExecPlan(sql)
  }

  // ==========================================================================================

  // ==========================================================================================

  private def expectExceptionThrown(
      sql: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    val callable: ThrowingCallable = () => testUtil.verifyExplain(sql)
    if (keywords != null) {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
        .withMessageContaining(keywords)
    } else {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
    }
  }
}

object LookupJoinTest {
  @Parameters(name = "LegacyTableSource={0}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](Array(JBoolean.TRUE), Array(JBoolean.FALSE))
  }
}
