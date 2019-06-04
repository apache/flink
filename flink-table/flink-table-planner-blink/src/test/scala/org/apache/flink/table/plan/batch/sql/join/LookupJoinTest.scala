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
package org.apache.flink.table.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.plan.stream.sql.join.TestTemporalTable
import org.apache.flink.table.util.TableTestBase

import org.junit.Assert.{assertTrue, fail}
import org.junit.{Before, Test}

class LookupJoinTest extends TableTestBase {
  private val testUtil = batchTestUtil()

  @Before
  def before(): Unit = {
    testUtil.addDataStream[(Int, String, Long)]("T0", 'a, 'b, 'c)
    testUtil.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    testUtil.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)
    testUtil.tableEnv.registerTableSource("temporalTest", new TestTemporalTable)
    val myTable = testUtil.tableEnv.sqlQuery("SELECT *, PROCTIME() as proctime FROM T0")
    testUtil.tableEnv.registerTable("MyTable", myTable)
  }

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest T.proc AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't query a dim table directly
    expectExceptionThrown(
      "SELECT * FROM temporalTest FOR SYSTEM_TIME AS OF TIMESTAMP '2017-08-09 14:36:11'",
      "Cannot generate a valid execution plan for the given query",
      classOf[TableException]
    )

    // can't on non-key fields
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.age",
      "Temporal table join requires an equality condition on ALL fields of table " +
        "[TestTemporalTable(id, name, age)]'s PRIMARY KEY or (UNIQUE) INDEX(s).",
      classOf[TableException]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Unsupported join type for semi-join RIGHT",
      classOf[IllegalArgumentException]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 1",
      "Temporal table join requires an equality condition on ALL fields of table " +
        "[TestTemporalTable(id, name, age)]'s PRIMARY KEY or (UNIQUE) INDEX(s).",
      classOf[TableException]
    )
  }


  @Test
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
         |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin
    val programs = FlinkBatchProgram.buildProgram(testUtil.tableEnv.getConfig.getConf)
    programs.remove(FlinkBatchProgram.PHYSICAL)
    val calciteConfig = CalciteConfig.createBuilder(testUtil.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(programs).build()
    testUtil.tableEnv.getConfig.setCalciteConfig(calciteConfig)
    testUtil.verifyPlan(sql)
  }

  @Test
  def testLogicalPlanWithImplicitTypeCast(): Unit = {
    val programs = FlinkBatchProgram.buildProgram(testUtil.tableEnv.getConfig.getConf)
    programs.remove(FlinkBatchProgram.PHYSICAL)
    val calciteConfig = CalciteConfig.createBuilder(testUtil.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(programs).build()
    testUtil.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    thrown.expect(classOf[TableException])
    thrown.expectMessage("VARCHAR(2147483647) and INTEGER does not have common type now")

    testUtil.verifyPlan("SELECT * FROM MyTable AS T JOIN temporalTest "
      + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id")
  }

  @Test
  def testJoinInvalidNonTemporalTable(): Unit = {
    // can't follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN nonTemporal " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Temporal table join only support join on a LookupableTableSource",
      classOf[TableException])
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proctime FROM MyTable WHERE c > 1000) AS T " +
      "JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
      """.stripMargin
    testUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin
    testUtil.verifyPlan(sql)
  }

  @Test
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
         |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin
    testUtil.verifyPlan(sql)
  }

  @Test
  def testReusing(): Unit = {
    testUtil.tableEnv.getConfig.getConf.setBoolean(
      PlannerConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proctime
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT * FROM ($sql1) AS T
         |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
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

    testUtil.verifyPlan(sql)
  }

  // ==========================================================================================

  // ==========================================================================================

  private def expectExceptionThrown(
    sql: String,
    keywords: String,
    clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    try {
      testUtil.verifyExplain(sql)
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The actual exception message \n${e.getMessage}\n" +
              s"doesn't contain expected keyword \n$keywords\n",
            e.getMessage.contains(keywords))
        }
      case e: Throwable =>
        e.printStackTrace()
        fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }
}
