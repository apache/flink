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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Assert.{assertTrue, fail}
import org.junit.{Before, Test}

/**
 * Test temporal join in stream mode.
 */
class TemporalJoinTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addTable(
      """
        |CREATE TABLE Orders (
        | amount INT,
        | currency STRING,
        | rowtime TIMESTAMP(3),
        | proctime AS PROCTIME(),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesHistory (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesHistoryWithPK (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime,
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesHistoryWithComputedColumn (
        | currency STRING,
        | rate INT,
        | rate1 AS rate + 1,
        | proctime AS PROCTIME(),
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime,
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesOnly (
        | currency STRING,
        | rate INT,
        | proctime AS PROCTIME()
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    util.addTable(
      " CREATE VIEW DeduplicatedView AS SELECT currency, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
        "   FROM RatesHistory" +
        "  ) T " +
        "  WHERE rowNum = 1")

    util.addTable(
      " CREATE VIEW latestView AS SELECT T.currency, T.rate, T.proctime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum " +
        "   FROM RatesOnly" +
        "  ) T " +
        "  WHERE T.rowNum = 1")

    util.addTable("CREATE VIEW latest_rates AS SELECT currency, LAST_VALUE(rate) AS rate " +
      "FROM RatesHistory " +
      "GROUP BY currency ")
  }

  @Test
  def testEventTimeTemporalJoin(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithView(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithFunctionCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND 'RMB-100' = concat('RMB-', cast(r.rate AS STRING))"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND o.amount > r.rate"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND amount > 10 AND r.rate < 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeLeftTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o LEFT JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND amount > 10 AND r.rate < 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoin(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "latestView " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithView(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > r.rate"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithComputedColumnAndPushDown(): Unit = {
    val sqlQuery = "SELECT o.currency, r.currency, rate1 " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithComputedColumn " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithComputedColumnAndPushDown(): Unit = {
    val sqlQuery = "SELECT o.currency, r.currency, rate1 " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithComputedColumn " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeLeftTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o LEFT JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithFunctionCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND 'RMB-100' = concat('RMB-', cast(r.rate AS STRING))"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInvalidTemporalTablJoin(): Unit = {
    util.addTable(
      """
        |CREATE TABLE leftTableWithoutTimeAttribute (
        | amount INT,
        | currency STRING,
        | ts TIMESTAMP(3)
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)
    val sqlQuery1 = "SELECT * FROM leftTableWithoutTimeAttribute AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.ts AS r ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery1,
      s"Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF'" +
        s" left table's time attribute field",
      classOf[ValidationException])

    util.addTable(
      """
        |CREATE TABLE versionedTableWithoutPk (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)

    val sqlQuery2 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.amount = r.rate"
    expectExceptionThrown(
      sqlQuery2,
      s"Join key must be the same as temporal table's primary key " +
        s"in Event-time temporal table join",
      classOf[ValidationException])

    util.addTable(
      """
        |CREATE TABLE versionedTableWithoutTimeAttribute (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)
    val sqlQuery3 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "versionedTableWithoutTimeAttribute FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery3,
      s"Event-Time Temporal Table Join requires both primary key and row time attribute in " +
        s"versioned table, but no row time attribute can be found.",
      classOf[ValidationException])

    util.addTable(
      """
        |CREATE TABLE versionedTableWithoutRowtime (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | proctime AS PROCTIME(),
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'false'
        |)
      """.stripMargin)
    val sqlQuery4 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "versionedTableWithoutRowtime FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery4,
      s"Event-Time Temporal Table Join requires both primary key and row time attribute in " +
        s"versioned table, but no row time attribute can be found.",
      classOf[ValidationException])
  }

  private def expectExceptionThrown(
    sql: String,
    keywords: String,
    clazz: Class[_ <: Throwable] = classOf[ValidationException])
  : Unit = {
    try {
      verifyTranslationSuccess(sql)
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

  private def verifyTranslationSuccess(sql: String): Unit = {
    util.tableEnv.sqlQuery(sql).explain()
  }
}
