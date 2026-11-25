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

import org.apache.flink.table.api.{ExplainDetail, TableException, ValidationException}
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.{BeforeEach, Test}

/** Test temporal join in stream mode. */
class TemporalJoinTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()

  @BeforeEach
  def before(): Unit = {
    util.addTable("""
                    |CREATE TABLE Orders (
                    | amount INT,
                    | currency STRING,
                    | rowtime TIMESTAMP(3),
                    | proctime AS PROCTIME(),
                    | WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesHistory (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesHistoryWithPK (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime,
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesBinlogWithComputedColumn (
                    | currency STRING,
                    | rate INT,
                    | rate1 AS rate + 1,
                    | proctime AS PROCTIME(),
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime,
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I,UB,UA,D',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesBinlogWithoutWatermark (
                    | currency STRING,
                    | rate INT,
                    | rate1 AS rate + 1,
                    | proctime AS PROCTIME(),
                    | rowtime TIMESTAMP(3),
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I,UB,UA,D',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE UpsertRates (
                    | currency STRING,
                    | rate INT,
                    | valid VARCHAR,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime,
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I,UA,D',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesOnly (
                    | currency STRING,
                    | rate INT,
                    | proctime AS PROCTIME()
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)

    util.addTable("""
                    |CREATE TABLE RatesHistoryLegacy (
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
      " CREATE VIEW rates_last_row_rowtime AS SELECT currency, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
        "   FROM RatesHistory" +
        "  ) T " +
        "  WHERE rowNum = 1")

    util.addTable(
      " CREATE VIEW rates_last_row_proctime AS SELECT T.currency, T.rate, T.proctime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum " +
        "   FROM RatesOnly" +
        "  ) T " +
        "  WHERE T.rowNum = 1")

    util.addTable(
      "CREATE VIEW rates_last_value AS SELECT currency, LAST_VALUE(rate) AS rate " +
        "FROM RatesHistory " +
        "GROUP BY currency ")

    util.tableEnv.executeSql(s"""
                                |CREATE TABLE OrdersLtz (
                                | amount INT,
                                | currency STRING,
                                | ts BIGINT,
                                | rowtime AS TO_TIMESTAMP_LTZ(ts, 3),
                                | WATERMARK FOR rowtime AS rowtime
                                |) WITH (
                                | 'connector' = 'values'
                                |)
      """.stripMargin)
    util.tableEnv.executeSql(s"""
                                |CREATE TABLE RatesLtz (
                                | currency STRING,
                                | rate INT,
                                | ts BIGINT,
                                | rowtime as TO_TIMESTAMP_LTZ(ts, 3),
                                | WATERMARK FOR rowtime AS rowtime,
                                | PRIMARY KEY(currency) NOT ENFORCED
                                |) WITH (
                                | 'connector' = 'values'
                                |)
      """.stripMargin)
  }

  @Test
  def testEventTimeTemporalJoinOnLegacySource(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryLegacy FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinOnLegacySource(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryLegacy FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "ON o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoin(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinOnTimestampLtzRowtime(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM OrdersLtz AS o JOIN " +
      "RatesLtz FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinOnTimestampLtzComputedColumn(): Unit = {
    util.addTable("""
                    |CREATE TABLE RatesWithLtzComputedColumn (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | rowtime_ltz AS CAST(rowtime AS TIMESTAMP_LTZ(3)),
                    | WATERMARK FOR rowtime_ltz AS rowtime_ltz,
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)
    val sqlQuery = "SELECT * " +
      "FROM OrdersLtz AS o JOIN " +
      "RatesWithLtzComputedColumn FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithView(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithFunctionCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND 'RMB-100' = concat('RMB-', cast(r.rate AS STRING))"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND o.amount > r.rate"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeLeftTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o LEFT JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency AND amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithLastRowView(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_proctime " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithLastValueView(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_value " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_value " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > r.rate"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithPredicates(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_value " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithComputedColumnAndPushDown(): Unit = {
    val sqlQuery = "SELECT o.currency, r.currency, rate1 " +
      "FROM Orders AS o JOIN " +
      "RatesBinlogWithComputedColumn " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testEventTimeTemporalJoinWithComputedColumnAndPushDown(): Unit = {
    val sqlQuery = "SELECT o.currency, r.currency, rate1 " +
      "FROM Orders AS o JOIN " +
      "RatesBinlogWithComputedColumn " +
      "FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithBinlogSource(): Unit = {
    val sqlQuery = "SELECT o.currency, r.currency, rate1 " +
      "FROM Orders AS o JOIN " +
      "RatesBinlogWithoutWatermark " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND o.amount > 10 AND r.rate < 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeLeftTemporalJoinWithViewWithConstantCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o LEFT JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND r.rate + 1 = 100"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProcTimeTemporalJoinWithViewWithFunctionCondition(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "on o.currency = r.currency AND 'RMB-100' = concat('RMB-', cast(r.rate AS STRING))"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInvalidTemporalTablJoin(): Unit = {
    util.addTable("""
                    |CREATE TABLE leftTableWithoutTimeAttribute (
                    | amount INT,
                    | currency STRING,
                    | ts TIMESTAMP(3)
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)
    val sqlQuery1 = "SELECT * FROM leftTableWithoutTimeAttribute AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.ts AS r ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery1,
      "FOR SYSTEM_TIME AS OF must use a time attribute of the left side of the temporal join, " +
        "but 'ts' is a regular TIMESTAMP(3) column. A time attribute is a column with a " +
        "WATERMARK or a PROCTIME() column. If the left side is append-only and the right table " +
        "has a WATERMARK, a LATERAL SNAPSHOT join can join each row with the current rows of " +
        "the right table instead.",
      classOf[ValidationException]
    )

    val sqlQueryRegularColumn = "SELECT * " +
      "FROM (SELECT *, CAST(rowtime AS TIMESTAMP(3)) AS order_time FROM Orders) AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.order_time AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQueryRegularColumn,
      "FOR SYSTEM_TIME AS OF must use a time attribute of the left side of the temporal join, " +
        "but 'order_time' is a regular TIMESTAMP(3) column. A time attribute is a column with a " +
        "WATERMARK or a PROCTIME() column. The left side has these time attributes: " +
        "'rowtime', 'proctime'.",
      classOf[ValidationException]
    )

    val sqlQuery2 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.amount = r.rate"
    expectExceptionThrown(
      sqlQuery2,
      "An event-time temporal join must join on the full primary key (currency) of its right " +
        "side, but the join condition does not use 'currency'. Add an equality condition on " +
        "'currency'. To join on columns other than the primary key, use a LATERAL SNAPSHOT " +
        "join, which joins with all current rows that match and needs an append-only left side.",
      classOf[ValidationException]
    )

    util.addTable("""
                    |CREATE TABLE RatesHistoryWithCompositePK (
                    | currency STRING,
                    | region STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime,
                    | PRIMARY KEY(currency, region) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)
    val sqlQueryPartialPk = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithCompositePK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQueryPartialPk,
      "An event-time temporal join must join on the full primary key (currency, region) of its " +
        "right side, but the join condition does not use 'region'. Add an equality condition " +
        "on 'region'.",
      classOf[ValidationException]
    )

    util.addTable("""
                    |CREATE TABLE versionedTableWithoutPk (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)

    val sqlQuery3 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "versionedTableWithoutPk FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery3,
      "The right side of an event-time temporal join must have a primary key on the join key, " +
        "but none can be derived. Declare PRIMARY KEY (currency) NOT ENFORCED on the right " +
        "table, or deduplicate it to the last row per key with ROW_NUMBER() OVER " +
        "(PARTITION BY currency ORDER BY rowtime DESC) = 1. To join each row with the current " +
        "rows of the right table instead of the version valid at the row's time, use a " +
        "LATERAL SNAPSHOT join, which needs no primary key but an append-only left side.",
      classOf[ValidationException]
    )

    util.addTable("""
                    |CREATE TABLE RatesWithoutPkAndLookup (
                    | currency STRING,
                    | rate INT,
                    | proctime AS PROCTIME()
                    |) WITH (
                    | 'connector' = 'values',
                    | 'disable-lookup' = 'true'
                    |)
      """.stripMargin)
    val sqlQueryProcTimeWithoutPk = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesWithoutPkAndLookup FOR SYSTEM_TIME AS OF o.proctime AS r " +
      "ON o.currency = r.currency"
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => verifyTranslationSuccess(sqlQueryProcTimeWithoutPk))
      .withMessageContaining(
        "Processing-time temporal joins on the right table are not supported. If the left side " +
          "is append-only and the right table has a WATERMARK, use a LATERAL SNAPSHOT join to " +
          "join each row with the current rows of the right table.")
      .withMessageNotContaining("event-time")

    util.addTable("""
                    |CREATE TABLE versionedTableWithoutTimeAttribute (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)
    val sqlQuery4 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "versionedTableWithoutTimeAttribute FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery4,
      "The right side of an event-time temporal join must have a column with a WATERMARK, " +
        "which marks when each row version becomes valid, but it has none. Declare a WATERMARK " +
        "on the right table, or keep its watermarked column unchanged in the query (a CAST " +
        "turns it into a regular column).",
      classOf[ValidationException]
    )

    util.addTable("""
                    |CREATE TABLE versionedTableWithoutRowtime (
                    | currency STRING,
                    | rate INT,
                    | rowtime TIMESTAMP(3),
                    | proctime AS PROCTIME(),
                    | PRIMARY KEY(currency) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values'
                    |)
      """.stripMargin)
    val sqlQuery5 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "versionedTableWithoutRowtime FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery5,
      "The right side of an event-time temporal join must have a column with a WATERMARK, " +
        "which marks when each row version becomes valid, but it has none.",
      classOf[ValidationException]
    )

    util.addTable(
      "CREATE VIEW RatesWithTwoRowtimes AS " +
        "SELECT currency, rate, rowtime, rowtime AS rowtime2 FROM RatesHistoryWithPK")
    val sqlQueryTwoRowtimes = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesWithTwoRowtimes FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQueryTwoRowtimes,
      "The right side of an event-time temporal join must have exactly one column with a " +
        "WATERMARK, but it has 2: 'rowtime', 'rowtime2'. CAST the others to TIMESTAMP(3).",
      classOf[ValidationException]
    )

    val sqlQuery8 =
      s"""
         |SELECT *
         | FROM OrdersLtz AS o JOIN
         | RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r
         | ON o.currency = r.currency
          """.stripMargin
    expectExceptionThrown(
      sqlQuery8,
      "The left time attribute 'rowtime' is TIMESTAMP_LTZ(3), but the right time attribute " +
        "'rowtime' is TIMESTAMP(3). An event-time temporal join needs both of the same type. " +
        "Change the schema of one table so that its WATERMARK column has the matching type. " +
        "Alternatively, declare the WATERMARK on a computed column of the matching type, " +
        "e.g. CAST(rowtime AS TIMESTAMP_LTZ(3)) in the right table. A CAST in the query does " +
        "not work, because it turns the time attribute into a regular column.",
      classOf[ValidationException]
    )

    val noEquiJoinCondition =
      "A temporal join needs an equality condition between a column of the left side and a " +
        "column of the right side in its ON clause, e.g. ON l.id = r.id."
    val sqlQueryOnTrue = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r ON TRUE"
    expectExceptionThrown(sqlQueryOnTrue, noEquiJoinCondition, classOf[ValidationException])

    val sqlQueryNonEqui = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency OR o.amount > r.rate"
    expectExceptionThrown(sqlQueryNonEqui, noEquiJoinCondition, classOf[ValidationException])

    val sqlQueryWhere = "SELECT * " +
      "FROM Orders AS o, " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "WHERE o.currency = r.currency"
    expectExceptionThrown(sqlQueryWhere, noEquiJoinCondition, classOf[ValidationException])

    val sqlQuery9 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF 'o.rowtime' AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery9,
      "The system time period specification expects Timestamp type but is 'CHAR'",
      classOf[ValidationException]
    )

    val sqlQuery10 = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.rowtime + INTERVAL '1' SECOND AS r " +
      "ON o.currency = r.currency"
    expectExceptionThrown(
      sqlQuery10,
      "FOR SYSTEM_TIME AS OF must reference a time attribute column of the left side of the " +
        "temporal join directly, not an expression.",
      classOf[ValidationException]
    )
  }

  @Test
  def testEventTimeTemporalJoinToSinkWithPk(): Unit = {
    val tEnv = util.tableEnv
    tEnv.executeSql(s"""
                       |CREATE TABLE orders_rowtime (
                       |  order_id BIGINT,
                       |  currency STRING,
                       |  currency_no STRING,
                       |  amount BIGINT,
                       |  order_time TIMESTAMP(3),
                       |  WATERMARK FOR order_time AS order_time,
                       |  PRIMARY KEY (order_id) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = 'rowTimeOrderDataId'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"""
                       |CREATE TABLE versioned_currency_with_multi_key (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
                       |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = 'rowTimeCurrencyDataId'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"""
                       |CREATE TABLE rowtime_default_sink (
                       |  order_id BIGINT,
                       |  currency STRING,
                       |  amount BIGINT,
                       |  l_time TIMESTAMP(3),
                       |  rate BIGINT,
                       |  r_time TIMESTAMP(3),
                       |  PRIMARY KEY(order_id) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false',
                       |  'changelog-mode' = 'I,UA,UB,D'
                       |)
                       |""".stripMargin)
    val sql =
      """
        |INSERT INTO rowtime_default_sink
        |  SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time
        |  FROM orders_rowtime AS o JOIN versioned_currency_with_multi_key
        |    FOR SYSTEM_TIME AS OF o.order_time as r
        |      ON o.currency_no = r.currency_no AND o.currency = r.currency
        |ON CONFLICT DO DEDUPLICATE
        |""".stripMargin
    util.verifyExplainInsert(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTemporalJoinUpsertSourceWithPostFilter(): Unit = {
    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "UpsertRates FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency WHERE valid = 'true'"

    util.verifyExplain(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTemporalJoinUpsertSourceWithPreFilter(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY VIEW V1 AS
                                |SELECT * FROM UpsertRates WHERE valid = 'true'
                                |""".stripMargin)

    /**
     * The problem is: there's exists a filter on an upsert changelog input(changelogMode=[I,UA,D]),
     * the UB message must exists for correctness.
     *
     * Intermediate plan with modify kind:
     * {{{
     * +- TemporalJoin(joinType=[InnerJoin], ..., changelogMode=[I])
     *    :- Exchange(distribution=[hash[currency]], changelogMode=[I])
     *      : +- WatermarkAssigner(rowtime=[rowtime], watermark=[rowtime], changelogMode=[I])
     *        : +- Calc(select=[amount, currency, rowtime, ... changelogMode=[I])
     *          : +- TableSourceScan(table= Orders ... changelogMode=[I])
     *    +- Exchange(distribution=[hash[currency]], changelogMode=[I,UA,D])
     *      +- Calc(select=[currency, ... where=[=(valid, _UTF-16LE'true')], changelogMode=[I,UA,D])
     *        +- WatermarkAssigner(rowtime=[rowtime], watermark=[rowtime], changelogMode=[I,UA,D])
     *          +- TableSourceScan(table= UpsertRates, ... changelogMode=[I,UA,D])
     * }}}
     */

    val sqlQuery = "SELECT * " +
      "FROM Orders AS o JOIN " +
      "V1 FOR SYSTEM_TIME AS OF o.rowtime AS r " +
      "ON o.currency = r.currency"

    expectExceptionThrown(
      sqlQuery,
      "Filter is not allowed for right changelog input of event time temporal join, it will corrupt the versioning of data.",
      classOf[TableException]
    )
  }

  private def expectExceptionThrown(
      sql: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    val callable: ThrowingCallable = () => verifyTranslationSuccess(sql)
    if (keywords != null) {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
        .withMessageContaining(keywords)
    } else {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
    }
  }

  private def verifyTranslationSuccess(sql: String): Unit = {
    util.tableEnv.sqlQuery(sql).explain()
  }
}
