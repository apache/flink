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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}

class MatchRecognizeTest extends TableTestBase {

  protected val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def before(): Unit = {
    val ddl =
      """
        |CREATE TABLE Ticker (
        | `symbol` STRING,
        | `ts_ltz` TIMESTAMP_LTZ(3),
        | `price` INT,
        | `tax` INT,
        | WATERMARK FOR `ts_ltz` AS `ts_ltz` - INTERVAL '1' SECOND
        |) WITH (
        | 'connector' = 'values'
        |)
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
  }

  @Test
  def testMatchRecognizeOnRowtime(): Unit = {
    val ddl =
      """
        |CREATE TABLE Ticker1 (
        | `symbol` STRING,
        | `ts` TIMESTAMP(3),
        | `price` INT,
        | `tax` INT,
        | WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND
        |) WITH (
        | 'connector' = 'values'
        |)
        |""".stripMargin
    util.tableEnv.executeSql(ddl)

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker1
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME() as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMatchRecognizeOnRowtimeLTZ(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testWindowTVFOnMatchRecognizeOnRowtimeLTZ(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |""".stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    util.tableEnv.createTemporaryView("T", table)
    val sqlQuery1 =
      s"""
         |SELECT *
         |FROM TABLE(TUMBLE(TABLE T, DESCRIPTOR(matchRowtime), INTERVAL '3' second))
         |""".stripMargin
    util.verifyRelPlanWithType(sqlQuery1)
  }

  @Test
  def testOverWindowOnMatchRecognizeOnRowtimeLTZ(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |""".stripMargin
    val table = util.tableEnv.sqlQuery(sqlQuery)
    util.tableEnv.createTemporaryView("T", table)
    val sqlQuery1 =
      """
        |SELECT
        |  symbol,
        |  price,
        |  tax,
        |  matchRowtime,
        |  SUM(price) OVER (
        |    PARTITION BY symbol ORDER BY matchRowtime RANGE UNBOUNDED PRECEDING) as price_sum
        |FROM T
    """.stripMargin
    util.verifyRelPlanWithType(sqlQuery1)
  }

  @Test
  def testCascadeMatch(): Unit = {
    val sqlQuery =
      s"""
         |SELECT *
         |FROM (
         |  SELECT
         |    symbol,
         |    matchRowtime,
         |    price,
         |    TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |  FROM Ticker
         |  MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, matchRowtime, price, TUMBLE(matchRowtime, interval '3' second)
         |)
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY matchRowtime
         |  MEASURES
         |    A.price as dPrice,
         |    A.matchRowtime as matchRowtime
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.matchRowtime >= (CURRENT_TIMESTAMP - INTERVAL '1' day)
         |)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for Illegal use of Match_RowTime
  // ----------------------------------------------------------------------------------------

  @Test
  def testMatchRowtimeWithoutArgumentOnRowtimeLTZ(): Unit = {
    thrown.expectMessage(
      "MATCH_ROWTIME(rowtimeField) should be used when input stream " +
        "contains rowtime attribute with TIMESTAMP_LTZ type.\n" +
        "Please pass rowtime attribute field as input argument of " +
        "MATCH_ROWTIME(rowtimeField) function.")
    thrown.expect(classOf[AssertionError])

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME() as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMatchRowtimeWithMultipleArgs(): Unit = {
    thrown.expectMessage("Invalid number of arguments to function 'MATCH_ROWTIME'.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz, price) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMatchRowtimeWithNonRowTimeAttributeAsArgs(): Unit = {
    thrown.expectMessage(
      "The function MATCH_ROWTIME requires argument to be a row time attribute type, " +
        "but is 'INTEGER'.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(price) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMatchRowtimeWithRexCallAsArg(): Unit = {
    thrown.expectMessage(
      "The function MATCH_ROWTIME requires a field reference as argument, " +
        "but actual argument is not a simple field reference.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT
         |  symbol,
         |  SUM(price) as price,
         |  TUMBLE_ROWTIME(matchRowtime, interval '3' second) as rowTime,
         |  TUMBLE_START(matchRowtime, interval '3' second) as startTime
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY ts_ltz
         |  MEASURES
         |    A.price as price,
         |    A.tax as tax,
         |    MATCH_ROWTIME(ts_ltz + INTERVAL '1' SECOND) as matchRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |) AS T
         |GROUP BY symbol, TUMBLE(matchRowtime, interval '3' second)
         |""".stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
