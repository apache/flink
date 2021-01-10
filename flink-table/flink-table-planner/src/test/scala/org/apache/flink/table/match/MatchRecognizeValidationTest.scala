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

package org.apache.flink.table.`match`

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.runtime.stream.sql.ToMillis
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Test

class MatchRecognizeValidationTest extends TableTestBase {

  private val streamUtils = streamTestUtil()
  streamUtils.addTable[(String, Long, Int, Int)]("Ticker",
    'symbol,
    'tstamp,
    'price,
    'tax,
    'proctime.proctime)
  streamUtils.addFunction("ToMillis", new ToMillis)

  @Test
  def testSortProcessingTimeDesc(): Unit = {
    thrown.expectMessage("Primary sort order of a streaming table must be ascending on time.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime DESC
         |  MEASURES
         |    A.symbol AS aSymbol
         |  PATTERN (A B)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testSortProcessingTimeSecondaryField(): Unit = {
    thrown.expectMessage("You must specify either rowtime or proctime for order by as " +
      "the first one.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY price, proctime
         |  MEASURES
         |    A.symbol AS aSymbol
         |  PATTERN (A B)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testSortNoOrder(): Unit = {
    thrown.expectMessage("You must specify either rowtime or proctime for order by.")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  MEASURES
         |    A.symbol AS aSymbol
         |  PATTERN (A B)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testUpdatesInUpstreamOperatorNotSupported(): Unit = {
    thrown.expectMessage("Retraction on match recognize is not supported. Note: Match " +
      "recognize should not follow a non-windowed GroupBy aggregation.")
    thrown.expect(classOf[TableException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM (SELECT DISTINCT * FROM Ticker)
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.symbol AS aSymbol
         |  ONE ROW PER MATCH
         |  PATTERN (A B)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toRetractStream[Row]
  }

  @Test
  def testAggregatesOnMultiplePatternVariablesNotSupported(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("SQL validation failed.")

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    SUM(A.price + B.tax) AS taxedPrice
         |  PATTERN (A B)
         |  DEFINE
         |    A AS A.symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testAggregatesOnMultiplePatternVariablesNotSupportedInUDAGs(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Aggregation must be applied to a single pattern variable")

    streamUtils.tableEnv.registerFunction("weightedAvg", new WeightedAvg)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    weightedAvg(A.price, B.tax) AS weightedAvg
         |  PATTERN (A B)
         |  DEFINE
         |    A AS A.symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testValidatingAmbiguousColumns(): Unit = {
    thrown.expectMessage("Columns ambiguously defined: {symbol, price}")
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol, price
         |  ORDER BY proctime
         |  MEASURES
         |    A.symbol AS symbol,
         |    A.price AS price
         |  PATTERN (A)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  // ***************************************************************************************
  // * Those validations are temporary. We should remove those tests once we support those *
  // * features.                                                                           *
  // ***************************************************************************************

  @Test
  def testAllRowsPerMatch(): Unit = {
    thrown.expectMessage("All rows per match mode is not supported yet.")
    thrown.expect(classOf[TableException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.symbol AS aSymbol
         |  ALL ROWS PER MATCH
         |  PATTERN (A B)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testGreedyQuantifierAtTheEndIsNotSupported(): Unit = {
    thrown.expectMessage("Greedy quantifiers are not allowed as the last element of a " +
      "Pattern yet. Finish your pattern with either a simple variable or reluctant quantifier.")
    thrown.expect(classOf[TableException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.symbol AS aSymbol
         |  PATTERN (A B+)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testPatternsProducingEmptyMatchesAreNotSupported(): Unit = {
    thrown.expectMessage("Patterns that can produce empty matches are not supported. " +
      "There must be at least one non-optional state.")
    thrown.expect(classOf[TableException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.symbol AS aSymbol
         |  PATTERN (A*)
         |  DEFINE
         |    A AS symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }

  @Test
  def testDistinctAggregationsNotSupported(): Unit = {
    thrown.expect(classOf[ValidationException])

    val sqlQuery =
      s"""
         |SELECT *
         |FROM Ticker
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    COUNT(DISTINCT A.price) AS price
         |  PATTERN (A B)
         |  DEFINE
         |    A AS A.symbol = 'a'
         |) AS T
         |""".stripMargin

    streamUtils.tableEnv.sqlQuery(sqlQuery).toAppendStream[Row]
  }
}
