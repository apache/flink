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
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.factories.TestValuesTableFactory.{MockedFilterPushDownTableSource, MockedLookupTableSource}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class TableScanTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testLegacyTableSourceScan(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT * FROM MyTable")
  }

  @Test
  def testDataStreamScan(): Unit = {
    util.addDataStream[(Int, Long, String)]("DataStreamTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT * FROM DataStreamTable")
  }

  @Test
  def testDDLTableScan(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'values'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testDDLWithComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a)
         |) with (
         |  'connector' = 'values'
         |)
       """.stripMargin)
    util.verifyPlan("SELECT * FROM t1")
  }

  @Test
  def testDDLWithWatermarkComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a),
         |  WATERMARK FOR d AS d - INTERVAL '0.001' SECOND
         |) with (
         |  'connector' = 'values'
         |)
       """.stripMargin)
    util.verifyPlan("SELECT * FROM t1")
  }

  @Test
  def testDDLWithComputedColumnReferRowtime(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  my_ts AS ts - INTERVAL '0.001' SECOND,
        |  proc AS PROCTIME(),
        |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'values'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testKeywordsWithWatermarkComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  `time` time,
         |  mytime as `time`,
         |  `current_time` as current_time,
         |  json_row ROW<`timestamp` TIMESTAMP(3)>,
         |  `timestamp` AS json_row.`timestamp`,
         |  WATERMARK FOR `timestamp` AS `timestamp`
         |) with (
         |  'connector' = 'values'
         |)
       """.stripMargin)
    util.verifyPlan("SELECT * FROM t1")
  }

  @Test
  def testScanOnBoundedSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |)
      """.stripMargin)
    // pass
    util.verifyPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFilterOnChangelogSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testScanOnChangelogSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT b,a,ts FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnionChangelogSourceAndAggregation(): Unit = {
    util.addTable(
      """
        |CREATE TABLE changelog_src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
      """.stripMargin)
    util.addTable(
      """
        |CREATE TABLE append_src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I'
        |)
      """.stripMargin)

    val query = """
      |SELECT b, ts, a
      |FROM (
      |  SELECT * FROM changelog_src
      |  UNION ALL
      |  SELECT MAX(ts) as t, a, MAX(b) as b FROM append_src GROUP BY a
      |)
      |""".stripMargin
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAggregateOnChangelogSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB'
        |)
      """.stripMargin)
    util.verifyPlan("SELECT COUNT(*) FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testJoinOnChangelogSource(): Unit = {
    util.addTable(
      """
         |CREATE TABLE orders (
         |  amount BIGINT,
         |  currency STRING
         |) WITH (
         | 'connector' = 'values',
         | 'changelog-mode' = 'I'
         |)
         |""".stripMargin)
    util.addTable(
      """
         |CREATE TABLE rates_history (
         |  currency STRING,
         |  rate BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'I,UB,UA'
         |)
      """.stripMargin)

    val sql =
      """
        |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
        |FROM orders AS o JOIN rates_history AS r
        |ON o.currency = r.currency
        |""".stripMargin
    util.verifyPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnsupportedWindowAggregateOnChangelogSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts AS PROCTIME(),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,UB'
        |)
      """.stripMargin)
    val query =
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), COUNT(*)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
        |""".stripMargin
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "GroupWindowAggregate doesn't support consuming update changes " +
        "which is produced by node TableSourceScan")
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidSourceChangelogMode(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UB,D'
        |)
      """.stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "'default_catalog.default_database.src' source produces ChangelogMode " +
        "which contains UPDATE_BEFORE but doesn't contain UPDATE_AFTER, this is invalid.")
    util.verifyPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnsupportedSourceChangelogMode(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UA,D'
        |)
      """.stripMargin)
    thrown.expect(classOf[UnsupportedOperationException])
    thrown.expectMessage("Currently, ScanTableSource doesn't support producing " +
      "ChangelogMode which contains UPDATE_AFTER but no UPDATE_BEFORE. " +
      "Please adapt the implementation of 'TestValues' source.")
    util.verifyPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnsupportedWatermarkAndChangelogSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'I,UB,UA,D'
        |)
      """.stripMargin)
    thrown.expect(classOf[UnsupportedOperationException])
    thrown.expectMessage(
      "Currently, defining WATERMARK on a changelog source is not supported.")
    util.verifyPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidScanOnLookupSource(): Unit = {
    util.addTable(
      s"""
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE
        |) WITH (
        |  'connector' = 'values',
        |  'table-source-class' = '${classOf[MockedLookupTableSource].getName}'
        |)
      """.stripMargin)
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    util.verifyPlan("SELECT * FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnsupportedAbilityInterface(): Unit = {
    util.addTable(
      s"""
         |CREATE TABLE src (
         |  ts TIMESTAMP(3),
         |  a INT,
         |  b DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'table-source-class' = '${classOf[MockedFilterPushDownTableSource].getName}'
         |)
      """.stripMargin)
    thrown.expect(classOf[UnsupportedOperationException])
    thrown.expectMessage("DynamicTableSource with SupportsFilterPushDown ability is not supported")
    util.verifyPlan("SELECT * FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidWatermarkOutputType(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Watermark strategy '' must be of type TIMESTAMP but is of type 'CHAR(0) NOT NULL'.")
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR `ts` AS ''
        |) WITH (
        |  'connector' = 'values'
        |)
      """.stripMargin)
  }
}
