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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api
import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}

class TableScanTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.registerFunction("my_udf", Func0)

    util.addTable(
      s"""
         |create table computed_column_t(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a)
         |) with (
         |  'connector' = 'values',
         |  'bounded' = 'true'
         |)
       """.stripMargin)

    util.addTable(
      s"""
         |create table c_watermark_t(
         |  a int,
         |  b varchar,
         |  c as a + 1,
         |  d as to_timestamp(b),
         |  e as my_udf(a),
         |  WATERMARK FOR d AS d - INTERVAL '0.001' SECOND
         |) with (
         |  'connector' = 'values',
         |  'bounded' = 'true'
         |)
       """.stripMargin)
  }

  @Test
  def testLegacyTableSourceScan(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.verifyExecPlan("SELECT * FROM MyTable")
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
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |)
      """.stripMargin)
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testScanOnUnboundedSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false'
        |)
      """.stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Querying an unbounded table 'default_catalog.default_database.src' in batch mode is not " +
      "allowed. The table source is unbounded.")
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
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
        |  'bounded' = 'true',
        |  'changelog-mode' = 'I,UA,UB'
        |)
      """.stripMargin)
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Querying a table in batch mode is currently only possible for INSERT-only table sources. " +
       "But the source for table 'default_catalog.default_database.src' produces other changelog " +
       "messages than just INSERT.")
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testScanOnUpsertSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  id STRING,
        |  a INT,
        |  b DOUBLE,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true',
        |  'changelog-mode' = 'UA,D'
        |)
      """.stripMargin)
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Querying a table in batch mode is currently only possible for INSERT-only table sources. " +
      "But the source for table 'default_catalog.default_database.src' produces other changelog " +
      "messages than just INSERT.")
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testDDLWithComputedColumn(): Unit = {
    util.verifyExecPlan("SELECT * FROM computed_column_t")
  }

  @Test
  def testDDLWithWatermarkComputedColumn(): Unit = {
    util.verifyExecPlan("SELECT * FROM c_watermark_t")
  }

  @Test
  def testDDLWithProctime(): Unit = {
    util.addTable(
      s"""
        |create table proctime_t (
        | a int,
        | b varchar,
        | c as a + 1,
        | d as to_timestamp(b),
        | e as my_udf(a),
        | ptime as proctime()
        |) with (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |)
      """.stripMargin
    )
    util.verifyExecPlan("SELECT * FROM proctime_t")
  }

  @Test
  def testTableApiScanWithComputedColumn(): Unit = {
    util.verifyExecPlan(util.tableEnv.from("computed_column_t"))
  }

  @Test
  def testTableApiScanWithWatermark(): Unit = {
    util.verifyExecPlan(util.tableEnv.from("c_watermark_t"))
  }

  @Test
  def testTableApiScanWithDDL(): Unit = {
    util.addTable(
      s"""
         |create table t1(
         |  a int,
         |  b varchar
         |) with (
         |  'connector' = 'values',
         |  'bounded' = 'true'
         |)
       """.stripMargin)
    util.verifyExecPlan(util.tableEnv.from("t1"))
  }

  @Test
  def testTableApiScanWithTemporaryTable(): Unit = {
    util.tableEnv.createTemporaryTable("t1", TableDescriptor.forConnector("datagen")
      .schema(Schema.newBuilder()
        .column("word", DataTypes.STRING)
        .build())
      .option("number-of-rows", "1")
      .build())

    util.verifyExecPlan(util.tableEnv.from("t1"))
  }
}
