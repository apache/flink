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

import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}

/**
 * Tests for pushing filter into table scan
 */
class FilterableSourceTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    val ddl =
      """
        | CREATE TABLE MyTable(
        |   a INT,
        |   b BIGINT,
        |   c TIMESTAMP(3),
        |   d STRING,
        |   WATERMARK FOR c AS c
        | ) WITH (
        |   'connector' = 'values',
        |   'enable-watermark-push-down' = 'true',
        |   'filterable-fields' = 'a;d',
        |   'bounded' = 'false',
        |   'disable-lookup' = 'true'
        | )
        |""".stripMargin

    util.tableEnv.executeSql(ddl)
  }

  @Test
  def testFullFilterMatchWithWatermark(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE LOWER(d) = 'hello'")
  }

  @Test
  def testPartialFilterMatchWithWatermark(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE LOWER(d) = 'h' AND d IS NOT NULL")
  }

  @Test
  def testNoFilterMatchWithWatermark(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable WHERE b > 5")
  }

  @Test def testFullPushdownWithoutWatermarkAssigner(): Unit = {
    val ddl =
      """
        |CREATE TABLE NoWatermark (
        |  name STRING,
        |  event_time TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'values',
        |  'filterable-fields' = 'name',
        |  'bounded' = 'false',
        |  'disable-lookup' = 'true'
        |)
        |""".stripMargin

    util.tableEnv.executeSql(ddl)
    util.verifyExecPlan(
      "SELECT * FROM NoWatermark WHERE LOWER(name) = 'foo' AND UPPER(name) = 'FOO'"
    )
  }

  @Test
  def testPartialPushdownWithoutWatermarkAssigner(): Unit = {
    val ddl =
      """
        |CREATE TABLE NoWatermark (
        |  name STRING,
        |  event_time TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'values',
        |  'filterable-fields' = 'name',
        |  'bounded' = 'false',
        |  'disable-lookup' = 'true'
        |)
        |""".stripMargin

    util.tableEnv.executeSql(ddl)
    util.verifyExecPlan("SELECT * FROM NoWatermark WHERE LOWER(name) = 'foo' AND name IS NOT NULL")
  }

  @Test
  def testComputedColumnPushdownAcrossWatermark() {
    val ddl =
      """
        |CREATE TABLE WithWatermark (
        |  event_time TIMESTAMP(3),
        |  name STRING,
        |  lowercase_name AS LOWER(name),
        |  WATERMARK FOR event_time AS event_time
        |) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'false',
        |  'enable-watermark-push-down' = 'true',
        |  'filterable-fields' = 'name',
        |  'disable-lookup' = 'true'
        |)
        |""".stripMargin

    util.tableEnv.executeSql(ddl)
    util.verifyExecPlan(
      "SELECT * FROM WithWatermark WHERE lowercase_name = 'foo'")
  }

  @Test
  def testFilterPushdownWithUdf(): Unit = {
    JavaFunc5.closeCalled = false
    JavaFunc5.openCalled = false
    util.tableEnv.createTemporarySystemFunction("func", new JavaFunc5)
    val ddl =
      """
         | CREATE Table UdfTable (
         |   a INT,
         |   b BIGINT,
         |   c timestamp(3),
         |   d as func(c, a),
         |   f STRING,
         |   WATERMARK FOR c as func(func(d, a), a)
         | ) with (
         |   'connector' = 'values',
         |   'bounded' = 'false',
         |   'filterable-fields' = 'f',
         |   'enable-watermark-push-down' = 'true',
         |   'disable-lookup' = 'true'
         | )
         |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyExecPlan("SELECT * FROM UdfTable WHERE UPPER(f) = 'welcome'")
  }
}
