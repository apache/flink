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

import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5

import org.junit.{Before, Ignore, Test}

/**
 * Tests for watermark push down.
 */
class SourceWatermarkTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    val ddl1 =
      """
        | CREATE TABLE VirtualTable (
        |   a INT,
        |   b BIGINT,
        |   c TIMESTAMP(3),
        |   d AS c + INTERVAL '5' SECOND,
        |   WATERMARK FOR d AS d - INTERVAL '5' SECOND
        | ) WITH (
        |   'connector' = 'values',
        |   'enable-watermark-push-down' = 'true',
        |   'bounded' = 'false',
        |   'disable-lookup' = 'true'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl1)

    val ddl2 =
      """
        | CREATE TABLE NestedTable (
        |   a INT,
        |   b BIGINT,
        |   c ROW<name STRING, d ROW<e STRING, f TIMESTAMP(3)>>,
        |   g AS c.d.f,
        |   WATERMARK FOR g AS g - INTERVAL '5' SECOND
        | ) WITH (
        |   'connector' = 'values',
        |   'enable-watermark-push-down' = 'true',
        |   'nested-projection-supported' = 'true',
        |   'bounded' = 'false',
        |   'disable-lookup' = 'true'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl2)

    JavaFunc5.closeCalled = false
    JavaFunc5.openCalled = false
    util.tableEnv.createTemporarySystemFunction("func", new JavaFunc5)
    val ddl3 =
      s"""
         | CREATE Table UdfTable (
         |   a INT,
         |   b BIGINT,
         |   c timestamp(3),
         |   d as func(c, a),
         |   WATERMARK FOR c as func(func(d, a), a)
         | ) with (
         |   'connector' = 'values',
         |   'bounded' = 'false',
         |   'enable-watermark-push-down' = 'true',
         |   'disable-lookup' = 'true'
         | )
         |""".stripMargin
    util.tableEnv.executeSql(ddl3)
  }

  @Test
  def testWatermarkOnComputedColumnExcludedRowTime2(): Unit = {
    util.verifyPlan("SELECT a, b, SECOND(d) FROM VirtualTable")
  }

  @Test
  def testWatermarkOnComputedColumnExcluedRowTime1(): Unit = {
    util.verifyPlan("SELECT a, b FROM VirtualTable WHERE b > 10")
  }

  @Test
  def testWatermarkOnNestedRowWithNestedProjection(): Unit = {
    util.verifyPlan("select c.e, c.d from NestedTable")
  }

  @Test
  def testWatermarkWithUdf(): Unit = {
    util.verifyPlan("SELECT a - b FROM UdfTable")
  }

  @Ignore
  @Test
  def testWatermarkWithMetadata(): Unit = {
    // TODO(FLINK-20029): define computed column on the metadata
    val ddl =
      """
        | CREATE TABLE MyTable(
        |   a INT,
        |   b BIGINT,
        |   c TIMESTAMP(3),
        |   originTime BIGINT METADATA,
        |   rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(originTime/1000), 'yyyy-MM-dd HH:mm:ss'),
        |   WATERMARK FOR rowtime AS rowtime
        | ) WITH (
        |   'connector' = 'values',
        |   'enable-watermark-push-down' = 'true',
        |   'bounded' = 'false',
        |   'disable-lookup' = 'true',
        |   'readable-metadata' = 'originTime:BIGINT'
        | )
        |""".stripMargin

    util.tableEnv.executeSql(ddl)
    util.verifyPlan("SELECT a, b FROM MyTable")
  }
}
