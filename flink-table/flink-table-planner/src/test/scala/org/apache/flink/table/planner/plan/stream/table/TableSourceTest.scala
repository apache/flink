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
package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.core.testutils.FlinkMatchers.containsMessage
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class TableSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id int,
         |  rowtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  watermark for rowtime as rowtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("rowTimeT").select($"rowtime", $"id", $"name", $"val")
    util.verifyExecPlan(t)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id int,
         |  rowtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  watermark for rowtime as rowtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv
      .from("rowTimeT")
      .where($"val" > 100)
      .window(Tumble.over(10.minutes).on('rowtime).as('w))
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)
    util.verifyExecPlan(t)
  }

  @Test
  def testRowTimeTableSourceGroupWindowWithNotNullRowTimeType(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE rowTimeT (
         |  id int,
         |  rowtime timestamp(3) not null,
         |  val bigint,
         |  name varchar(32),
         |  watermark for rowtime as rowtime - INTERVAL '5' SECONDS
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv
      .from("rowTimeT")
      .where($"val" > 100)
      .window(Tumble.over(10.minutes).on('rowtime).as('w))
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)
    util.verifyExecPlan(t)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE procTimeT (
         |  id int,
         |  val bigint,
         |  name varchar(32),
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv
      .from("procTimeT")
      .window(Over.partitionBy('id).orderBy('proctime).preceding(2.hours).as('w))
      .select('id, 'name, 'val.sum.over('w).as('valSum))
      .filter('valSum > 100)
    util.verifyExecPlan(t)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('ptime, 'name, 'val, 'id)
    util.verifyExecPlan(t)
  }

  @Test
  def testProjectWithoutProctime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('name, 'val, 'rtime, 'id)
    util.verifyExecPlan(t)
  }

  @Test
  def testProctimeOnWatermarkSpec(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expect(
      containsMessage("A watermark can not be defined for a processing-time attribute."))

    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for ptime as ptime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('ptime)
    util.verifyExecPlan(t)
  }

  @Test
  def testProjectOnlyRowtime(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  rtime timestamp(3),
         |  val bigint,
         |  name varchar(32),
         |  ptime as PROCTIME(),
         |  watermark for rtime as rtime
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('rtime)
    util.verifyExecPlan(t)
  }

  @Test
  def testNestedProject(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE T (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |                 nested2 row<num int, flag boolean>>,
         |  nested row<name string, `value` int>,
         |  name string
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'false',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv
      .from("T")
      .select(
        'id,
        'deepNested.get("nested1").get("name").as('nestedName),
        'nested.get("value").as('nestedValue),
        'deepNested.get("nested2").get("flag").as('nestedFlag),
        'deepNested.get("nested2").get("num").as('nestedNum)
      )
    util.verifyExecPlan(t)
  }

}
