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

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Over, Tumble}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Ignore, Test}

class TableSourceTest extends TableTestBase {

  val util = streamTestUtil()

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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("rowTimeT").select($"rowtime", $"id", $"name", $"val")
    util.verifyPlan(t)
  }

  @Ignore("remove ignore once FLINK-17753 is fixed")
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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("rowTimeT")
      .where($"val" > 100)
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)
    util.verifyPlan(t)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE procTimeT (
         |  id int,
         |  val bigint,
         |  name varchar(32),
         |  proctime as PROCTIME(),
         |  watermark for proctime as proctime
         |) WITH (
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("procTimeT").select($"proctime", $"id", $"name", $"val")
    util.verifyPlan(t)
  }

  @Ignore("remove ignore once FLINK-17751 is fixed")
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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("procTimeT")
      .window(Over partitionBy 'id orderBy 'proctime preceding 2.hours as 'w)
      .select('id, 'name, 'val.sum over 'w as 'valSum)
      .filter('valSum > 100)
    util.verifyPlan(t)
  }

  @Test
  def testProjectWithRowtimeProctime(): Unit = {
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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('name, 'val, 'id)
    util.verifyPlan(t)
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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('ptime, 'name, 'val, 'id)
    util.verifyPlan(t)
  }

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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('name, 'val, 'rtime, 'id)
    util.verifyPlan(t)
  }

  def testProjectOnlyProctime(): Unit = {
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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('ptime)
    util.verifyPlan(t)
  }

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
         |  'connector' = 'projectable-values',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv.from("T").select('rtime)
    util.verifyPlan(t)
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
         |  'connector' = 'projectable-values',
         |  'nested-projection-supported' = 'false',
         |  'bounded' = 'false'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)

    val t = util.tableEnv
      .from("T")
      .select('id,
        'deepNested.get("nested1").get("name") as 'nestedName,
        'nested.get("value") as 'nestedValue,
        'deepNested.get("nested2").get("flag") as 'nestedFlag,
        'deepNested.get("nested2").get("num") as 'nestedNum)
    util.verifyPlan(t)
  }

}
