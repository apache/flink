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
import org.apache.flink.streaming.api.transformations.SinkTransformation
import org.apache.flink.table.api._
import org.apache.flink.table.connector.source.abilities.{SupportsParallelismReport, SupportsStatisticsReport}
import org.apache.flink.table.operations.ModifyOperation
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Assert, Before, Test}

import _root_.scala.collection.JavaConversions._

/**
  * Test for [[SupportsParallelismReport]] and [[SupportsStatisticsReport]].
  */
class SourceReportsTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Int)]("MyTable", 'a, 'b)
  }

  @Test
  def testJoin(): Unit = {
    // should be hash join
    util.tableEnv.executeSql(
      s"""
         |create table ReportTable (i int, j int) with (
         |'connector'='values', 'bounded'='true')
         |""".stripMargin)
    util.verifyExplain("SELECT * FROM MyTable, ReportTable WHERE i = a")
  }

  @Test
  def testJoinWithStatistics(): Unit = {
    // should be broadcast join
    util.tableEnv.executeSql(
      s"""
         |create table ReportTable (i int, j int) with (
         |'connector'='values', 'bounded'='true', 'row-count'='999')
         |""".stripMargin)
    util.verifyExplain("SELECT * FROM MyTable, ReportTable WHERE i = a")
  }

  @Test
  def testParallelismReport(): Unit = {
    util.tableEnv.executeSql(
      s"""
         |create table SinkTable (i int, j int) with (
         |'connector'='values', 'bounded'='true', 'parallelism'='111')
         |""".stripMargin)
    util.tableEnv.executeSql(
      s"""
         |create table ReportTable (i int, j int) with (
         |'connector'='values', 'bounded'='true', 'parallelism'='222')
         |""".stripMargin)
    val transformations = util.getPlanner.translate(
      util.getPlanner.getParser.parse("INSERT INTO SinkTable SELECT * FROM ReportTable")
          .map(_.asInstanceOf[ModifyOperation]))
    Assert.assertEquals(111, transformations.head.getParallelism)
    val source = transformations.head.asInstanceOf[SinkTransformation[_]].getInput
    Assert.assertEquals(222, source.getParallelism)
  }

}


