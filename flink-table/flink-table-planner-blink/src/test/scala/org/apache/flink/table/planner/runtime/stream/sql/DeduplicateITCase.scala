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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class DeduplicateITCase(miniBatch: MiniBatchMode, mode: StateBackendMode)
  extends StreamingWithMiniBatchTestBase(miniBatch, mode) {

  @Test
  def testFirstRowOnProctime(): Unit = {
    val t = failingDataSource(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "2,2,Hello", "4,3,Hello world, how are you?",
      "7,4,Comment#1", "11,5,Comment#5", "16,6,Comment#10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFirstRowOnBuiltinProctime(): Unit = {
    val t = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime()) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "2,2,Hello", "4,3,Hello world, how are you?",
      "7,4,Comment#1", "11,5,Comment#5", "16,6,Comment#10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLastRowOnProctime(): Unit = {
    val t = failingDataSource(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime DESC) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "3,2,Hello world", "6,3,Luke Skywalker",
      "10,4,Comment#4", "15,5,Comment#9", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLastRowOnBuiltinProctime(): Unit = {
    val t = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, b, c
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY b ORDER BY proctime() DESC) as rowNum
        |  FROM T
        |)
        |WHERE rowNum = 1
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "3,2,Hello world", "6,3,Luke Skywalker",
      "10,4,Comment#4", "15,5,Comment#9", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}
