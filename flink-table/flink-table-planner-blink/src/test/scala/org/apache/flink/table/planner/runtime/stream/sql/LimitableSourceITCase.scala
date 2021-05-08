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
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.rules.logical.PushLimitIntoTableSourceScanRule
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

/**
 * Test for [[PushLimitIntoTableSourceScanRule]].
 */
class LimitableSourceITCase extends StreamingTestBase() {

  val data = Seq(
    row("book", 1, 12),
    row("book", 2, 19),
    row("book", 4, 11),
    row("fruit", 4, 33),
    row("fruit", 3, 44),
    row("fruit", 5, 22))

  @Before
  def setup(): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
         |CREATE TABLE Source (
         |  category STRING,
         |  shopId INT,
         |  num INT
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)
  }

  @Test
  def testLimit(): Unit = {
    val sql = "SELECT * FROM Source LIMIT 4"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "book,1,12",
      "book,2,19",
      "book,4,11",
      "fruit,4,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testOffsetAndFetch(): Unit = {
    val sql = "SELECT * FROM Source LIMIT 4 OFFSET 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "book,4,11",
      "fruit,4,33",
      "fruit,3,44",
      "fruit,5,22")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}
