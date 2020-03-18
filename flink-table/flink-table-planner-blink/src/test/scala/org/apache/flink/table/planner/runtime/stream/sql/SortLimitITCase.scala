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
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class SortLimitITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def test(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.registerTable("T", ds)

    val sql = "SELECT * FROM T ORDER BY num DESC LIMIT 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "fruit,3,44",
      "fruit,4,33")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRetractSortLimit(): Unit = {
    val data = List(
      (1, 1), (1, 2), (1, 3),
      (2, 2), (2, 3), (2, 4),
      (3, 3), (3, 4), (3, 5))

    val ds = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", ds)

    // We use max here to ensure the usage of update rank
    val sql = "SELECT a, max(b) FROM T GROUP BY a ORDER BY a LIMIT 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,3",
      "2,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRetractSortLimitWithOffset(): Unit = {
    val data = List(
      (1, 1), (1, 2), (1, 3),
      (2, 2), (2, 3), (2, 4),
      (3, 3), (3, 4), (3, 5))

    val ds = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", ds)

    // We use max here to ensure the usage of update rank
    val sql = "SELECT a, max(b) FROM T GROUP BY a ORDER BY a LIMIT 2 OFFSET 1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "2,4",
      "3,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
