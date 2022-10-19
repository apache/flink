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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.Seq

@RunWith(classOf[Parameterized])
class RemoveShuffleITCase(state: StateBackendMode) extends StreamingWithStateTestBase(state) {

  override def before(): Unit = {
    super.before()
    val tableT1 = failingDataSource(TestData.smallTupleData3).toTable(tEnv, 'a1, 'b1, 'c1)
    val tableT2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'a2, 'b2, 'c2, 'd2, 'e2)
    tEnv.registerTable("T1", tableT1)
    tEnv.registerTable("T2", tableT2)
  }

  @Test
  def testMultipleInnerJoinsWithMultipleKeys(): Unit = {
    val query =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND b1 = e2 AND a2 = a3 AND b3 = e2 AND a3 = a4 AND b3 = b4
         """.stripMargin

    val t3Data = List((3, 1L, "HiHi"), (2, 1L, "HelloHello"), (3, 2L, "Hello world!"))
    tEnv.registerTable("T3", failingDataSource(t3Data).toTable(tEnv, 'a3, 'b3, 'c3))

    val t4Data = List((1, 1L, 0), (1, 2L, 1), (2, 3L, 2), (3, 1L, 3), (3, 5L, 4), (3, 2L, 5))
    tEnv.registerTable("T4", failingDataSource(t4Data).toTable(tEnv, 'a4, 'b4, 'c4))

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "3,2,Hello world,3,4,3,Hallo Welt wie gehts?,2,3,2,Hello world!,3,2,5",
      "3,2,Hello world,3,5,4,ABC,2,3,2,Hello world!,3,2,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithAggs(): Unit = {
    val query1 = "SELECT a1, SUM(b1) AS b1 FROM T1 GROUP BY a1"
    val query2 = "SELECT a2, SUM(b2) AS b2 FROM T2 GROUP BY a2"
    val query = s"SELECT * FROM ($query1) JOIN ($query2) ON a1 = a2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1", "2,2,2,5", "3,2,3,15")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggWithJoinFromLeftKeys(): Unit = {
    val query =
      s"""
         |SELECT a1, b1, COUNT(a2), SUM(b2) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY a1, b1
         """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1", "2,2,1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggWithJoinFromRightKeys(): Unit = {
    val query =
      s"""
         |SELECT a2, b2, COUNT(a1), SUM(b1) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY b2, a2
         """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1", "2,2,1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMultipleGroupAggWithSameKeys(): Unit = {
    val query =
      s"""
         |SELECT a1, count(*) FROM (SELECT a1 FROM T1 GROUP BY a1) t GROUP BY a1
         """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,1", "3,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}
