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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class GroupingSetsITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testGroupingSetsWithOneGrouping(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid, avg(id) as a, " +
      "  group_id() as g, " +
      "  grouping(gid) as gb, " +
      "  grouping_id(gid) as gib " +
      "from A " +
      "  group by grouping sets (gid)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "1,1.0,0,0,0",
      "2,2.5,0,0,0",
      "3,5.0,0,0,0",
      "4,8.5,0,0,0",
      "5,13.0,0,0,0",
      "6,18.5,0,0,0")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testBasicGroupingSets(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid, count(*) as c from A group by grouping sets ((), (gid))"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "1,1",
      "2,2",
      "3,3",
      "4,4",
      "5,5",
      "6,6",
      "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testGroupingSetsOnExpression(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, count(*) as c from A group by grouping sets ((), (gid + 1))"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "2,1",
      "3,2",
      "4,3",
      "5,4",
      "6,5",
      "7,6",
      "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCube(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, count(*) as c from A group by cube(gid, comment)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("2,1", "2,1", "3,1", "3,1", "3,2", "4,1",
      "4,1", "4,1", "4,3", "5,1", "5,1", "5,1", "5,1", "5,4", "6,1", "6,1", "6,1",
      "6,1", "6,1", "6,5", "7,1", "7,1", "7,1", "7,1", "7,1", "7,1", "7,6", "null,1",
      "null,1", "null,1", "null,1", "null,1", "null,1", "null,1", "null,1", "null,1",
      "null,1", "null,1", "null,1", "null,1", "null,1", "null,1", "null,1", "null,1",
      "null,1", "null,1", "null,1", "null,1", "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRollupOn1Column(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, count(*) as c from A group by rollup(gid)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "2,1",
      "3,2",
      "4,3",
      "5,4",
      "6,5",
      "7,6",
      "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRollupOn2Column(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, count(*) as c from A group by rollup(gid, comment)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("2,1", "2,1", "3,1", "3,1", "3,2", "4,1",
      "4,1", "4,1", "4,3", "5,1", "5,1", "5,1", "5,1", "5,4", "6,1", "6,1", "6,1",
      "6,1", "6,1", "6,5", "7,1", "7,1", "7,1", "7,1", "7,1", "7,1", "7,6", "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRollupWithHaving(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, count(*) as c from A group by rollup(gid) having count(*) > 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("5,4", "6,5", "7,6", "null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCubeAndDistinct(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select distinct count(*) from A group by rollup(gid, comment)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("1", "2", "3", "4", "5", "6", "21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDuplicateArgumentToGrouping_id(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid, comment, " +
      "  grouping_id(comment, gid, comment), " +
      "  count(*) as c " +
      "from A " +
      "  where gid = 5 " +
      "  group by rollup(gid, comment)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "5,Comment#5,0,1",
      "5,Comment#6,0,1",
      "5,Comment#7,0,1",
      "5,Comment#8,0,1",
      "5,Comment#9,0,1",
      "5,null,5,5",
      "null,null,7,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testGroupingInSelectClauseOfRollupQuery(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select count(*) as c, gid, grouping(gid) as g from A group by rollup(gid)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "1,1,0",
      "2,2,0",
      "3,3,0",
      "4,4,0",
      "5,5,0",
      "6,6,0",
      "21,null,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testGroupingGrouping_idAndGroup_id(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid, comment, grouping(gid), grouping(comment), " +
      "  grouping_id(gid, comment), " +
      "  grouping_id(comment, gid), " +
      "  group_id(), count(*) " +
      "from A " +
      "  group by cube(gid, comment) " +
      "  having gid > 5"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "6,Comment#10,0,0,0,0,0,1",
      "6,Comment#11,0,0,0,0,0,1",
      "6,Comment#12,0,0,0,0,0,1",
      "6,Comment#13,0,0,0,0,0,1",
      "6,Comment#14,0,0,0,0,0,1",
      "6,Comment#15,0,0,0,0,0,1",
      "6,null,0,1,1,2,0,6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }


  @Test
  def testAllowExpressionInRollup(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select gid + 1, gid + 1 - 1, count(*) from A group by rollup(gid + 1)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "2,1,1",
      "3,2,2",
      "4,3,3",
      "5,4,4",
      "6,5,5",
      "7,6,6",
      "null,null,21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAllowExpressionInCube(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select mod(gid, 4), comment, count(*) from A " +
      "  group by cube(mod(gid, 4), comment) having mod(gid, 4) = 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "3,Hello world, how are you?,1",
      "3,I am fine.,1",
      "3,Luke Skywalker,1",
      "3,null,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRollupConstant(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select count(*) as c from A group by rollup(1)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("21", "21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCubeConstant(): Unit = {
    val tableA = failingDataSource(StreamTestData.get3TupleData)
      .toTable(tEnv, 'id, 'gid, 'comment)
    tEnv.registerTable("A", tableA)

    val sqlQuery = "select count(*) as c from A group by cube(1)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = mutable.MutableList("21", "21")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
