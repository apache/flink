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
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestData, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.Seq

@RunWith(classOf[Parameterized])
class SemiAntiJoinStreamITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state)  {

  override def before(): Unit = {
    super.before()
    val tableA = failingDataSource(TestData.smallTupleData3)
      .toTable(tEnv, 'a1, 'a2, 'a3)
    val tableB = failingDataSource(TestData.tupleData5)
      .toTable(tEnv, 'b1, 'b2, 'b3, 'b4, 'b5)
    tEnv.registerTable("A", tableA)
    tEnv.registerTable("B", tableB)
  }

  val data = List(
    (1, 1L, 0, "Hallo", 1L),
    (2, 2L, 1, "Hallo Welt", 2L),
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  val data2 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world")
  )

  val dataCannotBeJoin = List(
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  @Test
  def testGenericSemiJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT a, b, c FROM ds1 WHERE a in (SELECT d from ds2 WHERE d < 3)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,Hi", "2,2,Hello")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinWithOneSideRetraction(): Unit = {
    val leftTable = List(
      (1, "a"),
      (2, "b"),
      (10, "c"),
      (6, "d"),
      (8, "e")
    )

    val rightTable = List(
      (0, "a"),
      (1, "a"),
      (1, "b"),
      (1, "b"),
      (1, "c"),
      (2, "c"),
      (3, "c"),
      (4, "c"),
      (1, "d"),
      (2, "d"),
      (3, "d"),
      (4, "e"),
      (4, "e")
    )

    val ds1 = failingDataSource(leftTable).toTable(tEnv, 'a, 'b)
    val ds2 = failingDataSource(rightTable).toTable(tEnv, 'c, 'd)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT a FROM ds1 WHERE a in (SELECT sum(c) from ds2 GROUP BY d)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("1", "2", "10", "6", "8")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinWithRetractTwoSidesRetraction(): Unit = {

    val tableData = List(
      (0, "a"),
      (1, "a"),
      (1, "b"),
      (1, "b"),
      (1, "c"),
      (2, "c"),
      (3, "c"),
      (4, "c"),
      (1, "d"),
      (2, "d"),
      (3, "d"),
      (3, "e"),
      (5, "e")
    )
    val ds1 = failingDataSource(tableData).toTable(tEnv, 'a, 'b)
    val ds2 = failingDataSource(tableData).toTable(tEnv, 'c, 'd)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val ds3 = tEnv.sqlQuery("SELECT sum(a) as a FROM ds1 GROUP BY b")
    tEnv.registerTable("ds3", ds3)
    val query = "SELECT a FROM ds3 WHERE a in (SELECT sum(c) from ds2 GROUP BY d)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1", "2", "10", "6", "8")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testGenericAntiJoin(): Unit = {
    val ds1 = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    val ds2 = failingDataSource(data2).toTable(tEnv, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT c FROM ds1 WHERE NOT EXISTS (SELECT * from ds2 WHERE b = g)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("2", "3", "4", "5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAntiJoinWithOneSideRetraction(): Unit = {
    val leftTable = List(
      (1, "a"),
      (2, "b"),
      (10, "c"),
      (6, "d"),
      (8, "e"),
      (11, "f")
    )

    val rightTable = List(
      (0, "a"),
      (1, "a"),
      (1, "b"),
      (1, "b"),
      (1, "c"),
      (2, "c"),
      (3, "c"),
      (4, "c"),
      (1, "d"),
      (2, "d"),
      (3, "d"),
      (4, "e"),
      (4, "e")
    )

    val ds1 = failingDataSource(leftTable).toTable(tEnv, 'a, 'b)
    val ds2 = failingDataSource(rightTable).toTable(tEnv, 'c, 'd)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val ds3 = tEnv.sqlQuery("SELECT SUM(c) as c FROM ds2 GROUP BY d")
    tEnv.registerTable("ds3", ds3)
    val query = "SELECT * FROM ds1 WHERE NOT EXISTS (SELECT c from ds3 WHERE a = c)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("11,f")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAntiJoinWithTwoSidesRetraction(): Unit = {
    val leftTable = List(
      (0, "a"),
      (5, "f"),
      (-2, "a"),
      (1, "b"),
      (1, "b"),
      (1, "c"),
      (2, "c"),
      (3, "c"),
      (1, "f"),
      (4, "c"),
      (1, "d"),
      (2, "d"),
      (3, "d"),
      (4, "e"),
      (3, "a"),
      (3, "e"),
      (2, "f")
    )

    val rightTable = List(
      (0, "a"),
      (1, "a"),
      (1, "b"),
      (1, "b"),
      (1, "c"),
      (2, "c"),
      (3, "c"),
      (4, "c"),
      (1, "d"),
      (2, "d"),
      (3, "d"),
      (4, "e"),
      (3, "e")
    )

    val ds1 = failingDataSource(leftTable).toTable(tEnv, 'a, 'b)
    val ds2 = failingDataSource(rightTable).toTable(tEnv, 'c, 'd)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val ds3 = tEnv.sqlQuery("SELECT SUM(c) as c FROM ds2 GROUP BY d")
    tEnv.registerTable("ds3", ds3)
    val ds4 = tEnv.sqlQuery("SELECT SUM(a) as a, b FROM ds1 GROUP BY b")
    tEnv.registerTable("ds4", ds4)
    val query = "SELECT * FROM ds4 WHERE NOT EXISTS (SELECT c from ds3 WHERE a = c)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("8,f")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE a1 in (SELECT b1 from B)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinNonEqui(): Unit = {
    val query = "SELECT * FROM A WHERE a1 in (SELECT b1 from B WHERE a2 < b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE a1 in (SELECT b1 from ($query2) WHERE a2 < b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT * FROM ($query1) WHERE a1 in (SELECT b1 from B WHERE a2 < b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "2,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSemiJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE a2 in (SELECT b2 from ($query2) WHERE a1 > b1)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    assertEquals(0, sink.getRetractResults.size)
  }

  @Test
  def testAntiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    assertEquals(0, sink.getRetractResults.size)
  }

  @Test
  def testAntiJoinNonEqui(): Unit = {
    val query = "SELECT * FROM A WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1 AND a2 < b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,Hi")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAntiJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b1 from ($query2) WHERE a1 = " +
      s"b1 AND a2 < b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAntiJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1 AND a2" +
      s" > b2)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "1,1", "2,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAntiJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b2 from ($query2) WHERE a2 = " +
      s"b2 AND a1 > b1)"
    val result = tEnv.sqlQuery(query)

    val sink = new TestingRetractSink
    result.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamNotInWithoutEqual(): Unit = {
    val data1 = List(
      (1, 1),
      (1, 1),
      (2, 2),
      (2, 2),
      (3, 3),
      (3, 3),
      (4, 4),
      (4, 4),
      (5, 5),
      (5, 5))

    val data2 = List(
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 5),
      (6, 6),
      (7, 7),
      (8, 8),
      (9, 9),
      (10, 10))

    val ds1 = failingDataSource(data1).toTable(tEnv, 'pk, 'a)
    val ds2 = failingDataSource(data2).toTable(tEnv, 'pk, 'a)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)

    val sql =
      """
        |SELECT pk FROM ds1 WHERE pk not in
        |(SELECT pk FROM ds1 WHERE pk > 3)
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1", "1",
      "2", "2",
      "3", "3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamExistsWithoutEqual(): Unit = {
    val data1 = List(
      (10, "ACCOUNTING", "NEW YORK"),
      (20, "RESEARCH", "DALLAS"),
      (30, "SALES", "CHICAGO"),
      (40, "OPERATIONS", "BOSTON"))

    val data2 = List(
      (7369, "SMITH", 20),
      (7499, "ALLEN", 30),
      (7566, "JONES", 20),
      (7654, "MARTIN", 30))

    val ds1 = failingDataSource(data1).toTable(tEnv, 'deptno, 'dname, 'loc)
    val ds2 = failingDataSource(data2).toTable(tEnv, 'empno, 'ename, 'deptno)
    tEnv.registerTable("scott_dept", ds1)
    tEnv.registerTable("scott_emp", ds2)

    val sql =
      """
        |select *
        |from scott_dept as d
        |where exists (select 1 from scott_emp where empno > d.deptno)
        |and exists (select 0 from scott_emp where deptno = d.deptno and ename = 'SMITH')
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("20,RESEARCH,DALLAS")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamNotExistsWithoutEqual(): Unit = {
    val data1 = List(
      (1, 1),
      (1, 1),
      (2, 2),
      (2, 2),
      (3, 3),
      (3, 3),
      (4, 4),
      (4, 4),
      (5, 5),
      (5, 5))

    val data2 = List(
      (5, 5),
      (6, 6),
      (7, 7),
      (8, 8),
      (9, 9),
      (10, 10))

    val ds1 = failingDataSource(data1).toTable(tEnv, 'pk, 'a)
    val ds2 = failingDataSource(data2).toTable(tEnv, 'pk, 'a)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)

    val sql =
      """
        |SELECT pk FROM ds1 WHERE NOT EXISTS
        |(SELECT 1 FROM ds2 WHERE ds2.pk < ds1.pk)
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1", "1",
      "2", "2",
      "3", "3",
      "4", "4",
      "5", "5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}

