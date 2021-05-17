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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.expressions.utils.FuncWithOpen
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.{Seq, mutable}

@RunWith(classOf[Parameterized])
class JoinITCase(state: StateBackendMode) extends StreamingWithStateTestBase(state) {

  val smallTuple5Data = List(
    (1, 1L, 0, "Hallo", 1L),
    (2, 2L, 1, "Hallo Welt", 2L),
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  val tuple3Data = List(
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

  override def before(): Unit = {
    super.before()
    val tableA = failingDataSource(TestData.smallTupleData3)
      .toTable(tEnv, 'a1, 'a2, 'a3)
    val tableB = failingDataSource(TestData.tupleData5)
      .toTable(tEnv, 'b1, 'b2, 'b3, 'b4, 'b5)
    tEnv.registerTable("A", tableA)
    tEnv.registerTable("B", tableB)
  }



  // Tests for inner join.
  override def after(): Unit = {}


  /** test non-window inner join **/
  @Test
  def testNonWindowInnerJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))
    data1.+=((3, 8L, "Hi9"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))
    data2.+=((3, 2L, "HeHe"))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b, 'c)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1
        |) as t1
        |JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2
        |) as t2
        |ON t1.a = t2.a AND t1.b > t2.b
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi2",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5")

    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIsNullInnerJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))
    data1.+=((3, 8L, "Hi9"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))
    data2.+=((3, 2L, "HeHe"))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b, 'c)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t2.a, t2.c, t1.c
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1
        |) as t1
        |JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2
        |) as t2
        |ON
        |  ((t1.a is null AND t2.a is null) OR
        |  (t1.a = t2.a))
        |  AND t1.b > t2.b
        |""".stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi2",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5",
      "null,HeHe,Hi9")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoin(): Unit = {

    val sqlQuery = "SELECT a3, b4 FROM A, B WHERE a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoin(): Unit = {
    val ds1 = failingDataSource(tuple3Data).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(smallTuple5Data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 JOIN ds2 ON b = e"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInnerJoin2(): Unit = {
    val query = "SELECT a1, b1 FROM A JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,3", "1,1", "3,3", "2,2", "3,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val sqlQuery = "SELECT a3, b4 FROM A, B WHERE a2 = b2 AND a2 < 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithDuplicateKey(): Unit = {
    val query = "SELECT a1, b1, b3 FROM A JOIN B ON a1 = b1 AND a1 = b3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2,2", "3,3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b"

    val ds1 = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d AND b = h"

    val ds1 = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithAlias(): Unit = {
    val sqlQuery =
      "SELECT B.b5, T.`1-_./Ü` FROM (SELECT a1, a2, a3 AS `1-_./Ü` FROM A) AS T, B " +
        "WHERE a1 = b1 AND a1 < 4"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi", "2,Hello", "1,Hello",
      "2,Hello world", "2,Hello world", "3,Hello world")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDataStreamJoinWithAggregation(): Unit = {
    val sqlQuery = "SELECT COUNT(b4), COUNT(a2) FROM A, B WHERE a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("6,6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val ds1 = failingDataSource(tuple3Data).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(dataCannotBeJoin).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithRetraction(): Unit = {
    val ds1 = failingDataSource(tuple3Data).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(smallTuple5Data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamJoinWithSameRecord(): Unit = {
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

    val table1 = failingDataSource(data1).toTable(tEnv, 'pk, 'a)
    val table2 = failingDataSource(data2).toTable(tEnv, 'pk, 'a)
    tEnv.registerTable("ds1", table1)
    tEnv.registerTable("ds2", table2)

    val sql =
      """
        |SELECT
        |  ds1.pk as leftPk,
        |  ds1.a as leftA,
        |  ds2.pk as rightPk,
        |  ds2.a as rightA
        |FROM ds1 JOIN ds2 ON ds1.pk = ds2.pk
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,1,1,1", "1,1,1,1",
      "2,2,2,2", "2,2,2,2",
      "3,3,3,3", "3,3,3,3",
      "4,4,4,4", "4,4,4,4",
      "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val sqlQuery = "SELECT a3, b4 FROM A FULL OUTER JOIN B ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin2(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table5 LEFT OUTER JOIN Table3 ON b = e"

    val ds1 = failingDataSource(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("Table3", ds1)
    tEnv.registerTable("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    val sqlQuery = "SELECT a3, b4 FROM A RIGHT OUTER JOIN B ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "null,Hallo Welt wie", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,HIJ",
      "null,IJK", "null,JKL", "null,KLM")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,null", "1,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2 AND a1 > b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,null,null", "3,2,null,null", "2,2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "3,3", "2,2", "3,3", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "1,1", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("3,3", "3,3", "3,3", "2,2", "2,2", "1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,1,1", "3,2,null,null", "2,2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,3", "null,3", "null,2", "null,5", "null,3",
      "null,5", "null,4", "null,5", "null,4", "null,5", "null,4", "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,1", "null,3", "null,2", "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,3", "null,2", "null,3", "null,5", "null,5",
      "null,3", "null,5", "null,5", "null,4", "null,5", "null,4", "null,4", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2 AND a1 > b1"

    env.setParallelism(1)
    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,2,5", "null,null,5,65",
      "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,2", "3,3", "3,3", "2,2", "3,3", "null,5", "null,4", "1,1", "null,5",
      "null,4", "null,5", "null,5", "null,5", "null,4", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "2,2", "null,5", "3,3", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "null,4", "null,4", "null,4", "null,5", "null,5", "null,5",
      "null,5", "null,5", "1,1", "2,2", "3,3", "3,3", "3,3", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,5,65",
      "1,1,1,1", "null,null,2,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "3,null", "2,null", "null,3", "null,2", "null,2", "null,3",
      "null,5", "null,3", "null,5", "null,4", "null,5", "null,4", "null,1", "null,5", "null,4",
      "null,5", "null,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,5", "null,3", "null,4", "3,null", "1,null", "null,1", "2," +
      "null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,2", "null,1", "null,2", "null,5", "null,5", "null,5", "null,5",
      "null,5", "null,3", "null,3", "null,3", "null,4", "null,4", "null,4", "null,4", "3,null",
      "1,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2 AND a1 > b1"

    env.setParallelism(1)
    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,null,null", "null,null,5,65", "null,null,2,5", "2,2,null,null", "3,2," +
      "null,null", "null,null,3,15", "null,null,4,34", "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1", "null,5", "null,5", "null,5", "null,4", "null,5", "null,4", "null," +
      "5", "null,4", "null,4", "2,2", "2,2", "3,3", "3,3", "3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "1,1", "3,3", "2,2", "null,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,4", "null,4", "null,4", "null,4", "null,5", "null,5", "null,5",
      "null,5", "null,5", "3,3", "3,3", "3,3", "1,1", "2,2", "2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def FullJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("null,null,3,15", "null,null,4,34", "null,null,5,65", "3,2,null,null", "2," +
      "2,null,null", "null,null,2,5", "1,1,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullLeftOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |LEFT OUTER JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2
        |ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "4,2,null,null",
      "null,8,null,null"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullLeftOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |LEFT OUTER JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2
        |ON t1.a = t2.a OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "4,2,null,null",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullRightOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |RIGHT OUTER JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2 ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "null,null,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullRightOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |RIGHT OUTER JOIN (
        | SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2
        |ON t1.a = t2.a OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullFullOuterJoin(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
         SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |FULL OUTER JOIN (
         SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2
        |ON t1.a = t2.a
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "4,2,null,null",
      "null,8,null,null",
      "null,null,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNullFullOuterJoinWithNullCond(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long)]
    data1.+=((1, 1L))
    data1.+=((3, 8L))
    data1.+=((4, 2L))

    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, 1L))
    data2.+=((2, 2L))
    data2.+=((3, 2L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery =
      """
        |SELECT t1.a, t1.b, t2.a, t2.b
        |FROM (
         SELECT if(a = 3, cast(null as int), a) as a, b FROM T1
        |) as t1
        |FULL OUTER JOIN (
         SELECT if(a = 3, cast(null as int), a) as a, b FROM T2
        |) as t2
        |ON t1.a = t2.a
        |OR (t1.a is null AND t2.a is null)
        |""".stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,1,1",
      "null,null,2,2",
      "4,2,null,null",
      "null,8,null,2"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithoutWatermark(): Unit = {
    // NOTE: Different from AggregateITCase, we do not set stream time characteristic
    // of environment to event time, so that emitWatermark() actually does nothing.
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val data1 = new mutable.MutableList[(Int, Long)]
    data1 .+= ((1, 1L))
    data1 .+= ((2, 2L))
    data1 .+= ((3, 3L))
    val data2 = new mutable.MutableList[(Int, Long)]
    data2 .+= ((1, -1L))
    data2 .+= ((2, -2L))
    data2 .+= ((3, -3L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T1", t1)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'c)
    tEnv.registerTable("T2", t2)

    val t3 = tEnv.sqlQuery(
      "select T1.a, b, c from T1, T2 WHERE T1.a = T2.a")
    val sink = new TestingRetractSink
    t3.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("1,1,-1", "2,2,-2", "3,3,-3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testBigDataOfJoin(): Unit = {
    env.setParallelism(1)

    val data = new mutable.MutableList[(Int, Long, String)]
    for (i <- 0 until 500) {
      data.+=((i % 10, i, i.toString))
    }

    val t1 = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    val t2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f)
    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sql =
      """
        |SELECT COUNT(DISTINCT b) FROM (SELECT b FROM T1, T2 WHERE b = e)
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("500")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithUDFFilter(): Unit = {
    val ds1 = failingDataSource(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    tEnv.registerTable("T3", ds1)
    tEnv.registerTable("T5", ds2)
    tEnv.registerFunction("funcWithOpen", new FuncWithOpen)

    val sql = "SELECT c, g FROM T3 join T5 on funcWithOpen(a + d) where b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
