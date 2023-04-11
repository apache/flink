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
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.{mutable, Seq}

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
    tEnv.createTemporaryView("A", tableA)
    tEnv.createTemporaryView("B", tableB)

    val dataId1 = TestValuesTableFactory.registerData(TestData.data2_1)
    tEnv.executeSql(s"""
                       |create table l (
                       |  a int,
                       |  b double
                       |) with (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId1',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
    val dataId2 = TestValuesTableFactory.registerData(TestData.data2_2)
    tEnv.executeSql(s"""
                       |create table r (
                       |  c int,
                       |  d double
                       |) with (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId2',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
  }

  // Tests for inner join.
  override def after(): Unit = {}

  @Test
  def testDependentConditionDerivationInnerJoin(): Unit = {
    val sqlQuery = "SELECT * FROM A, B WHERE (a2 = 1 and b2 = 2) or (a1 = 2 and b1 = 4)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.Seq(
      "1,1,Hi,2,2,1,Hallo Welt,2",
      "2,2,Hello,4,10,9,FGH,2",
      "2,2,Hello,4,7,6,CDE,2",
      "2,2,Hello,4,8,7,DEF,1",
      "2,2,Hello,4,9,8,EFG,1")

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDependentConditionDerivationInnerJoinWithTrue(): Unit = {
    val sqlQuery = "SELECT * FROM A, B WHERE (a2 = 1 AND true) OR (a1 = 2 AND b1 = 4) "

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable
      .MutableList(
        "1,1,Hi,1,1,0,Hallo,1",
        "1,1,Hi,2,2,1,Hallo Welt,2",
        "1,1,Hi,2,3,2,Hallo Welt wie,1",
        "1,1,Hi,3,4,3,Hallo Welt wie gehts?,2",
        "1,1,Hi,3,5,4,ABC,2",
        "1,1,Hi,3,6,5,BCD,3",
        "1,1,Hi,4,10,9,FGH,2",
        "1,1,Hi,4,7,6,CDE,2",
        "1,1,Hi,4,8,7,DEF,1",
        "1,1,Hi,4,9,8,EFG,1",
        "1,1,Hi,5,11,10,GHI,1",
        "1,1,Hi,5,12,11,HIJ,3",
        "1,1,Hi,5,13,12,IJK,3",
        "1,1,Hi,5,14,13,JKL,2",
        "1,1,Hi,5,15,14,KLM,2",
        "2,2,Hello,4,10,9,FGH,2",
        "2,2,Hello,4,7,6,CDE,2",
        "2,2,Hello,4,8,7,DEF,1",
        "2,2,Hello,4,9,8,EFG,1"
      )
      .toList

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDependentConditionDerivationInnerJoinWithNull(): Unit = {
    val data1 = List(
      (0, 1, "hi a1"),
      (1, 2, "hi a2"),
      (2, 3, "hi a3")
    )

    val data2 = List(
      (3, 4, "hi b1"),
      (4, 5, null),
      (5, 6, "hi b3")
    )

    val table1 = failingDataSource(data1).toTable(tEnv, 'a1, 'a2, 'a3)
    val table2 = failingDataSource(data2).toTable(tEnv, 'b1, 'b2, 'b3)
    tEnv.createTemporaryView("a", table1)
    tEnv.createTemporaryView("b", table2)

    val sqlQuery = "SELECT * FROM a, b WHERE (a1 = 1 AND b1 = 3) OR (a1 = 2 AND b3 is null) "

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,2,hi a2,3,4,hi b1", "2,3,hi a3,4,5,null").toList

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  /** test non-window inner join * */
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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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
    tEnv.createTemporaryView("ds1", ds1)
    tEnv.createTemporaryView("ds2", ds2)
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
  def testInnerJoinWithBooleanFilterCondition(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String, Boolean)]
    data1.+=((1, 1L, "Hi", true))
    data1.+=((2, 2L, "Hello", false))
    data1.+=((3, 2L, "Hello world", true))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a1, 'b1, 'c1, 'd1)
    val t2 = failingDataSource(data1).toTable(tEnv, 'a2, 'b2, 'c2, 'd2)
    tEnv.createTemporaryView("Table3", t1)
    tEnv.createTemporaryView("Table5", t2)

    val sqlQuery = "SELECT a1, a1, c2 FROM Table3 INNER JOIN Table5 ON d1 = d2 where d1 is true"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,Hello world", "1,1,Hi", "3,3,Hello world", "3,3,Hi")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE b = e AND a < 6 AND h < b"

    val ds1 = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.createTemporaryView("Table3", ds1)
    tEnv.createTemporaryView("Table5", ds2)

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
    tEnv.createTemporaryView("Table3", ds1)
    tEnv.createTemporaryView("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo",
      "Hello,Hallo Welt",
      "Hello world,Hallo Welt wie gehts?",
      "Hello world,ABC",
      "I am fine.,HIJ",
      "I am fine.,IJK")
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

    val expected =
      Seq("1,Hi", "2,Hello", "1,Hello", "2,Hello world", "2,Hello world", "3,Hello world")
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
    tEnv.createTemporaryView("ds1", ds1)
    tEnv.createTemporaryView("ds2", ds2)
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
    tEnv.createTemporaryView("ds1", ds1)
    tEnv.createTemporaryView("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamJoinWithSameRecord(): Unit = {
    val data1 = List((1, 1), (1, 1), (2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4), (5, 5), (5, 5))

    val data2 =
      List((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10))

    val table1 = failingDataSource(data1).toTable(tEnv, 'pk, 'a)
    val table2 = failingDataSource(data2).toTable(tEnv, 'pk, 'a)
    tEnv.createTemporaryView("ds1", table1)
    tEnv.createTemporaryView("ds2", table2)

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

    val expected = Seq(
      "1,1,1,1",
      "1,1,1,1",
      "2,2,2,2",
      "2,2,2,2",
      "3,3,3,3",
      "3,3,3,3",
      "4,4,4,4",
      "4,4,4,4",
      "5,5,5,5",
      "5,5,5,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val sqlQuery = "SELECT a3, b4 FROM A FULL OUTER JOIN B ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo",
      "Hello,Hallo Welt",
      "Hello world,Hallo Welt",
      "null,Hallo Welt wie",
      "null,Hallo Welt wie gehts?",
      "null,ABC",
      "null,BCD",
      "null,CDE",
      "null,DEF",
      "null,EFG",
      "null,FGH",
      "null,GHI",
      "null,HIJ",
      "null,IJK",
      "null,JKL",
      "null,KLM"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin2(): Unit = {
    val sqlQuery = "SELECT c, g FROM Table5 LEFT OUTER JOIN Table3 ON b = e"

    val ds1 = failingDataSource(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.createTemporaryView("Table3", ds1)
    tEnv.createTemporaryView("Table5", ds2)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo",
      "Hello,Hallo Welt",
      "Hello world,Hallo Welt",
      "null,Hallo Welt wie",
      "null,Hallo Welt wie gehts?",
      "null,ABC",
      "null,BCD",
      "null,CDE",
      "null,DEF",
      "null,EFG",
      "null,FGH",
      "null,GHI",
      "null,HIJ",
      "null,IJK",
      "null,JKL",
      "null,KLM"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    val sqlQuery = "SELECT a3, b4 FROM A RIGHT OUTER JOIN B ON a2 = b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo",
      "Hello,Hallo Welt",
      "Hello world,Hallo Welt",
      "null,Hallo Welt wie",
      "null,Hallo Welt wie gehts?",
      "null,ABC",
      "null,BCD",
      "null,CDE",
      "null,DEF",
      "null,EFG",
      "null,FGH",
      "null,GHI",
      "null,HIJ",
      "null,IJK",
      "null,JKL",
      "null,KLM"
    )
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

    val expected = Seq(
      "null,2",
      "null,1",
      "null,3",
      "null,3",
      "null,2",
      "null,5",
      "null,3",
      "null,5",
      "null,4",
      "null,5",
      "null,4",
      "null,5",
      "null,4",
      "null,5",
      "null,4")
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

    val expected = Seq(
      "null,2",
      "null,1",
      "null,3",
      "null,2",
      "null,3",
      "null,5",
      "null,5",
      "null,3",
      "null,5",
      "null,5",
      "null,4",
      "null,5",
      "null,4",
      "null,4",
      "null,4")
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

    val expected =
      Seq("null,null,3,15", "null,null,4,34", "null,null,2,5", "null,null,5,65", "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "2,2",
      "3,3",
      "3,3",
      "2,2",
      "3,3",
      "null,5",
      "null,4",
      "1,1",
      "null,5",
      "null,4",
      "null,5",
      "null,5",
      "null,5",
      "null,4",
      "null,4")
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

    val expected = Seq(
      "null,4",
      "null,4",
      "null,4",
      "null,4",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "1,1",
      "2,2",
      "3,3",
      "3,3",
      "3,3",
      "2,2")
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

    val expected =
      Seq("null,null,3,15", "null,null,4,34", "null,null,5,65", "1,1,1,1", "null,null,2,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,null",
      "3,null",
      "2,null",
      "null,3",
      "null,2",
      "null,2",
      "null,3",
      "null,5",
      "null,3",
      "null,5",
      "null,4",
      "null,5",
      "null,4",
      "null,1",
      "null,5",
      "null,4",
      "null,5",
      "null,4"
    )
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

    val expected = Seq(
      "null,2",
      "null,5",
      "null,3",
      "null,4",
      "3,null",
      "1,null",
      "null,1",
      "2," +
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

    val expected = Seq(
      "null,2",
      "null,1",
      "null,2",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "null,3",
      "null,3",
      "null,3",
      "null,4",
      "null,4",
      "null,4",
      "null,4",
      "3,null",
      "1,null",
      "2,null"
    )
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

    val expected = Seq(
      "1,1,null,null",
      "null,null,5,65",
      "null,null,2,5",
      "2,2,null,null",
      "3,2," +
        "null,null",
      "null,null,3,15",
      "null,null,4,34",
      "null,null,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(query).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,1",
      "null,5",
      "null,5",
      "null,5",
      "null,4",
      "null,5",
      "null,4",
      "null," +
        "5",
      "null,4",
      "null,4",
      "2,2",
      "2,2",
      "3,3",
      "3,3",
      "3,3")
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

    val expected = Seq(
      "null,4",
      "null,4",
      "null,4",
      "null,4",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "null,5",
      "3,3",
      "3,3",
      "3,3",
      "1,1",
      "2,2",
      "2,2")
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

    val expected = Seq(
      "null,null,3,15",
      "null,null,4,34",
      "null,null,5,65",
      "3,2,null,null",
      "2," +
        "2,null,null",
      "null,null,2,5",
      "1,1,1,1")
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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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
    data1.+=((1, 1L))
    data1.+=((2, 2L))
    data1.+=((3, 3L))
    val data2 = new mutable.MutableList[(Int, Long)]
    data2.+=((1, -1L))
    data2.+=((2, -2L))
    data2.+=((3, -3L))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T1", t1)
    val t2 = failingDataSource(data2).toTable(tEnv, 'a, 'c)
    tEnv.createTemporaryView("T2", t2)

    val t3 = tEnv.sqlQuery("select T1.a, b, c from T1, T2 WHERE T1.a = T2.a")
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
    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

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

    tEnv.createTemporaryView("T3", ds1)
    tEnv.createTemporaryView("T5", ds2)
    tEnv.registerFunction("funcWithOpen", new FuncWithOpen)

    val sql = "SELECT c, g FROM T3 join T5 on funcWithOpen(a + d) where b = e"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithFilterPushDown(): Unit = {
    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where c >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 where c >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1))
    )

    // For left join, we will push c = 3 into left side l by
    // derived from a = c and c = 3.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c = 3
        |""".stripMargin,
      Seq(
        row(3, 3.0, 3, 2.0)
      )
    )

    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // c IS NULL cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c IS NULL
        |""".stripMargin,
      Seq(
        row(1, 2.0, null, null),
        row(1, 2.0, null, null),
        row(null, 5.0, null, null),
        row(null, null, null, null)
      )
    )

    checkResult(
      """
        |select * from
        | l left join r on a = c where c IS NULL AND a <= 1
        |""".stripMargin,
      Seq(
        row(1, 2.0, null, null),
        row(1, 2.0, null, null)
      )
    )

    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // c < 3 cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c < 3 AND a <= 3
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0)
      )
    )

    // C <> 3 cannot be push into left side.
    checkResult(
      """
        |select * from
        | l left join r on a = c where c <> 3 AND a <= 3
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0)
      )
    )
  }

  @Test
  def testJoinWithJoinConditionPushDown(): Unit = {
    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(row(2, 1.0, 2, 2, 3.0, 2), row(3, 3.0, 1, 3, 2.0, 1), row(6, null, 1, 6, null, 1))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(
        row(1, 2.0, 2, null, null, null),
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, 5.0, 2, null, null, null))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  left join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and c >= 2
        |""".stripMargin,
      Seq(
        row(1, 2.0, 2, null, null, null),
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, 5.0, 2, null, null, null))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and a >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1),
        row(null, null, null, null, 5.0, 2))
    )

    checkResult(
      """
        |select * from
        |  (select a, max(b) b, count(*) c1 from l group by a)
        |  right join
        |  (select c, max(d) d, count(*) c2 from r group by c)
        |  on a = c and c1 = c2 and c >= 2
        |""".stripMargin,
      Seq(
        row(2, 1.0, 2, 2, 3.0, 2),
        row(3, 3.0, 1, 3, 2.0, 1),
        row(6, null, 1, 6, null, 1),
        row(null, null, null, 4, 1.0, 1),
        row(null, null, null, null, 5.0, 2))
    )
  }

  private def checkResult(sql: String, expected: Seq[Row]): Unit = {
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expectedResult = expected
      .map(
        r => {
          (0 until r.getArity).map(i => r.getField(i)).mkString(",")
        })
      .sorted
    assertEquals(expectedResult, sink.getRetractResults.sorted)
  }
}
