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

package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Tumble, Types}
import org.apache.flink.table.planner.expressions.utils.FuncWithOpen
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.utils.CountAggFunction
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Ignore, Test}

@RunWith(classOf[Parameterized])
class JoinITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  val data2 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world")
  )

  val data3 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world"),
    (4, 9L, "Having fun")
  )

  val data = List(
    (1, 1L, 0, "Hallo", 1L),
    (2, 2L, 1, "Hallo Welt", 2L),
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  val dataCannotBeJoinedByData2 = List(
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  val retractLeftData = List(
    (1, "left"),
    (2, "left")
  )

  val retractRightData = List(
    (1, "right"),
    (1, "right")
  )

  @Before
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
  }

  @Test
  def testInnerJoinOutputWithPk(): Unit = {
    // data input
    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5),
      (5, null),
      (6, null)
    )

    val data2 = List(
      (0L, 0),
      (1L, 1),
      (2L, 0),
      (2L, 1),
      (2L, 2),
      (3L, 3),
      (4L, 4),
      (5L, 4),
      (5L, 5),
      (6L, 6),
      (7L, null),
      (8L, null)
    )

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c)

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.count as 'b)

    val rightTableWithPk = rightTable
        .groupBy('bb)
        .select('bb, 'c.count as 'c)

    val t = leftTableWithPk
      .join(rightTableWithPk, 'b === 'bb)
      .select('a, 'b, 'c)

    val sink = new TestingUpsertTableSink(Array(0, 1)).configure(
      Array[String]("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.LONG))

    tEnv.registerTableSink("upsertSink", sink)
    execInsertTableAndWaitResult(t, "upsertSink")

    val expected = Seq("0,1,1", "1,2,3", "2,1,1", "3,1,1", "4,1,1", "5,2,3", "6,0,1")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }


  @Test
  def testInnerJoinOutputWithoutPk(): Unit = {
    // data input

    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5)
    )

    val data2 = List(
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 4, 4),
      (5, 5, 5),
      (5, 5, 5),
      (6, 6, 6)
    )

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c, 'd)

    val sink = new TestingRetractTableSink().configure(
      Array[String]("a", "b", "c", "d"),
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.INT, Types.INT))

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.max as 'b)

    val t = leftTableWithPk
      .join(rightTable, 'a === 'bb && ('a < 4 || 'a > 4))
      .select('a, 'b, 'c, 'd)
    tEnv.registerTableSink("retractSink", sink)
    execInsertTableAndWaitResult(t, "retractSink")

    val expected = Seq("1,1,1,1", "1,1,1,1", "1,1,1,1", "1,1,1,1", "2,2,2,2", "3,3,3,3",
                       "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithProcTimeAttributeOutput() {

    val data1 = List(
      (1L, 1, "LEFT:Hi"),
      (2L, 2, "LEFT:Hello"),
      (4L, 2, "LEFT:Hello"),
      (8L, 3, "LEFT:Hello world"),
      (16L, 3, "LEFT:Hello world"))

    val data2 = List(
      (1L, 1, "RIGHT:Hi"),
      (2L, 2, "RIGHT:Hello"),
      (4L, 2, "RIGHT:Hello"),
      (8L, 3, "RIGHT:Hello world"),
      (16L, 3, "RIGHT:Hello world"))

    val stream1 = env
      .fromCollection(data1)
    val stream2 = env
      .fromCollection(data2)

    val table1 = stream1.toTable(tEnv, 'long_l, 'int_l, 'string_l, 'proctime.proctime)
    val table2 = stream2.toTable(tEnv, 'long_r, 'int_r, 'string_r)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val table = table1
      .join(table2, 'long_l === 'long_r)
      .select('long_l as 'long, 'int_r as 'int, 'string_r as 'string, 'proctime)

    val windowedTable = table
      .window(Tumble over 5.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg, weightAvgFun('long, 'int),
              weightAvgFun('int, 'int), 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end,
              countDistinct('long))

    val sink = new TestingAppendSink
    val results = windowedTable.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    // Proctime window output uncertain results, so assert has been ignored here.
  }

  @Test
  def testInnerJoin(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val testOpenCall = new FuncWithOpen

    val joinT = ds1.join(ds2)
      .where('b === 'e)
      .where(testOpenCall('a + 'd))
      .select('c, 'g)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    val expected = Seq("Hi,Hallo")
    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithJoinFilter(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)

    val expected = Seq("Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithAggregation(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('a === 'd).select('g.count)

    val expected = Seq("6")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithGroupedAggregation(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)

    val expected = Seq("6,3", "4,2", "1,1")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinPushThroughJoin(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val ds3 = env.fromCollection(smallTupleData3).toTable(tEnv, 'j, 'k, 'l)

    val joinT = ds1.join(ds2)
      .where(true)
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)

    val expected = Seq("2,1,Hello", "2,1,Hello world", "1,0,Hi")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithDisjunctivePred(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "I am fine.,IJK")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInnerJoinWithExpressionPreds(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)

    val expected = Seq("I am fine.,Hallo Welt", "Luke Skywalker,Hallo Welt wie gehts?",
      "Luke Skywalker,ABC", "Comment#2,HIJ", "Comment#2,IJK")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 21) ? (nullOf(Types.INT), 'a) as 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
      .select(('e === 15) ? (nullOf(Types.INT), 'd) as 'd,  'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world, how are you?,null", "I am fine.,HIJ",
      "I am fine.,IJK", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null",
      "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null",
      "Comment#15,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithNonEquiJoinPred(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b <= 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world,BCD", "I am fine.,HIJ", "I am fine.,IJK",
      "Hello world, how are you?,null", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null", "Comment#7,null",
      "Comment#8,null", "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithLeftLocalPred(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 2).select('c, 'g)

    val expected = Seq(
      "Hello,Hallo Welt", "Hello,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "Hello world,BCD",
      "Hi,null", "Hello world, how are you?,null", "I am fine.,null", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null", "Comment#5,null",
      "Comment#6,null", "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null", "Comment#15,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftJoinWithRetractionInput(): Unit = {
    val ds1 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val ds2 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val leftT = ds1.groupBy('e).select('e, 'd.count as 'd)
    val rightT = ds2.groupBy('b).select('b, 'a.count as 'a)

    val joinT = leftT.leftOuterJoin(rightT, 'b === 'e).select('e, 'd, 'a)
    val expected = Seq(
      "1,1,1", "2,1,2", "3,1,3", "4,1,4", "5,1,5", "6,1,6", "7,1,null", "8,1,null", "9,1,null",
      "10,1,null", "11,1,null", "12,1,null", "13,1,null", "14,1,null", "15,1,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "null,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "null,BCD", "null,CDE",
      "null,DEF", "null,EFG", "null,FGH", "null,GHI", "I am fine.,HIJ",
      "I am fine.,IJK", "null,JKL", "null,KLM")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithNonEquiJoinPred(): Unit = {
    val ds2 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds1 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b <= 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world,BCD", "I am fine.,HIJ", "I am fine.,IJK",
      "Hello world, how are you?,null", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null", "Comment#7,null",
      "Comment#8,null", "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testRightJoinWithLeftLocalPred(): Unit = {
    val ds2 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds1 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 2).select('c, 'g)

    val expected = Seq(
      "Hello,Hallo Welt", "Hello,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "Hello world,BCD",
      "Hi,null", "Hello world, how are you?,null", "I am fine.,null", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null", "Comment#5,null",
      "Comment#6,null", "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null", "Comment#15,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.fullOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "null,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "null,BCD", "null,CDE",
      "null,DEF", "null,EFG", "null,FGH", "null,GHI", "I am fine.,HIJ",
      "I am fine.,IJK", "null,JKL", "null,KLM", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null",
      "Comment#5,null", "Comment#6,null", "Comment#7,null", "Comment#8,null",
      "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null",
      "Hello world, how are you?,null")

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithNonEquiJoinPred(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.fullOuterJoin(ds2, 'a === 'd && 'b <= 'h).select('c, 'g)

    val expected = Seq(
      // join matches
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world,BCD", "I am fine.,HIJ", "I am fine.,IJK",
      // preserved left
      "Hello world, how are you?,null", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null", "Comment#7,null",
      "Comment#8,null", "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null",
      // preserved right
      "null,Hallo Welt wie", "null,CDE", "null,DEF", "null,EFG", "null,FGH", "null,GHI", "null,JKL",
      "null,KLM")
    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testFullJoinWithLeftLocalPred(): Unit = {
    val ds1 = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.fullOuterJoin(ds2, 'a === 'd && 'b >= 2 && 'h === 1).select('c, 'g)

    val expected = Seq(
      // join matches
      "Hello,Hallo Welt wie", "Hello world, how are you?,DEF", "Hello world, how are you?,EFG",
      "I am fine.,GHI",
      // preserved left
      "Hi,null", "Hello world,null", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null", "Comment#5,null",
      "Comment#6,null", "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null", "Comment#15,null",
      // preserved right
      "null,Hallo", "null,Hallo Welt", "null,Hallo Welt wie gehts?", "null,ABC", "null,BCD",
      "null,CDE", "null,FGH", "null,HIJ", "null,IJK", "null,JKL", "null,KLM")
    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Ignore("Non-equi-join could be supported later.")
  @Test
  def testNonEqualInnerJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data3).toTable(tEnv, 'd, 'e, 'f)
    val results = ds1.join(ds2, 'a < 'd).select('a, 'd).toAppendStream[Row]

    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = Seq("1,2", "1,3", "1,4", "2,3", "2,4", "3,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Ignore("Non-equi-join could be supported later.")
  @Test
  def testNonEqualInnerJoinWithRetract(): Unit = {
    env.setParallelism(1)
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c).select('a.sum as 'a)
    val ds2 = failingDataSource(data3).toTable(tEnv, 'd, 'e, 'f).select('d.sum as 'd)
    val results = ds1.join(ds2, 'a < 'd).select('a, 'd)

    val sink = new TestingRetractSink
    results.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("6,10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinBase(): Unit = {
    val ds1 = failingDataSource(retractLeftData).toTable(tEnv, 'a, 'b)
    val ds2 = failingDataSource(retractRightData).toTable(tEnv, 'c, 'd)

    val table1 = ds2.groupBy('d).select('c.sum as 'c, 'd)

    val joined = ds1.leftOuterJoin(table1, 'a === 'c)

    val sink = new TestingRetractSink
    joined.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,left,null,null", "2,left,2,right")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'c, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithRetraction(): Unit = {
    val ds1 = failingDataSource(data3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'c, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt",
      "2,Hello,2,Hallo Welt", "9,Having fun,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testStreamJoin(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val joinT = ds1.join(ds2).where('b === 'e).select('b, 'c, 'e, 'g)

    val sink = new TestingAppendSink
    joinT.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJoinRetraction(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 2)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 2),
      (1, 4),
      (1, 5)
    )

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA < 3)
    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA > 3)
    val leftTableWithPk = leftTable
      .groupBy('leftPk)
      .select('leftPk as 'leftPk, 'leftA.max as 'leftA)

    val resultTable = leftTableWithPk
      .join(rightTable)
      .where('leftPk === 'rightPk)
      .groupBy('leftPk)
      .select('leftPk, 'leftA.count)

    val sink = new TestingRetractSink
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinKeyEqualsGroupByKey(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 2)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 2),
      (1, 4),
      (1, 5)
    )

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA > 3)
    val leftTableWithPk = leftTable
      .groupBy('leftPk)
      .select('leftPk as 'leftPk, 'leftA.max as 'leftA)

    val t = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftPk === 'rightPk)
      .select('leftPk, 'leftA, 'rightPk, 'rightA)
    val schema = t.getSchema
    val sink = new TestingUpsertTableSink(Array(0, 2))
      .configure(schema.getFieldNames, schema.getFieldTypes)
    tEnv.registerTableSink("MySink", sink)
    execInsertTableAndWaitResult(t, "MySink")

    val expected = Seq("1,5,1,2")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }


  @Test
  def testJoinKeyEqualsGroupByKeyWithRetractSink(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 2)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 2),
      (1, 4),
      (1, 5)
    )

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA > 3)
    val leftTableWithPk = leftTable
      .groupBy('leftPk)
      .select('leftPk as 'leftPk, 'leftA.max as 'leftA)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftPk === 'rightPk)

    val sink = new TestingRetractSink
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,5,1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testOneSideJoinKeyEqualsGroupByKey(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 2)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 2),
      (1, 4),
      (1, 5)
    )

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA > 3)

    val t = leftTable
      .join(rightTableWithPk)
      .where('leftPk === 'rightPk)
      .select('leftPk, 'leftA, 'rightPk, 'rightA)
    val schema = t.getSchema
    val sink = new TestingRetractTableSink().configure(
      schema.getFieldNames, schema.getFieldTypes)
    tEnv.registerTableSink("MySink", sink)
    execInsertTableAndWaitResult(t, "MySink")

    val expected = Seq("1,4,1,2", "1,5,1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }


  @Test
  def testOneSideJoinKeyEqualsGroupByKeyWithRetractSink(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 2)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 2),
      (1, 4),
      (1, 5)
    )

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA > 3)

    val resultTable = leftTable
      .join(rightTableWithPk)
      .where('leftPk === 'rightPk)

    val sink = new TestingRetractSink
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,4,1,2", "1,5,1,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinRetractionWithSameRecord(): Unit = {
    // data input
    // rightTable with (1, 1) and (1, 1)
    // leftTable with (1, 4) and (1, 5)
    val data = List(
      (1, 1),
      (1, 1),
      (1, 4),
      (1, 5))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftA < 3)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightA > 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val resultTable = rightTableWithPk
      .join(leftTable)
      .where('leftPk === 'rightPk)
      .groupBy('leftPk)
      .select('leftPk, 'leftA.count)

    val sink = new TestingRetractSink
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,2")
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
      (5, 5)
    )

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
      (10, 10)
    )

    val stream1 = failingDataSource(data1)
    val table1 = stream1.toTable(tEnv, 'pk, 'a)

    val stream2 = failingDataSource(data2)
    val table2 = stream2.toTable(tEnv, 'pk, 'a)

    val leftTable = table1.select('pk as 'leftPk, 'a as 'leftA)
    val rightTable = table2.select('pk as 'rightPk, 'a as 'rightA)

    val resultTable = rightTable
      .join(leftTable)
      .where('leftPk === 'rightPk)

    val sink = new TestingAppendSink
    resultTable.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("1,1,1,1", "1,1,1,1",
      "2,2,2,2", "2,2,2,2",
      "3,3,3,3", "3,3,3,3",
      "4,4,4,4", "4,4,4,4",
      "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testOutputWithPk(): Unit = {
    // data input
    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5),
      (5, null),
      (6, null)
    )

    val data2 = List(
      (0L, 0),
      (1L, 1),
      (2L, 0),
      (2L, 1),
      (2L, 2),
      (3L, 3),
      (4L, 4),
      (5L, 4),
      (5L, 5),
      (6L, 6),
      (7L, null),
      (8L, null)
    )

    val leftTable = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val rightTable = failingDataSource(data2).toTable(tEnv, 'bb, 'c)

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.count as 'b)

    val rightTableWithPk = rightTable
      .groupBy('bb)
      .select('bb, 'c.count as 'c)

    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))

    val t = leftTableWithPk
      .join(rightTableWithPk, 'b === 'bb)
      .select('a, 'b, 'c)
    val schema = t.getSchema
    val sink = new TestingUpsertTableSink(Array(0, 1))
      .configure(schema.getFieldNames, schema.getFieldTypes)
    tEnv.registerTableSink("MySink", sink)
    execInsertTableAndWaitResult(t, "MySink")

    val expected = Seq("0,1,1", "1,2,3", "2,1,1", "3,1,1", "4,1,1", "5,2,3", "6,0,1")
    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
  }


  @Test
  def testOutputWithoutPk(): Unit = {
    // data input

    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5)
    )

    val data2 = List(
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 4, 4),
      (5, 5, 5),
      (5, 5, 5),
      (6, 6, 6)
    )

    val leftTable = failingDataSource(data1).toTable(tEnv, 'a, 'b)
    val rightTable = failingDataSource(data2).toTable(tEnv, 'bb, 'c, 'd)

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.max as 'b)

    val result = leftTableWithPk
      .join(rightTable, 'a === 'bb && ('a < 4 || 'a > 4))
      .select('a, 'b, 'c, 'd)
      .toRetractStream[Row]

    val sink = new TestingRetractSink
    result.addSink(sink).setParallelism(1)
    env.execute()
    val expected = Seq("1,1,1,1", "1,1,1,1", "1,1,1,1", "1,1,1,1", "2,2,2,2", "3,3,3,3",
      "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithProcTimeAttributeOutput() {

    val data1 = List(
      (1L, 1, "LEFT:Hi"),
      (2L, 2, "LEFT:Hello"),
      (4L, 2, "LEFT:Hello"),
      (8L, 3, "LEFT:Hello world"),
      (16L, 3, "LEFT:Hello world"))

    val data2 = List(
      (1L, 1, "RIGHT:Hi"),
      (2L, 2, "RIGHT:Hello"),
      (4L, 2, "RIGHT:Hello"),
      (8L, 3, "RIGHT:Hello world"),
      (16L, 3, "RIGHT:Hello world"))

    val stream1 = failingDataSource(data1)
    val stream2 = failingDataSource(data2)

    val table1 = stream1.toTable(tEnv, 'long_l, 'int_l, 'string_l, 'proctime.proctime)
    val table2 = stream2.toTable(tEnv, 'long_r, 'int_r, 'string_r)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val table = table1
      .join(table2, 'long_l === 'long_r)
      .select('long_l as 'long, 'int_r as 'int, 'string_r as 'string, 'proctime)

    val windowedTable = table
      .window(Tumble over 5.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg, weightAvgFun('long, 'int),
        weightAvgFun('int, 'int), 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end,
        countDistinct('long))

    val sink = new TestingAppendSink
    windowedTable.toAppendStream[Row].addSink(sink)
    env.execute()

    // proctime window output uncertain results, so assert has been ignored here.
  }

  @Test
  def testJoin(): Unit = {
    val ds1 = failingDataSource(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val ds1 = failingDataSource(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    val ds1 = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val ds1 = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    val ds1 = failingDataSource(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('a === 'd).select('g.count)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithGroupedAggregation(): Unit = {
    val ds1 = failingDataSource(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("6,3", "4,2", "1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinPushThroughJoin(): Unit = {
    val ds1 = failingDataSource(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val ds3 = failingDataSource(smallTupleData3).toTable(tEnv, 'j, 'k, 'l)

    val joinT = ds1.join(ds2)
      .where(true)
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("2,1,Hello", "2,1,Hello world", "1,0,Hi")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithDisjunctivePred(): Unit = {
    val ds1 = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "I am fine.,IJK")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithExpressionPreds(): Unit = {
    val ds1 = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("I am fine.,Hallo Welt", "Luke Skywalker,Hallo Welt wie gehts?",
      "Luke Skywalker,ABC", "Comment#2,HIJ", "Comment#2,IJK")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinKeyNotEqualPrimaryKey(): Unit = {
    // data input
    val data = List(
      (1, 1),
      (1, 1),
      (2, 2),
      (4, 1),
      (5, 5))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightPk < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftPk > 3)
    val leftTableWithPk = leftTable
      .groupBy('leftPk)
      .select('leftPk as 'leftPk, 'leftA.max as 'leftA)

    val t = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftA === 'rightA)
      .select('leftPk, 'leftA, 'rightPk, 'rightA)

    val schema = t.getSchema
    val sink = new TestingUpsertTableSink(Array(0, 1, 2))
      .configure(schema.getFieldNames, schema.getFieldTypes)
    tEnv.registerTableSink("sinkTests", sink)
    execInsertTableAndWaitResult(t, "sinkTests")

    val expected = Seq("4,1,1,1")
    assertEquals(expected, sink.getUpsertResults)
  }


  @Test
  def testJoinKeyNotEqualPrimaryKeyWithRetractSink(): Unit = {
    // data input
    val data = List(
      (1, 1),
      (1, 1),
      (2, 1),
      (4, 1),
      (5, 5))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'pk, 'a)
    val rightTable = table
      .select('pk as 'rightPk, 'a as 'rightA)
      .where('rightPk < 3)
    val rightTableWithPk = rightTable
      .groupBy('rightPk)
      .select('rightPk, 'rightA.max as 'rightA)

    val leftTable = table
      .select('pk as 'leftPk, 'a as 'leftA)
      .where('leftPk > 3)
    val leftTableWithPk = leftTable
      .groupBy('leftPk)
      .select('leftPk as 'leftPk, 'leftA.max as 'leftA)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftA === 'rightA)

    val sink = new TestingRetractSink
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("4,1,1,1", "4,1,2,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithTwoSideJoinKeyContainPrimaryKey(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b).select('a.sum.as('a), 'b)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
      .groupBy('e).select('d.sum.as('d), 'e)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'e)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null", "2,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinContainPrimaryKey(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b).select('a.sum.as('a), 'b)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null,null", "2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinContainPrimaryKeyAndWithNonEquiJoinPred(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b).select('a.sum.as('a), 'b)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e && 'a < 'b).select('b, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,null,null", "2,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithoutPrimaryKey(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'c, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftOuterJoinWithoutPrimaryKeyAndNonEquiJoinPred(): Unit = {
    val ds1 = failingDataSource(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = failingDataSource(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e && 'a < 'd).select('a, 'b, 'c, 'e, 'g)

    val sink = new TestingRetractSink
    joinT.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1,Hi,null,null", "2,2,Hello,null,null", "3,2,Hello world,null,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
