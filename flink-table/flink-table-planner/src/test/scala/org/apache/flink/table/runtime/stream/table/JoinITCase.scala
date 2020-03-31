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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{StreamQueryConfig, Tumble, Types}
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.expressions.utils.Func20
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class JoinITCase extends StreamingWithStateTestBase {

  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c)

    tEnv.registerTableSink(
      "upsertSink",
      new TestUpsertSink(Array("a,b"), false).configure(
        Array[String]("a", "b", "c"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.LONG)))

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.count as 'b)

    val rightTableWithPk = rightTable
        .groupBy('bb)
        .select('bb, 'c.count as 'c)

    leftTableWithPk
      .join(rightTableWithPk, 'b === 'bb)
      .select('a, 'b, 'c)
      .insertInto("upsertSink", queryConfig)

    env.execute()
    val results = RowCollector.getAndClearValues
    val retracted = RowCollector.upsertResults(results, Array(0, 1))

    val expected = Seq("0,1,1", "1,2,3", "2,1,1", "3,1,1", "4,1,1", "5,2,3", "6,0,1")
    assertEquals(expected.sorted, retracted.sorted)
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c, 'd)

    tEnv.registerTableSink(
      "retractSink",
      new TestRetractSink().configure(
        Array[String]("a", "b", "c", "d"),
        Array[TypeInformation[_]](Types.INT, Types.INT, Types.INT, Types.INT)))

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.max as 'b)

    leftTableWithPk
      .join(rightTable, 'a === 'bb && ('a < 4 || 'a > 4))
      .select('a, 'b, 'c, 'd)
      .insertInto("retractSink", queryConfig)

    env.execute()
    val results = RowCollector.getAndClearValues
    val retracted = RowCollector.retractResults(results)
    val expected = Seq("1,1,1,1", "1,1,1,1", "1,1,1,1", "1,1,1,1", "2,2,2,2", "3,3,3,3",
                       "5,5,5,5", "5,5,5,5")
    assertEquals(expected.sorted, retracted.sorted)
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream1 = env
      .fromCollection(data1)
    val stream2 = env
      .fromCollection(data2)

    val table1 = stream1.toTable(tEnv, 'long_l, 'int_l, 'string_l, 'proctime_l.proctime)
    val table2 = stream2.toTable(tEnv, 'long_r, 'int_r, 'string_r)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val table = table1
      .join(table2, 'long_l === 'long_r)
      .select('long_l as 'long, 'int_r as 'int, 'string_r as 'string, 'proctime_l as 'proctime)

    val windowedTable = table
      .window(Tumble over 5.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg, weightAvgFun('long, 'int),
              weightAvgFun('int, 'int), 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end,
              countDistinct('long))

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    // Proctime window output uncertain results, so assert has been ignored here.
  }

  @Test
  def testInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val testOpenCall = new Func20

    val joinT = ds1.join(ds2)
      .where('b === 'e)
      .where(testOpenCall('a + 'd))
      .select('c, 'g)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)

    val expected = Seq("Hi,Hallo")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithJoinFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt",
      "Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)

    val expected = Seq("Hello world, how are you?,Hallo Welt wie", "I am fine.,Hallo Welt wie")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "I am fine.,HIJ", "I am fine.,IJK")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('a === 'd).select('g.count)

    val expected = Seq("6")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithGroupedAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)

    val expected = Seq("6,3", "4,2", "1,1")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinPushThroughJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val ds3 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'j, 'k, 'l)

    val joinT = ds1.join(ds2)
      .where(true)
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)

    val expected = Seq("2,1,Hello", "2,1,Hello world", "1,0,Hi")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithDisjunctivePred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)

    val expected = Seq("Hi,Hallo", "Hello,Hallo Welt", "I am fine.,IJK")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInnerJoinWithExpressionPreds(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)

    val expected = Seq("I am fine.,Hallo Welt", "Luke Skywalker,Hallo Welt wie gehts?",
      "Luke Skywalker,ABC", "Comment#2,HIJ", "Comment#2,IJK")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select(('a === 21) ? (nullOf(Types.INT), 'a) as 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
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
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftJoinWithNonEquiJoinPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b <= 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world,BCD", "I am fine.,HIJ", "I am fine.,IJK",
      "Hello world, how are you?,null", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null", "Comment#7,null",
      "Comment#8,null", "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null")

    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftJoinWithLeftLocalPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 2).select('c, 'g)

    val expected = Seq(
      "Hello,Hallo Welt", "Hello,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "Hello world,BCD",
      "Hi,null", "Hello world, how are you?,null", "I am fine.,null", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null", "Comment#5,null",
      "Comment#6,null", "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null", "Comment#15,null")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testLeftJoinWithRetractionInput(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    val ds2 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val leftT = ds1.groupBy('e).select('e, 'd.count as 'd)
    val rightT = ds2.groupBy('b).select('b, 'a.count as 'a)

    val joinT = leftT.leftOuterJoin(rightT, 'b === 'e).select('e, 'd, 'a)
    val expected = Seq(
      "1,1,1", "2,1,2", "3,1,3", "4,1,4", "5,1,5", "6,1,6", "7,1,null", "8,1,null", "9,1,null",
      "10,1,null", "11,1,null", "12,1,null", "13,1,null", "14,1,null", "15,1,null")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "null,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "null,BCD", "null,CDE",
      "null,DEF", "null,EFG", "null,FGH", "null,GHI", "I am fine.,HIJ",
      "I am fine.,IJK", "null,JKL", "null,KLM")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testRightJoinWithNonEquiJoinPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds2 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds1 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b <= 'h).select('c, 'g)

    val expected = Seq(
      "Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt wie gehts?", "Hello world,ABC",
      "Hello world,BCD", "I am fine.,HIJ", "I am fine.,IJK",
      "Hello world, how are you?,null", "Luke Skywalker,null", "Comment#1,null", "Comment#2,null",
      "Comment#3,null", "Comment#4,null", "Comment#5,null", "Comment#6,null", "Comment#7,null",
      "Comment#8,null", "Comment#9,null", "Comment#10,null", "Comment#11,null", "Comment#12,null",
      "Comment#13,null", "Comment#14,null", "Comment#15,null")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testRightJoinWithLeftLocalPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds2 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds1 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 2).select('c, 'g)

    val expected = Seq(
      "Hello,Hallo Welt", "Hello,Hallo Welt wie",
      "Hello world,Hallo Welt wie gehts?", "Hello world,ABC", "Hello world,BCD",
      "Hi,null", "Hello world, how are you?,null", "I am fine.,null", "Luke Skywalker,null",
      "Comment#1,null", "Comment#2,null", "Comment#3,null", "Comment#4,null", "Comment#5,null",
      "Comment#6,null", "Comment#7,null", "Comment#8,null", "Comment#9,null", "Comment#10,null",
      "Comment#11,null", "Comment#12,null", "Comment#13,null", "Comment#14,null", "Comment#15,null")
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

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
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testFullJoinWithNonEquiJoinPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

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
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testFullJoinWithLeftLocalPred(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

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
    val results = joinT.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }
}
