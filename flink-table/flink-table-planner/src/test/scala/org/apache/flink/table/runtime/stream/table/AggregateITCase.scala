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
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, DataViewTestAgg, WeightedAvg}
import org.apache.flink.table.runtime.utils.StreamITCase.RetractingSink
import org.apache.flink.table.runtime.utils.{JavaUserDefinedAggFunctions, StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.utils.CountMinMax
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests of groupby (without window) aggregations
  */
class AggregateITCase extends StreamingWithStateTestBase {
  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

  @Test
  def testDistinctUDAGG(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val testAgg = new DataViewTestAgg
    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e)
      .select('e, testAgg.distinct('d, 'e))

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,10", "2,21", "3,12")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctUDAGGMixedWithNonDistinctUsage(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val testAgg = new WeightedAvg
    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e)
      .select('e, testAgg.distinct('a, 'a), testAgg('a, 'a))

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,3,3", "2,3,4", "3,4,4")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, Int, String)]
    data.+=((1, 1, "A"))
    data.+=((2, 2, "B"))
    data.+=((2, 2, "B"))
    data.+=((4, 3, "C"))
    data.+=((5, 3, "C"))
    data.+=((4, 3, "C"))
    data.+=((7, 3, "B"))
    data.+=((1, 4, "A"))
    data.+=((9, 4, "D"))
    data.+=((4, 1, "A"))
    data.+=((3, 2, "B"))

    val testAgg = new WeightedAvg
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('c)
      .select('c, 'a.count.distinct, 'a.sum.distinct,
              testAgg.distinct('a, 'b), testAgg.distinct('b, 'a), testAgg('a, 'b))

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("A,2,5,1,1,1", "B,3,12,4,2,3", "C,2,9,4,3,4", "D,1,9,9,4,9")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctAggregateMixedWithNonDistinct(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e)
      .select('e, 'a.count.distinct, 'b.count)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,4,5", "2,4,7", "3,2,3")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinct(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('b, nullOf(Types.LONG)).distinct()

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,null", "2,null", "3,null", "4,null", "5,null", "6,null")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctAfterAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e).select('e, 'a.count).distinct()

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,5", "2,7", "3,3")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testNonKeyedGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
            .select('a.sum, 'b.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("231,91")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1", "2,5", "3,15", "4,34", "5,65", "6,111")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDoubleGroupAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.count as 'cnt, 'b)
      .groupBy('cnt)
      .select('cnt, 'b.count as 'freq, 'b.min as 'min, 'b.max as 'max)

    val results = t.toRetractStream[Row](queryConfig)

    results.addSink(new RetractingSink)
    env.execute()
    val expected = List("1,1,1,1", "2,1,2,2", "3,1,3,3", "4,1,4,4", "5,1,5,5", "6,1,6,6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new RetractingSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1", "7,1,4,2", "2,1,3,2",
      "3,2,3,3", "1,2,3,3", "14,2,5,1",
      "12,3,5,1", "5,3,4,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testCollect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.collect)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new RetractingSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,{1=1}", "2,{2=1, 3=1}", "3,{4=1, 5=1, 6=1}", "4,{7=1, 8=1, 9=1, 10=1}",
      "5,{11=1, 12=1, 13=1, 14=1, 15=1}", "6,{16=1, 17=1, 18=1, 19=1, 20=1, 21=1}")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregateWithStateBackend(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val distinct = new CountDistinct
    val testAgg = new DataViewTestAgg
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, distinct('c), testAgg('c, 'b))

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1,2", "2,1,5", "3,1,10", "4,4,20", "5,2,12")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)

    // verify agg close is called
    assert(JavaUserDefinedAggFunctions.isCloseCalled)
  }

  @Test
  def testRemoveDuplicateRecordsWithUpsertSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))

    tEnv.registerTableSink(
      "testSink",
      new TestUpsertSink(Array("c"), false).configure(
        Array[String]("c", "bMax"), Array[TypeInformation[_]](Types.STRING, Types.LONG)))

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
            .groupBy('c)
            .select('c, 'b.max)

    t.insertInto("testSink")
    env.execute()

    val expected = List("(true,A,1)", "(true,B,2)", "(true,C,3)")
    assertEquals(expected.sorted, RowCollector.getAndClearValues.map(_.toString).sorted)
  }

  @Test
  def testNonGroupedAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val testAgg = new CountMinMax
    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .aggregate(testAgg('a))
      .select('f0, 'f1, 'f2)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("21,1,21")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val testAgg = new CountMinMax
    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .aggregate(testAgg('a))
      .select('b, 'f0, 'f1, 'f2)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1,1,1", "2,2,2,3", "3,3,4,6", "4,4,7,10", "5,5,11,15", "6,6,16,21")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }
}
