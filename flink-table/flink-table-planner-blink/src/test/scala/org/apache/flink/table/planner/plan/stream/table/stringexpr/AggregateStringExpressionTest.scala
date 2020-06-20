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

package org.apache.flink.table.planner.plan.stream.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMergeAndReset}
import org.apache.flink.table.planner.utils.{CountAggFunction, CountMinMax, TableTestBase}

import org.junit.Test

class AggregateStringExpressionTest extends TableTestBase {


  @Test
  def testDistinctNonGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1.sum.distinct, '_1.count.distinct, '_1.avg.distinct)
    val t2 = t.select("_1.sum.distinct, _1.count.distinct, _1.avg.distinct")
    val t3 = t.select("sum.distinct(_1), count.distinct(_1), avg.distinct(_1)")

    verifyTableEquals(t1, t2)
    verifyTableEquals(t1, t3)
  }

  @Test
  def testDistinctGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.groupBy('b).select('b, 'a.sum.distinct, 'a.sum)
    val t2 = t.groupBy("b").select("b, a.sum.distinct, a.sum")
    val t3 = t.groupBy("b").select("b, sum.distinct(a), sum(a)")

    verifyTableEquals(t1, t2)
    verifyTableEquals(t1, t3)
  }

  @Test
  def testDistinctNonGroupAggregateWithUDAGG(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myCnt = new CountAggFunction
   util.addFunction("myCnt", myCnt)
    val myWeightedAvg = new WeightedAvgWithMergeAndReset
   util.addFunction("myWeightedAvg", myWeightedAvg)

    val t1 = t.select(myCnt.distinct('a) as 'aCnt, myWeightedAvg.distinct('b, 'a) as 'wAvg)
    val t2 = t.select("myCnt.distinct(a) as aCnt, myWeightedAvg.distinct(b, a) as wAvg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testDistinctGroupedAggregateWithUDAGG(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)


    val myCnt = new CountAggFunction
   util.addFunction("myCnt", myCnt)
    val myWeightedAvg = new WeightedAvgWithMergeAndReset
   util.addFunction("myWeightedAvg", myWeightedAvg)

    val t1 = t.groupBy('b)
      .select('b,
        myCnt.distinct('a) + 9 as 'aCnt,
        myWeightedAvg.distinct('b, 'a) * 2 as 'wAvg,
        myWeightedAvg.distinct('a, 'a) as 'distAgg,
        myWeightedAvg('a, 'a) as 'agg)
    val t2 = t.groupBy("b")
      .select("b, myCnt.distinct(a) + 9 as aCnt, myWeightedAvg.distinct(b, a) * 2 as wAvg, " +
        "myWeightedAvg.distinct(a, a) as distAgg, myWeightedAvg(a, a) as agg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val weightAvgFun = new WeightedAvg
   util.addFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .groupBy('string)
      .select('int.count as 'cnt, weightAvgFun('long, 'int))

    // String / Java API
    val resJava = t
      .groupBy("string")
      .select("int.count as cnt, weightAvgFun(long, int)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testNonGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    // Expression / Scala API
    val resScala = t.select('int.count as 'cnt, 'long.sum)

    // String / Java API
    val resJava = t.select("int.count as cnt, long.sum")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testProctimeRename(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)](
      "T1", 'int, 'long, 'string, 'proctime.proctime)

    // Expression / Scala API
    val resScala = t
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'w1.start as 'start, 'w1.end as 'end, 'string, 'int.count)

    // String / Java API
    val resJava = t
      .window(Tumble.over("50.milli").on("proctime").as("w1"))
      .groupBy("w1, string")
      .select("w1.proctime as proctime, w1.start as start, w1.end as end, string, int.count")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testRowtimeRename(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[TestPojo](
      "T1",'int, 'rowtime.rowtime, 'string)

    // Expression / Scala API
    val resScala = t
      .window(Tumble over 50.milli on 'rowtime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.rowtime as 'rowtime, 'string, 'int.count)

    // String / Java API
    val resJava = t
      .window(Tumble.over("50.milli").on("rowtime").as("w1"))
      .groupBy("w1, string")
      .select("w1.rowtime as rowtime, string, int.count")

    verifyTableEquals(resJava, resScala)
  }

  def testNonGroupedRowBasedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val testAgg = new CountMinMax
   util.addFunction("testAgg", testAgg)

    // Expression / Scala API
    val resScala = t
      .aggregate(testAgg('a))
      .select('f0, 'f1)

    // String / Java API
    val resJava = t
      .aggregate("testAgg(a)")
      .select("f0, f1")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testGroupedRowBasedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val testAgg = new CountMinMax
   util.addFunction("testAgg", testAgg)

    // Expression / Scala API
    val resScala = t
      .groupBy('b)
      .aggregate(testAgg('a))
      .select('b, 'f0, 'f1)

    // String / Java API
    val resJava = t
      .groupBy("b")
      .aggregate("testAgg(a)")
      .select("b, f0, f1")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testAggregateWithAlias(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val testAgg = new CountMinMax
   util.addFunction("testAgg", testAgg)

    // Expression / Scala API
    val resScala = t
      .groupBy('b)
      .aggregate(testAgg('a) as ('x, 'y, 'z))
      .select('b, 'x, 'y)

    // String / Java API
    val resJava = t
      .groupBy("b")
      .aggregate("testAgg(a) as (x, y, z)")
      .select("b, x, y")

    verifyTableEquals(resScala, resJava)
  }
}

class TestPojo() {
  var int: Int = _
  var long: Long = _
  var string: String = _
}
