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

package org.apache.flink.table.planner.plan.batch.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.planner.utils.{CountAggFunction, TableTestBase}

import org.junit._

class AggregateStringExpressionTest extends TableTestBase {

  @Test
  def testDistinctAggregationTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1.sum.distinct, '_1.count.distinct, '_1.avg.distinct)
    val t2 = t.select("_1.sum.distinct, _1.count.distinct, _1.avg.distinct")
    val t3 = t.select("sum.distinct(_1), count.distinct(_1), avg.distinct(_1)")

    verifyTableEquals(t1, t2)
    verifyTableEquals(t1, t3)
  }

  @Test
  def testAggregationTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1.sum, '_1.sum0, '_1.min, '_1.max, '_1.count, '_1.avg)
    val t2 = t.select("_1.sum, _1.sum0, _1.min, _1.max, _1.count, _1.avg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testWorkingAggregationDataTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Byte, Short, Int, Long, Float, Double, String)]("Table7")

    val t1 = t.select('_1.avg, '_2.avg, '_3.avg, '_4.avg, '_5.avg, '_6.avg, '_7.count, '_7.collect)
    val t2 = t.select("_1.avg, _2.avg, _3.avg, _4.avg, _5.avg, _6.avg, _7.count, _7.collect")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testProjection(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Byte, Short)]("Table2")

    val t1 = t.select('_1.avg, '_1.sum, '_1.count, '_2.avg, '_2.sum)
    val t2 = t.select("_1.avg, _1.sum, _1.count, _2.avg, _2.sum")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAggregationWithArithmetic(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Long, String)]("Table2")

    val t1 = t.select(('_1 + 2).avg + 2, '_2.count + 5)
    val t2 = t.select("(_1 + 2).avg + 2, _2.count + 5")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAggregationWithTwoCount(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Long, String)]("Table2")

    val t1 = t.select('_1.count, '_2.count)
    val t2 = t.select("_1.count, _2.count")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAggregationAfterProjection(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Byte, Short, Int, Long, Float, Double, String)]("Table7")

    val t1 = t.select('_1, '_2, '_3)
      .select('_1.avg, '_2.sum, '_3.count)

    val t2 = t.select("_1, _2, _3")
      .select("_1.avg, _2.sum, _3.count")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testDistinct(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val distinct = ds.select('b).distinct()
    val distinct2 = ds.select("b").distinct()

    verifyTableEquals(distinct, distinct2)
  }

  @Test
  def testDistinctAfterAggregate(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'a, 'b, 'c, 'd, 'e)

    val distinct = ds.groupBy('a, 'e).select('e).distinct()
    val distinct2 = ds.groupBy("a, e").select("e").distinct()

    verifyTableEquals(distinct, distinct2)
  }

  @Test
  def testDistinctGroupedAggregate(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.groupBy('b).select('b, 'a.sum.distinct, 'a.sum)
    val t2 = t.groupBy("b").select("b, a.sum.distinct, a.sum")
    val t3 = t.groupBy("b").select("b, sum.distinct(a), sum(a)")

    verifyTableEquals(t1, t2)
    verifyTableEquals(t1, t3)
  }

  @Test
  def testGroupedAggregate(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.groupBy('b).select('b, 'a.sum)
    val t2 = t.groupBy("b").select("b, a.sum")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupingKeyForwardIfNotUsed(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.groupBy('b).select('a.sum)
    val t2 = t.groupBy("b").select("a.sum")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupNoAggregation(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)

    val t2 = t
      .groupBy("b")
      .select("a.sum as d, b")
      .groupBy("b, d")
      .select("b")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregateWithConstant1(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val t2 = t.select("a, 4 as four, b")
      .groupBy("four, a")
      .select("four, b.sum")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregateWithConstant2(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)
    val t2 = t.select("b, 4 as four, a")
      .groupBy("b, four")
      .select("four, a.sum")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregateWithExpression(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'a, 'b, 'c, 'd, 'e)

    val t1 = t.groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)
    val t2 = t.groupBy("e, b % 3")
      .select("c.min, e, a.avg, d.count")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregateWithFilter(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val t1 = t.groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)
    val t2 = t.groupBy("b")
      .select("b, a.sum")
      .where("b = 2")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAnalyticAggregation(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, Float, Double)]('_1, '_2, '_3, '_4)

    val resScala = t.select(
      '_1.stddevPop, '_2.stddevPop, '_3.stddevPop, '_4.stddevPop,
      '_1.stddevSamp, '_2.stddevSamp, '_3.stddevSamp, '_4.stddevSamp,
      '_1.varPop, '_2.varPop, '_3.varPop, '_4.varPop,
      '_1.varSamp, '_2.varSamp, '_3.varSamp, '_4.varSamp)
    val resJava = t.select("""
      _1.stddevPop, _2.stddevPop, _3.stddevPop, _4.stddevPop,
      _1.stddevSamp, _2.stddevSamp, _3.stddevSamp, _4.stddevSamp,
      _1.varPop, _2.varPop, _3.varPop, _4.varPop,
      _1.varSamp, _2.varSamp, _3.varSamp, _4.varSamp""")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testDistinctAggregateWithUDAGG(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myCnt = new CountAggFunction
    util.addFunction("myCnt", myCnt)
    util.addTemporarySystemFunction("myWeightedAvg", classOf[WeightedAvgWithMergeAndReset])

    val t1 = t.select(
      myCnt.distinct('a) as 'aCnt,
      call("myWeightedAvg", 'b, 'a).distinct() as 'wAvg
    )
    val t2 = t.select("myCnt.distinct(a) as aCnt, myWeightedAvg.distinct(b, a) as wAvg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAggregateWithUDAGG(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    val myCnt = new CountAggFunction
    util.addFunction("myCnt", myCnt)
    util.addTemporarySystemFunction("myWeightedAvg", classOf[WeightedAvgWithMergeAndReset])

    val t1 = t.select(myCnt('a) as 'aCnt, call("myWeightedAvg", 'b, 'a) as 'wAvg)
    val t2 = t.select("myCnt(a) as aCnt, myWeightedAvg(b, a) as wAvg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testDistinctGroupedAggregateWithUDAGG(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)


    val myCnt = new CountAggFunction
    util.addFunction("myCnt", myCnt)
    util.addTemporarySystemFunction("myWeightedAvg", classOf[WeightedAvgWithMergeAndReset])

    val t1 = t.groupBy('b)
      .select('b,
        myCnt.distinct('a) + 9 as 'aCnt,
        call("myWeightedAvg", 'b, 'a).distinct() * 2 as 'wAvg,
        call("myWeightedAvg", 'a, 'a).distinct() as 'distAgg,
        call("myWeightedAvg", 'a, 'a) as 'agg)
    val t2 = t.groupBy("b")
      .select("b, myCnt.distinct(a) + 9 as aCnt, myWeightedAvg.distinct(b, a) * 2 as wAvg, " +
        "myWeightedAvg.distinct(a, a) as distAgg, myWeightedAvg(a, a) as agg")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testGroupedAggregateWithUDAGG(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)


    val myCnt = new CountAggFunction
   util.addFunction("myCnt", myCnt)
   util.addTemporarySystemFunction("myWeightedAvg", classOf[WeightedAvgWithMergeAndReset])

    val t1 = t.groupBy('b)
      .select(
        'b,
        myCnt('a) + 9 as 'aCnt,
        call("myWeightedAvg", 'b, 'a) * 2 as 'wAvg,
        call("myWeightedAvg", 'a, 'a))
    val t2 = t.groupBy("b")
      .select("b, myCnt(a) + 9 as aCnt, myWeightedAvg(b, a) * 2 as wAvg, myWeightedAvg(a, a)")

    verifyTableEquals(t1, t2)
  }
}
