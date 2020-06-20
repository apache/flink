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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{CountDistinctWithMergeAndReset, WeightedAvgWithMergeAndReset}
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.table.planner.utils.{CountAggFunction, NonMergableCount}
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import java.lang.{Float => JFloat, Integer => JInt}
import java.math.BigDecimal

import scala.collection.JavaConverters._
import scala.collection.mutable

class AggregationITCase extends BatchTestBase {

  @Test
  def testAggregationWithCaseClass(): Unit = {
    val inputTable = CollectionBatchExecTable.getSmallNestedTupleDataSet(tEnv, "a, b")

    val result = inputTable
      .where('a.get("_1") > 0)
      .select('a.get("_1").avg, 'a.get("_2").sum, 'b.count)

    val expected = "2,6,3"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationTypes(): Unit = {

    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, null)
      .select('_1.sum, '_1.sum0, '_1.min, '_1.max, '_1.count, '_1.avg)

    val results = executeQuery(t)
    val expected = "231,231,1,21,21,11"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testWorkingAggregationDataTypes(): Unit = {

    val t = BatchTableEnvUtil.fromElements(tEnv,
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao"))
      .select('_1.avg, '_2.avg, '_3.avg, '_4.avg, '_5.avg, '_6.avg, '_7.count)

    val expected = "1,1,1,1,1.5,1.5,2"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjection(): Unit = {
    val t = BatchTableEnvUtil.fromElements(tEnv,
      (1: Byte, 1: Short),
      (2: Byte, 2: Short))
      .select('_1.avg, '_1.sum, '_1.count, '_2.avg, '_2.sum)

    val expected = "1,3,2,1,3"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithArithmetic(): Unit = {
    val t = BatchTableEnvUtil.fromElements(tEnv, (1f, "Hello"), (2f, "Ciao"))
      .select(('_1 + 2).avg + 2, '_2.count + 5)

    val expected = "5.5,7"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithTwoCount(): Unit = {
    val t = BatchTableEnvUtil.fromElements(tEnv, (1f, "Hello"), (2f, "Ciao"))
      .select('_1.count, '_2.count)

    val expected = "2,2"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationAfterProjection(): Unit = {
    val t = BatchTableEnvUtil.fromElements(tEnv,
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao"))
      .select('_1, '_2, '_3)
      .select('_1.avg, '_2.sum, '_3.count)

    val expected = "1,3,2"
    val result = executeQuery(t)
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSQLStyleAggregations(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b ,c")
      .select(
        """Sum( a) as a1, a.sum as a2,
          |Min (a) as b1, a.min as b2,
          |Max (a ) as c1, a.max as c2,
          |Avg ( a ) as d1, a.avg as d2,
          |Count(a) as e1, a.count as e2
        """.stripMargin)

    val expected = "231,231,1,1,21,21,11,11,21,21"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPojoAggregation(): Unit = {
    val input = BatchTableEnvUtil.fromElements(tEnv,
      WC("hello", 1),
      WC("hello", 1),
      WC("ciao", 1),
      WC("hola", 1),
      WC("hola", 1))
    val expr = input
    val result = executeQuery(expr
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
      .select('word, 'frequency * 10))

    val mappedResult = result.map((row) => (row.getField(0), row.getField(1)))
    val expected = "(hello,20)\n" + "(hola,20)"
    TestBaseUtils.compareResultAsText(mappedResult.asJava, expected)
  }

  @Test
  def testDistinct(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val distinct = ds.select('b).distinct()

    val expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
    val results = executeQuery(distinct)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDistinctAfterAggregate(): Unit = {
    val ds = CollectionBatchExecTable.get5TupleDataSet(tEnv, "a, b, c, d, e")
    val distinct = ds.groupBy('a, 'e).select('e).distinct()

    val expected = "1\n" + "2\n" + "3\n"
    val results = executeQuery(distinct)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregate(): Unit = {
    val countFun = new CountAggFunction
    val wAvgFun = new WeightedAvgWithMergeAndReset
    val countDistinct = new CountDistinctWithMergeAndReset

    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('b, 'a.sum, countFun('c), wAvgFun('b, 'a), wAvgFun('a, 'a), countDistinct('c))

    val expected = "1,1,1,1,1,1\n" + "2,5,2,2,2,2\n" + "3,15,3,3,5,3\n" + "4,34,4,4,8,4\n" +
      "5,65,5,5,13,5\n" + "6,111,6,6,18,6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupingKeyForwardIfNotUsed(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('a.sum)

    val expected = "1\n" + "5\n" + "15\n" + "34\n" + "65\n" + "111\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupNoAggregation(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)

    val expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testAggregateEmptyDataSets(): Unit = {
    val myAgg = new NonMergableCount

    val t1 = BatchTableEnvUtil.fromCollection(
      tEnv, new mutable.MutableList[(Int, String)], "a, b")
      .select('a.sum, 'a.count)
    val t2 = BatchTableEnvUtil.fromCollection(
      tEnv, new mutable.MutableList[(Int, String)], "a, b")
      .select('a.sum, myAgg('b), 'a.count)

    val expected1 = "null,0"
    val expected2 = "null,0,0"

    val results1 = executeQuery(t1)
    val results2 = executeQuery(t2)

    TestBaseUtils.compareResultAsText(results1.asJava, expected1)
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)

  }

  @Test
  def testGroupedAggregateWithLongKeys(): Unit = {
    val ds = BatchTableEnvUtil.fromCollection(tEnv,
      Seq(
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
        ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2)),
      "a,b,c"
    )
      .groupBy('a, 'b)
      .select('c.sum)

    val expected = "10\n" + "8\n"
    val results = executeQuery(ds)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant1(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val expected = "4,2\n" + "4,3\n" + "4,5\n" + "4,5\n" + "4,5\n" + "4,6\n" +
      "4,6\n" + "4,6\n" + "4,3\n" + "4,4\n" + "4,6\n" + "4,1\n" + "4,4\n" +
      "4,4\n" + "4,5\n" + "4,6\n" + "4,2\n" + "4,3\n" + "4,4\n" + "4,5\n" + "4,6\n"
    val results = executeQuery(t)

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant2(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    val expected = "4,1\n" + "4,5\n" + "4,15\n" + "4,34\n" + "4,65\n" + "4,111\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithExpression(): Unit = {
    val t = CollectionBatchExecTable.get5TupleDataSet(tEnv, "a, b, c, d, e")
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val expected = Seq(
      s"0,1,${1 / 1},1", s"7,1,${9 / 2},2", s"2,1,${6 / 2},2",
      s"3,2,${11 / 3},3", s"1,2,${10 / 3},3", s"14,2,${5 / 1},1",
      s"12,3,${5 / 1},1", s"5,3,${8 / 2},2").mkString("\n")
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithFilter(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    val expected = "2,5\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAnalyticAggregation(): Unit = {
    val ds = BatchTableEnvUtil.fromElements(tEnv,
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, BigDecimal.ONE),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, new BigDecimal(2)))
    val res = ds.select(
      '_1.stddevPop, '_2.stddevPop, '_3.stddevPop, '_4.stddevPop, '_5.stddevPop,
      '_6.stddevPop, '_7.stddevPop,
      '_1.stddevSamp, '_2.stddevSamp, '_3.stddevSamp, '_4.stddevSamp, '_5.stddevSamp,
      '_6.stddevSamp, '_7.stddevSamp,
      '_1.varPop, '_2.varPop, '_3.varPop, '_4.varPop, '_5.varPop,
      '_6.varPop, '_7.varPop,
      '_1.varSamp, '_2.varSamp, '_3.varSamp, '_4.varSamp, '_5.varSamp,
      '_6.varSamp, '_7.varSamp)
    val expected =
      "0,0,0," +
        "0,0.5,0.5,0.500000000000000000," +
        "1,1,1," +
        "1,0.70710677,0.7071067811865476,0.707106781186547600," +
        "0,0,0," +
        "0,0.25,0.25,0.250000000000000000," +
        "1,1,1," +
        "1,0.5,0.5,0.500000000000000000"
    val results = executeQuery(res)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testComplexAggregate(): Unit = {
    val top10Fun = new Top10

    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('b, top10Fun('b.cast(Types.INT), 'a.cast(Types.FLOAT)))

    val expected =
      "1,[1,1.0, null, null, null, null, null, null, null, null, null]\n" +
        "2,[2,3.0, 2,2.0, null, null, null, null, null, null, null, null]\n" +
        "3,[3,6.0, 3,5.0, 3,4.0, null, null, null, null, null, null, null]\n" +
        "4,[4,10.0, 4,9.0, 4,8.0, 4,7.0, null, null, null, null, null, null]\n" +
        "5,[5,15.0, 5,14.0, 5,13.0, 5,12.0, 5,11.0, null, null, null, null, null]\n" +
        "6,[6,21.0, 6,20.0, 6,19.0, 6,18.0, 6,17.0, 6,16.0, null, null, null, null]"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCollect(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
      .groupBy('b)
      .select('b, 'a.collect)

    val expected =
      "1,{1=1}\n" +
        "2,{2=1, 3=1}\n" +
        "3,{4=1, 5=1, 6=1}\n" +
        "4,{7=1, 8=1, 9=1, 10=1}\n" +
        "5,{11=1, 12=1, 13=1, 14=1, 15=1}\n" +
        "6,{16=1, 17=1, 18=1, 19=1, 20=1, 21=1}"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

case class WC(word: String, frequency: Long)

/**
  * User-defined aggregation function to compute the TOP 10 most visited pages.
  * We use and Array[Tuple2[Int, Float]] as accumulator to store the 10 most visited pages.
  *
  * The result is emitted as Array as well.
  */
class Top10 extends AggregateFunction[Array[JTuple2[JInt, JFloat]], Array[JTuple2[JInt, JFloat]]] {

  @Override
  def createAccumulator(): Array[JTuple2[JInt, JFloat]] = {
    new Array[JTuple2[JInt, JFloat]](10)
  }

  /**
    * Adds a new pages and count to the Top10 pages if necessary.
    *
    * @param acc The current top 10
    * @param adId The id of the ad
    * @param revenue The revenue for the ad
    */
  def accumulate(acc: Array[JTuple2[JInt, JFloat]], adId: Int, revenue: Float) {

    var i = 9
    var skipped = 0

    // skip positions without records
    while (i >= 0 && acc(i) == null) {
      if (acc(i) == null) {
        // continue until first entry in the top10 list
        i -= 1
      }
    }
    // backward linear search for insert position
    while (i >= 0 && revenue > acc(i).f1) {
      // check next entry
      skipped += 1
      i -= 1
    }

    // set if necessary
    if (i < 9) {
      // move entries with lower count by one position
      if (i < 8 && skipped > 0) {
        System.arraycopy(acc, i + 1, acc, i + 2, skipped)
      }

      // add page to top10 list
      acc(i + 1) = JTuple2.of(adId, revenue)
    }
  }

  override def getValue(acc: Array[JTuple2[JInt, JFloat]]): Array[JTuple2[JInt, JFloat]] = acc

  def resetAccumulator(acc: Array[JTuple2[JInt, JFloat]]): Unit = {
    java.util.Arrays.fill(acc.asInstanceOf[Array[Object]], null)
  }

  def merge(
      acc: Array[JTuple2[JInt, JFloat]],
      its: java.lang.Iterable[Array[JTuple2[JInt, JFloat]]]): Unit = {

    val it = its.iterator()
    while (it.hasNext) {
      val acc2 = it.next()

      var i = 0
      var i2 = 0
      while (i < 10 && i2 < 10 && acc2(i2) != null) {
        if (acc(i) == null) {
          // copy to empty place
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        } else if (acc(i).f1.asInstanceOf[Float] >= acc2(i2).f1.asInstanceOf[Float]) {
          // forward to next
          i += 1
        } else {
          // shift and copy
          System.arraycopy(acc, i, acc, i + 1, 9 - i)
          acc(i) = acc2(i2)
          i += 1
          i2 += 1
        }
      }
    }
  }

  override def getAccumulatorType = {
    ObjectArrayTypeInfo.getInfoFor(
      new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }

  override def getResultType = {
    ObjectArrayTypeInfo.getInfoFor(
      new TupleTypeInfo[JTuple2[JInt, JFloat]](Types.INT, Types.FLOAT))
  }
}
