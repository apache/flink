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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinctWithMergeAndReset, WeightedAvgWithMergeAndReset}
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.{NonMergableCount, Top10}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row

import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.math.BigDecimal

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class AggregationsITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testAggregationWithCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val inputTable = CollectionDataSets.getSmallNestedTupleDataSet(env).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("MyTable", inputTable)

    val result = tEnv.scan("MyTable")
      .where('a.get("_1") > 0)
      .select('a.get("_1").avg, 'a.get("_2").sum, 'b.count)

    val expected = "2,6,3"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      .select('_1.sum, '_1.sum0, '_1.min, '_1.max, '_1.count, '_1.avg)

    val results = t.toDataSet[Row].collect()
    val expected = "231,231,1,21,21,11"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testWorkingAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable(tEnv)
      .select('_1.avg, '_2.avg, '_3.avg, '_4.avg, '_5.avg, '_6.avg, '_7.count)

    val expected = "1,1,1,1,1.5,1.5,2"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testProjection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = env.fromElements(
      (1: Byte, 1: Short),
      (2: Byte, 2: Short)).toTable(tEnv)
      .select('_1.avg, '_1.sum, '_1.count, '_2.avg, '_2.sum)

    val expected = "1,3,2,1,3"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable(tEnv)
      .select(('_1 + 2).avg + 2, '_2.count + 5)

    val expected = "5.5,7"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithTwoCount(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable(tEnv)
      .select('_1.count, '_2.count)

    val expected = "2,2"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationAfterProjection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable(tEnv)
      .select('_1, '_2, '_3)
      .select('_1.avg, '_2.sum, '_3.count)

    val expected = "1,3,2"
    val result = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSQLStyleAggregations(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .select(
        """Sum( a) as a1, a.sum as a2,
          |Min (a) as b1, a.min as b2,
          |Max (a ) as c1, a.max as c2,
          |Avg ( a ) as d1, a.avg as d2,
          |Count(a) as e1, a.count as e2
        """.stripMargin)

    val expected = "231,231,1,1,21,21,11,11,21,21"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPojoAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val input = env.fromElements(
      WC("hello", 1),
      WC("hello", 1),
      WC("ciao", 1),
      WC("hola", 1),
      WC("hola", 1))
    val expr = input.toTable(tEnv)
    val result = expr
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
      .toDataSet[WC]

    val mappedResult = result.map(w => (w.word, w.frequency * 10)).collect()
    val expected = "(hello,20)\n" + "(hola,20)"
    TestBaseUtils.compareResultAsText(mappedResult.asJava, expected)
  }

  @Test
  def testDistinct(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val distinct = ds.select('b).distinct()

    val expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
    val results = distinct.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDistinctAfterAggregate(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    val distinct = ds.groupBy('a, 'e).select('e).distinct()

    val expected = "1\n" + "2\n" + "3\n"
    val results = distinct.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val countFun = new CountAggFunction
    val wAvgFun = new WeightedAvgWithMergeAndReset
    val countDistinct = new CountDistinctWithMergeAndReset

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum, countFun('c), wAvgFun('b, 'a), wAvgFun('a, 'a), countDistinct('c))

    val expected = "1,1,1,1,1,1\n" + "2,5,2,2,2,2\n" + "3,15,3,3,5,3\n" + "4,34,4,4,8,4\n" +
      "5,65,5,5,13,5\n" + "6,111,6,6,18,6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupingKeyForwardIfNotUsed(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.sum)

    val expected = "1\n" + "5\n" + "15\n" + "34\n" + "65\n" + "111\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupNoAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)
    val expected = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregateEmptyDataSets(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val myAgg = new NonMergableCount

    val t1 = env.fromCollection(new mutable.MutableList[(Int, String)]).toTable(tEnv, 'a, 'b)
      .select('a.sum, 'a.count)
    val t2 = env.fromCollection(new mutable.MutableList[(Int, String)]).toTable(tEnv, 'a, 'b)
      .select('a.sum, myAgg('b), 'a.count)

    val expected1 = "null,0"
    val expected2 = "null,0,0"

    val results1 = t1.toDataSet[Row].collect()
    val results2 = t2.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results1.asJava, expected1)
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)

  }

  @Test
  def testGroupedAggregateWithLongKeys(): Unit = {
    // This uses very long keys to force serialized comparison.
    // With short keys, the normalized key is sufficient.
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = env.fromElements(
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaa", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2),
      ("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhab", 1, 2))
      .rebalance().setParallelism(2).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('a, 'b)
      .select('c.sum)

    val expected = "10\n" + "8\n"
    val results = ds.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant1(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val expected = "4,2\n" + "4,3\n" + "4,5\n" + "4,5\n" + "4,5\n" + "4,6\n" +
      "4,6\n" + "4,6\n" + "4,3\n" + "4,4\n" + "4,6\n" + "4,1\n" + "4,4\n" +
      "4,4\n" + "4,5\n" + "4,6\n" + "4,2\n" + "4,3\n" + "4,4\n" + "4,5\n" + "4,6\n"
    val results = t.toDataSet[Row].collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithConstant2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    val expected = "4,1\n" + "4,5\n" + "4,15\n" + "4,34\n" + "4,65\n" + "4,111\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithExpression(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val expected = "0,1,1,1\n" + "3,2,3,3\n" + "7,1,4,2\n" + "14,2,5,1\n" +
      "5,3,4,2\n" + "2,1,3,2\n" + "1,2,3,3\n" + "12,3,5,1"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedAggregateWithFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    val expected = "2,5\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAnalyticAggregation(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv =BatchTableEnvironment.create(env)
    val ds = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, BigDecimal.ONE),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, new BigDecimal(2))).toTable(tEnv)
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
        "0,0.5,0.5,0.5," +
        "1,1,1," +
        "1,0.70710677,0.7071067811865476,0.7071067811865476," +
        "0,0,0," +
        "0,0.25,0.25,0.25," +
        "1,1,1," +
        "1,0.5,0.5,0.5"
    val results = res.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testComplexAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)
    val top10Fun = new Top10

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, top10Fun('b.cast(Types.INT), 'a.cast(Types.FLOAT)))

    val expected =
      "1,[(1,1.0), null, null, null, null, null, null, null, null, null]\n" +
      "2,[(2,3.0), (2,2.0), null, null, null, null, null, null, null, null]\n" +
      "3,[(3,6.0), (3,5.0), (3,4.0), null, null, null, null, null, null, null]\n" +
      "4,[(4,10.0), (4,9.0), (4,8.0), (4,7.0), null, null, null, null, null, null]\n" +
      "5,[(5,15.0), (5,14.0), (5,13.0), (5,12.0), (5,11.0), null, null, null, null, null]\n" +
      "6,[(6,21.0), (6,20.0), (6,19.0), (6,18.0), (6,17.0), (6,16.0), null, null, null, null]"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCollect(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.collect)

    val expected =
      "1,{1=1}\n2,{2=1, 3=1}\n3,{4=1, 5=1, 6=1}\n4,{8=1, 9=1, 10=1, 7=1}\n" +
        "5,{11=1, 12=1, 13=1, 14=1, 15=1}\n6,{16=1, 17=1, 18=1, 19=1, 20=1, 21=1}"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

case class WC(word: String, frequency: Long)
