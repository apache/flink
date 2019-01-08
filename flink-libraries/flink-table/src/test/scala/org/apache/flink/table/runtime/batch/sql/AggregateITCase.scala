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

package org.apache.flink.table.runtime.batch.sql

import org.apache.calcite.runtime.SqlFunctions.{internalToTimestamp => toTimestamp}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.runtime.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.NonMergableCount
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class AggregateITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testAggregationTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT sum(_1), min(_1), max(_1), count(_1), avg(_1) FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "231,1,21,21,11"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT sum(_1) FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "231"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDataSetAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT sum(_1) FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "231"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationDataTypes(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT avg(a), avg(b), avg(c), avg(d), avg(e), avg(f), count(g), " +
      "min(g), min('Ciao'), max(g), max('Ciao'), sum(CAST(f AS DECIMAL)) FROM MyTable"

    val ds = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f, 'g)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "1,1,1,1,1.5,1.5,2,Ciao,Ciao,Hello,Ciao,3.0"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableProjection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT avg(a), sum(a), count(a), avg(b), sum(b) " +
      "FROM MyTable"

    val ds = env.fromElements((1: Byte, 1: Short), (2: Byte, 2: Short)).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "1,3,2,1,3"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableAggregationWithArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT avg(a + 2) + 2, count(b) + 5 " +
      "FROM MyTable"

    val ds = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "5.5,7"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAggregationWithTwoCount(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT count(_1), count(_2) FROM MyTable"

    val ds = env.fromElements((1f, "Hello"), (2f, "Ciao")).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "2,2"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testAggregationAfterProjection(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM " +
      "(SELECT _1 as a, _2 as b, _3 as c FROM MyTable)"

    val ds = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, "Hello"),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d, "Ciao")).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "1,3,2"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDistinctAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT sum(_1) as a, count(distinct _3) as b FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "231,21"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupedDistinctAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT _2, avg(distinct _1) as a, count(_3) as b FROM MyTable GROUP BY _2"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)

    val expected =
      "6,18,6\n5,13,5\n4,8,4\n3,5,3\n2,2,2\n1,1,1"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupingSetAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery =
      "SELECT _2, _3, avg(_1) as a, GROUP_ID() as g FROM MyTable GROUP BY GROUPING SETS (_2, _3)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()

    val expected =
      "6,null,18,1\n5,null,13,1\n4,null,8,1\n3,null,5,1\n2,null,2,1\n1,null,1,1\n" +
        "null,Luke Skywalker,6,2\nnull,I am fine.,5,2\nnull,Hi,1,2\n" +
        "null,Hello world, how are you?,4,2\nnull,Hello world,3,2\nnull,Hello,2,2\n" +
        "null,Comment#9,15,2\nnull,Comment#8,14,2\nnull,Comment#7,13,2\n" +
        "null,Comment#6,12,2\nnull,Comment#5,11,2\nnull,Comment#4,10,2\n" +
        "null,Comment#3,9,2\nnull,Comment#2,8,2\nnull,Comment#15,21,2\n" +
        "null,Comment#14,20,2\nnull,Comment#13,19,2\nnull,Comment#12,18,2\n" +
        "null,Comment#11,17,2\nnull,Comment#10,16,2\nnull,Comment#1,7,2"

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testAggregateEmptyDataSets(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    val myAgg = new NonMergableCount
    tEnv.registerFunction("myAgg", myAgg)

    val sqlQuery = "SELECT avg(a), sum(a), count(b) " +
      "FROM MyTable where a = 4 group by a"

    val sqlQuery2 = "SELECT avg(a), sum(a), count(b) " +
      "FROM MyTable where a = 4"

    val sqlQuery3 = "SELECT avg(a), sum(a), count(b) " +
      "FROM MyTable"

    val sqlQuery4 = "SELECT avg(a), sum(a), count(b), myAgg(b)" +
      "FROM MyTable where a = 4"

    val ds = env.fromElements(
      (1: Byte, 1: Short),
      (2: Byte, 2: Short))
      .toTable(tEnv, 'a, 'b)

    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sqlQuery(sqlQuery)
    val result2 = tEnv.sqlQuery(sqlQuery2)
    val result3 = tEnv.sqlQuery(sqlQuery3)
    val result4 = tEnv.sqlQuery(sqlQuery4)

    val results = result.toDataSet[Row].collect()
    val expected = Seq.empty
    val results2 =  result2.toDataSet[Row].collect()
    val expected2 = "null,null,0"
    val results3 = result3.toDataSet[Row].collect()
    val expected3 = "1,3,2"
    val results4 =  result4.toDataSet[Row].collect()
    val expected4 = "null,null,0,0"

    assert(results.equals(expected),
      "Empty result is expected for grouped set, but actual: " + results)
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)
    TestBaseUtils.compareResultAsText(results3.asJava, expected3)
    TestBaseUtils.compareResultAsText(results4.asJava, expected4)
  }

  @Test
  def testTumbleWindowAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithMergeAndReset", new WeightedAvgWithMergeAndReset)

    val sqlQuery =
      "SELECT b, SUM(a), countFun(c), wAvgWithMergeAndReset(b, a), wAvgWithMergeAndReset(a, a)" +
        "FROM T " +
        "GROUP BY b, TUMBLE(ts, INTERVAL '3' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "1,1,1,1,1",
      "2,2,1,2,2", "2,3,1,2,3",
      "3,9,2,3,4", "3,6,1,3,6",
      "4,15,2,4,7", "4,19,2,4,9",
      "5,11,1,5,11", "5,39,3,5,13", "5,15,1,5,15",
      "6,33,2,6,16", "6,57,3,6,19", "6,21,1,6,21"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testTumbleWindowAggregateWithCollect(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery =
      "SELECT b, COLLECT(b)" +
        "FROM T " +
        "GROUP BY b, TUMBLE(ts, INTERVAL '3' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "1,{1=1}",
      "2,{2=1}", "2,{2=1}",
      "3,{3=1}", "3,{3=2}",
      "4,{4=2}", "4,{4=2}",
      "5,{5=1}", "5,{5=1}", "5,{5=3}",
      "6,{6=1}", "6,{6=2}", "6,{6=3}"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c, 'ts)

    val t2 = tEnv.sqlQuery("SELECT b, COLLECT(b) as `set`" +
        "FROM t1 " +
        "GROUP BY b, TUMBLE(ts, INTERVAL '3' SECOND)")
    tEnv.registerTable("t2", t2)

    val result = tEnv.sqlQuery("SELECT b, s FROM t2, UNNEST(t2.`set`) AS A(s) where b < 3")
      .toDataSet[Row]
      .collect()

    val expected = Seq(
      "1,1",
      "2,2",
      "2,2"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testTumbleWindowWithProperties(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "TUMBLE_START(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_END(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND)" +
      "FROM T " +
      "GROUP BY b, TUMBLE(ts, INTERVAL '5' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // min time unit is seconds
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "3,1,1970-01-01 00:00:00.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "3,2,1970-01-01 00:00:05.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "4,3,1970-01-01 00:00:05.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "4,1,1970-01-01 00:00:10.0,1970-01-01 00:00:15.0,1970-01-01 00:00:14.999",
      "5,4,1970-01-01 00:00:10.0,1970-01-01 00:00:15.0,1970-01-01 00:00:14.999",
      "5,1,1970-01-01 00:00:15.0,1970-01-01 00:00:20.0,1970-01-01 00:00:19.999",
      "6,4,1970-01-01 00:00:15.0,1970-01-01 00:00:20.0,1970-01-01 00:00:19.999",
      "6,2,1970-01-01 00:00:20.0,1970-01-01 00:00:25.0,1970-01-01 00:00:24.999"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testHopWindowAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithMergeAndReset", new WeightedAvgWithMergeAndReset)

    val sqlQuery =
      "SELECT b, SUM(a), countFun(c), wAvgWithMergeAndReset(b, a), wAvgWithMergeAndReset(a, a)" +
        "FROM T " +
        "GROUP BY b, HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "1,1,1,1,1","1,1,1,1,1",
      "2,5,2,2,2","2,5,2,2,2",
      "3,9,2,3,4", "3,15,3,3,5", "3,6,1,3,6",
      "4,7,1,4,7", "4,24,3,4,8", "4,27,3,4,9", "4,10,1,4,10",
      "5,11,1,5,11", "5,36,3,5,12", "5,54,4,5,13", "5,29,2,5,14",
      "6,33,2,6,16", "6,70,4,6,17", "6,78,4,6,19", "6,41,2,6,20"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testHopWindowWithProperties(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_ROWTIME(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
      "FROM T " +
      "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "1,1,1969-12-31 23:59:55.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "2,2,1969-12-31 23:59:55.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "3,1,1969-12-31 23:59:55.0,1970-01-01 00:00:05.0,1970-01-01 00:00:04.999",
      "1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "2,2,1970-01-01 00:00:00.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "3,3,1970-01-01 00:00:00.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "4,3,1970-01-01 00:00:00.0,1970-01-01 00:00:10.0,1970-01-01 00:00:09.999",
      "3,2,1970-01-01 00:00:05.0,1970-01-01 00:00:15.0,1970-01-01 00:00:14.999",
      "4,4,1970-01-01 00:00:05.0,1970-01-01 00:00:15.0,1970-01-01 00:00:14.999",
      "5,4,1970-01-01 00:00:05.0,1970-01-01 00:00:15.0,1970-01-01 00:00:14.999",
      "4,1,1970-01-01 00:00:10.0,1970-01-01 00:00:20.0,1970-01-01 00:00:19.999",
      "5,5,1970-01-01 00:00:10.0,1970-01-01 00:00:20.0,1970-01-01 00:00:19.999",
      "6,4,1970-01-01 00:00:10.0,1970-01-01 00:00:20.0,1970-01-01 00:00:19.999",
      "5,1,1970-01-01 00:00:15.0,1970-01-01 00:00:25.0,1970-01-01 00:00:24.999",
      "6,6,1970-01-01 00:00:15.0,1970-01-01 00:00:25.0,1970-01-01 00:00:24.999",
      "6,2,1970-01-01 00:00:20.0,1970-01-01 00:00:30.0,1970-01-01 00:00:29.999"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSessionWindowAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithMergeAndReset", new WeightedAvgWithMergeAndReset)

    val sqlQuery =
      "SELECT MIN(a), MAX(a), SUM(a), countFun(c), wAvgWithMergeAndReset(b, a), " +
        "wAvgWithMergeAndReset(a, a)" +
        "FROM T " +
        "GROUP BY SESSION(ts, INTERVAL '4' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .filter(x => (x._2 % 2) == 0)
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "2,10,39,6,3,7",
      "16,21,111,6,6,18"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSessionWindowWithProperties(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery =
      "SELECT COUNT(a), " +
        "SESSION_START(ts, INTERVAL '4' SECOND), " +
        "SESSION_END(ts, INTERVAL '4' SECOND), " +
        "SESSION_ROWTIME(ts, INTERVAL '4' SECOND) " +
      "FROM T " +
      "GROUP BY SESSION(ts, INTERVAL '4' SECOND)"

    val ds = CollectionDataSets.get3TupleDataSet(env)
      // create timestamps
      .filter(x => (x._2 % 2) == 0)
      .map(x => (x._1, x._2, x._3, toTimestamp(x._1 * 1000)))
    tEnv.registerDataSet("T", ds, 'a, 'b, 'c, 'ts)

    val result = tEnv.sqlQuery(sqlQuery).toDataSet[Row].collect()
    val expected = Seq(
      "6,1970-01-01 00:00:02.0,1970-01-01 00:00:14.0,1970-01-01 00:00:13.999",
      "6,1970-01-01 00:00:16.0,1970-01-01 00:00:25.0,1970-01-01 00:00:24.999"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }
}
