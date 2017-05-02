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

package org.apache.flink.table.api.scala.batch.table

import java.math.BigDecimal

import org.apache.flink.api.java.{DataSet => JDataSet, ExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => ScalaExecutionEnv}
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMergeAndReset}
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mockito.Mockito.{mock, when}

import scala.collection.JavaConverters._

/**
  * We only test some aggregations until better testing of constructed DataSet
  * programs is possible.
  */
@RunWith(classOf[Parameterized])
class DSetUDAGGITCase(configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  val data = List(
    //('long, 'int, 'double, 'float, 'bigdec, 'string)
    (1000L, 1, 1d, 1f, new BigDecimal("1"), "Hello"),
    (2000L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (3000L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (5000L, 5, 5d, 5f, new BigDecimal("5"), "Hi"),
    (6000L, 6, 6d, 6f, new BigDecimal("6"), "Hi"),
    (7000L, 7, 7d, 7f, new BigDecimal("7"), "Hi"),
    (8000L, 8, 8d, 8f, new BigDecimal("8"), "Hello"),
    (9000L, 9, 9d, 9f, new BigDecimal("9"), "Hello"),
    (4000L, 4, 4d, 4f, new BigDecimal("4"), "Hello"),
    (10000L, 10, 10d, 10f, new BigDecimal("10"), "Hi"),
    (11000L, 11, 11d, 11f, new BigDecimal("11"), "Hi"),
    (12000L, 12, 12d, 12f, new BigDecimal("12"), "Hi"),
    (16000L, 16, 16d, 16f, new BigDecimal("16"), "Hello"))

  @Test
  def testUdaggTumbleWindowGroupedAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val dataset = env.fromCollection(data).map(t => (t._1, t._2, t._3, t._4, t._6))
    val table = dataset.toTable(tEnv, 'long, 'int, 'double, 'float, 'string)

    val countFun = new CountAggFunction

    val weightedAvgWithResetAndMergeFun = new WeightedAvgWithMergeAndReset

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select(
        'string, countFun('float), 'double.sum, weightedAvgWithResetAndMergeFun('long, 'int),
        weightedAvgWithResetAndMergeFun('int, 'int))

    val results = windowedTable.toDataSet[Row].collect()
    val expected = "Hi,2,11.0,5545,5\n" + "Hi,2,17.0,8764,8\n" + "Hi,2,23.0,11521,11\n" +
      "Hello,2,3.0,1666,1\n" + "Hello,2,7.0,3571,3\n" + "Hello,2,17.0,8529,8"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUdaggTumbleWindowAllGroupedAggregate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val dataset = env.fromCollection(data).map(t => (t._1, t._2, t._3, t._4, t._6))
    val table = dataset.toTable(tEnv, 'long, 'int, 'double, 'float, 'string)

    val countFun = new CountAggFunction

    val weightedAvgWithResetAndMergeFun = new WeightedAvgWithMergeAndReset

    val windowedTable = table
      .window(Tumble over 3.second on 'long as 'w)
      .groupBy('w)
      .select(
        countFun('float),
        'double.sum,
        weightedAvgWithResetAndMergeFun('long, 'int),
        weightedAvgWithResetAndMergeFun('int, 'int))

    val results = windowedTable.toDataSet[Row].collect()
    val expected = "1,12.0,12000,12\n" + "1,16.0,16000,16\n" + "2,3.0,1666,1\n" +
      "3,12.0,4166,4\n" + "3,21.0,7095,7\n" + "3,30.0,10066,10"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUdaggGroupedAllAggregateSQL(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val dataset = env.fromCollection(data).map(t => (t._1, t._2, t._3, t._6))
    val table = dataset.toTable(tEnv, 'l, 'i, 'd, 's)

    tEnv.registerTable("T1", table)
    tEnv.registerFunction("countFun", new CountAggFunction)
    tEnv.registerFunction("wAvgWithMergeAndReset", new WeightedAvgWithMergeAndReset)
    val sqlQuery =
      "SELECT s, countFun(i), SUM(d), wAvgWithMergeAndReset(l,i), wAvgWithMergeAndReset(i,i)" +
        "FROM T1 " +
        "GROUP BY s"

    val results = tEnv.sql(sqlQuery).toDataSet[Row].collect()
    val expected = "Hello,7,43.0,10023,10\n" + "Hi,6,51.0,9313,9\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUdaggJavaAPI(): Unit = {
    // mock
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    when(ds.javaSet).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)

    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as("int, long, string")

    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = javaTableEnv.fromDataSet(jDs).as("int, long, string")

    // Java API
    javaTableEnv.registerFunction("myCountFun", new CountAggFunction)
    javaTableEnv.registerFunction("weightAvgFun", new WeightedAvg)
    var javaTable = in2
      .groupBy("string")
      .select(
        "string, " +
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int)")

    // Scala API
    val myCountFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    var scalaTable = in1
      .groupBy('string)
      .select(
        'string,
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int))

    val helper = new TableTestBase
    helper.verifyTableEquals(scalaTable, javaTable)
  }
}
