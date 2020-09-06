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

package org.apache.flink.table.api.stream.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.Func1
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithRetract}
import org.apache.flink.table.utils.TableTestBase

import org.junit.Test

class OverWindowStringExpressionTest extends TableTestBase {

  @Test
  def testPartitionedUnboundedOverRow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
      .select('a, 'b.sum over 'w as 'cnt, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_row").as("w"))
      .select("a, SUM(b) OVER w as cnt, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testUnboundedOverRow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over orderBy 'rowtime preceding UNBOUNDED_ROW following CURRENT_ROW as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.orderBy("rowtime").preceding("unbounded_row").following("current_row").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testPartitionedBoundedOverRow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over partitionBy('a, 'd) orderBy 'rowtime preceding 10.rows as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.partitionBy("a, d").orderBy("rowtime").preceding("10.rows").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testBoundedOverRow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over orderBy 'rowtime preceding 10.rows following CURRENT_ROW as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.orderBy("rowtime").preceding("10.rows").following("current_row").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testPartitionedUnboundedOverRange(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_range").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testRowTimeUnboundedOverRange(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over orderBy 'rowtime preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(
        Over.orderBy("rowtime").preceding("unbounded_range").following("current_range").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")
    val resJava2 = t
      .window(
        Over.orderBy("rowtime").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
    verifyTableEquals(resScala, resJava2)
  }

  @Test
  def testProcTimeUnboundedOverRange(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'proctime.proctime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over orderBy 'proctime preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(
        Over.orderBy("proctime").preceding("unbounded_range").following("current_range").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")
    val resJava2 = t
      .window(
        Over.orderBy("proctime").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
    verifyTableEquals(resScala, resJava2)
  }

  @Test
  def testPartitionedBoundedOverRange(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over partitionBy('a, 'c) orderBy 'rowtime preceding 10.minutes as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.partitionBy("a, c").orderBy("rowtime").preceding("10.minutes").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testBoundedOverRange(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    val resScala = t
      .window(Over orderBy 'rowtime preceding 4.hours following CURRENT_RANGE as 'w)
      .select('a, 'b.sum over 'w, weightAvgFun('a, 'b) over 'w as 'myCnt)
    val resJava = t
      .window(Over.orderBy("rowtime").preceding("4.hours").following("current_range").as("w"))
      .select("a, SUM(b) OVER w, weightAvgFun(a, b) over w as myCnt")

    verifyTableEquals(resScala, resJava)
  }

  @Test
  def testScalarFunctionsOnOverWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithRetract
    val plusOne = Func1
    util.addFunction("plusOne", plusOne)
    util.addFunction("weightedAvg", weightedAvg)

    val resScala = t
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
      .select(
        array('a.sum over 'w, 'a.count over 'w),
        plusOne('b.sum over 'w as 'wsum) as 'd,
        ('a.count over 'w).exp(),
        (weightedAvg('a, 'b) over 'w) + 1,
        "AVG:".toExpr + (weightedAvg('a, 'b) over 'w))

    val resJava = t
      .window(Over.partitionBy("a").orderBy("rowtime").preceding("unbounded_row").as("w"))
      .select(
        s"""
           |ARRAY(SUM(a) OVER w, COUNT(a) OVER w),
           |plusOne(SUM(b) OVER w AS wsum) AS d,
           |EXP(COUNT(a) OVER w),
           |(weightedAvg(a, b) OVER w) + 1,
           |'AVG:' + (weightedAvg(a, b) OVER w)
         """.stripMargin)

    verifyTableEquals(resScala, resJava)
  }
}
