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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.planner.utils.{CountAggFunction, TableFunc0, TableTestBase}

import org.junit.Test

/**
  * Tests for column functions which includes tests for different column functions.
  */
class ColumnFunctionsTest extends TableTestBase {

  val util = streamTestUtil()

  @Test
  def testStar(): Unit = {

    val t = util.addTableSource[(Double, Long)]('double, 'long)

   util.addFunction("TestFunc", TestFunc)
    val tab1 = t.select(call("TestFunc", withColumns('*)))
    val tab2 = t.select("TestFunc(withColumns(*))")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testColumnRange(): Unit = {
    val t = util.addTableSource[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t.select(withColumns('b to 'c), 'a, withColumns(5 to 6, 'd))
    val tab2 = t.select("withColumns(b to c), a, withColumns(5 to 6, d)")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testColumnWithoutRange(): Unit = {
    val t = util.addTableSource[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t.select(withColumns(1, 'b, 'c), 'f)
    val tab2 = t.select("withColumns(1, b, c), f")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testInverseSelection(): Unit = {
    val t = util.addTableSource[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t
      .select(withoutColumns(1, 'b))
      .select(withoutColumns(1 to 2))

    val tab2 = t
      .select("withoutColumns(1, b)")
      .select("withoutColumns(1 to 2)")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testColumnFunctionsInUDF(): Unit = {
    val t = util.addTableSource[(Int, Long, String, String)]('int, 'long, 'string1, 'string2)

    val tab1 = t.select(concat(withColumns('string1 to 'string2)))
    val tab2 = t.select("concat(withColumns(string1 to string2))")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testJoin(): Unit = {
    val t1 = util.addTableSource[(Int, Long, String)]('int1, 'long1, 'string1)
    val t2 = util.addTableSource[(Int, Long, String)]('int2, 'long2, 'string2)

    val tab1 = t1.join(t2, withColumns(1) === withColumns(4))
    val tab2 = t1.join(t2, "withColumns(1) === withColumns(4)")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testJoinLateral(): Unit = {
    val t = util.addTableSource[(Double, Long, String)]('int, 'long, 'string)
    val func0 = new TableFunc0
   util.addFunction("func0", func0)

    val tab1 = t.joinLateral(func0(withColumns('string)))
    util.verifyPlan(tab1)
  }

  @Test
  def testFilter(): Unit = {
    val t = util.addTableSource[(Int, Long, String, String)]('int, 'long, 'string1, 'string2)

    val tab1 = t.where(concat(withColumns('string1 to 'string2)) === "a")
    val tab2 = t.where("concat(withColumns(string1 to string2)) = 'a'")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testGroupBy(): Unit = {
    val t = util.addTableSource[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t
      .groupBy(withColumns(1), 'b)
      .select('a, 'b, withColumns('c).count)

    val tab2 = t
      .groupBy("withColumns(1), b")
      .select("a, b, withColumns(c).count")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testWindowGroupBy(): Unit = {
    val t = util.addDataStream[(Int, Long, String, Int)]("T1",'a, 'rowtime.rowtime, 'c, 'd)
      .as("a", "b", "c", "d")

    val tab1 = t
      .window(Slide over 3.milli every 10.milli on withColumns('b) as 'w)
      .groupBy(withColumns('a, 'b), 'w)
      .select(withColumns(1 to 2), withColumns('c).count as 'c)

    val tab2 = t
      .window(Slide.over("3.milli").every("10.milli").on("withColumns(b)").as("w"))
      .groupBy("withColumns(a, b), w")
      .select("withColumns(1 to 2), withColumns(c).count as c")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testOver(): Unit = {
    val table = util.addDataStream[(Long, Int, String)]("T1", 'a, 'b, 'c, 'proctime.proctime)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDist = new CountDistinct

   util.addFunction("countFun", countFun)
   util.addTemporarySystemFunction("weightAvgFun", weightAvgFun)
   util.addFunction("countDist", countDist)

    val tab1 = table
      .window(
        Over partitionBy withColumns('c) orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c,
        call("countFun", withColumns('b)) over 'w as 'mycount,
        call("weightAvgFun", withColumns('a to 'b)) over 'w as 'wAvg,
        call("countDist", 'a) over 'w as 'countDist)
      .select('c, 'mycount, 'wAvg, 'countDist)

    val tab2 = table
      .window(
        Over.partitionBy("withColumns(c)")
          .orderBy("proctime")
          .preceding("UNBOUNDED_ROW")
          .as("w"))
      .select("c, countFun(withColumns(b)) over w as mycount, " +
        "weightAvgFun(withColumns(a to b)) over w as wAvg, countDist(a) over w as countDist")
      .select('c, 'mycount, 'wAvg, 'countDist)
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testAddColumns(): Unit = {
    val t = util.addTableSource[(Double, Long, String)]('a, 'b, 'c)

   util.addFunction("TestFunc", TestFunc)
    val tab1 = t.addColumns(call("TestFunc", withColumns('a, 'b)) as 'd)
    val tab2 = t.addColumns("TestFunc(withColumns(a, b)) as d")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testRenameColumns(): Unit = {
    val t = util.addTableSource[(Double, Long, String)]('a, 'b, 'c)

    val tab1 = t.renameColumns(withColumns('a) as 'd).select("d, b")
    val tab2 = t.renameColumns("withColumns(a) as d").select('d, 'b)
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }

  @Test
  def testDropColumns(): Unit = {
    val t = util.addTableSource[(Double, Long, String)]('a, 'b, 'c)

    val tab1 = t.dropColumns(withColumns('a to 'b))
    val tab2 = t.dropColumns("withColumns(a to b)")
    verifyTableEquals(tab1, tab2)
    util.verifyPlan(tab1)
  }
}

@SerialVersionUID(1L)
object TestFunc extends ScalarFunction {
  def eval(a: Double, b: Long): Double = {
    a
  }
}
