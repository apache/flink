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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.expressions.utils.Func23
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class CalcStringExpressionTest extends TableTestBase {

  @Test
  def testSimpleSelect(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]()

    val resScala = t.select('_1, '_2)
    val resJava = t.select("_1, _2")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testSelectStar(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.select('*)
    val resJava = t.select("*")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testSelectWithWhere(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)
    val resScala = t.where('string === "true").select('int)
    val resJava = t.where("string === 'true'").select("int")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.select('int, 'string)
    val resJava = t.select("int, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testSimpleSelectWithAlias(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)
    val resScala = t.select('int as 'myInt, 'string as 'myString)
    val resJava = t.select("int as myInt, string as myString")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testSimpleFilter(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.filter('int === 3).select('int as 'myInt, 'string)
    val resJava = t.filter("int === 3").select("int as myInt, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.filter(false).select('int as 'myInt, 'string)
    val resJava = t.filter("false").select("int as myInt, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.filter(true).select('int as 'myInt, 'string)
    val resJava = t.filter("true").select("int as myInt, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testNotEqualsFilter(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.filter('int !== 2).filter('string.like("%world%")).select('int, 'string)
    val resJava = t.filter("int !== 2").filter("string.like('%world%')").select("int, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testFilterWithExpression(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('int, 'long, 'string)

    val resScala = t.filter('int % 2 === 0).select('int, 'string)
    val resJava = t.filter("int % 2 === 0").select("int, string")
    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testAddColumns(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.addColumns(concat('c, "Sunny") as 'kid).addColumns('b + 1)
    val t2 = t.addColumns("concat(c, 'Sunny') as kid").addColumns("b + 1")

    verifyTableEquals(t1, t2)
  }

  @Test
  def addOrReplaceColumns(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    var t1 = t.addOrReplaceColumns(concat('c, "Sunny") as 'kid).addColumns('b + 1)
    var t2 = t.addOrReplaceColumns("concat(c, 'Sunny') as kid").addColumns("b + 1")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testRenameColumns(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.renameColumns('a as 'a2, 'c as 'c2)
    val t2 = t.renameColumns("a as a2, c as c2")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testDropColumns(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.dropColumns('a, 'c)
    val t2 = t.dropColumns("a,c")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testMap(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    util.tableEnv.registerFunction("func", Func23)

    val t1 = t.map("func(a, b, c)")
    val t2 = t.map(call("func", 'a, 'b, 'c))

    verifyTableEquals(t1, t2)
  }
}
