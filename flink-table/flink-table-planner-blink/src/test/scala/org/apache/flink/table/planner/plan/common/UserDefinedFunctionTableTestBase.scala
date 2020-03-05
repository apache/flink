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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, _}
import org.apache.flink.table.planner.expressions.utils.{Func0, Func13}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{VarSum3AggFunction, VarSumAggFunction}
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableAggFunc, TableFunc1, TableFunc2, TableFunc5, TableTestBase, TableTestUtil}

import org.junit.{Before, Test}


abstract class UserDefinedFunctionTableTestBase extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil

  protected def getTableTestUtil: TableTestUtil

  private var table: Table = _

  @Before
  def setup(): Unit = {
    table = util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testTableFunction(): Unit = {
    val func1 = new TableFunc1
    util.addFunction("table_func1", func1)
    val func5 = new TableFunc5(",")
    util.addFunction("table_func5", func5)

    val t = table
      .joinLateral("table_func1(c) as d").select('c, 'd)
      .joinLateral(func1('d) as 'e).select('c, 'e)
      .joinLateral("table_func5(e) as f").select('c, 'f)
      .joinLateral(func5('f) as 'h).select('c, 'h)
      .joinLateral(new TableFunc5(";")('h) as 'i).select('c, 'i)
      .joinLateral(new TableFunc2()('c) as('k, 'j)).select('c, 'i, 'k, 'j)
    util.verifyPlan(t)
  }

  @Test
  def testScalarFunction(): Unit = {
    util.addFunction("func0", Func0)
    val func13 = new Func13("test")
    util.addFunction("func13", func13)

    val t = table
      .select("a, c, func0(a) as a1, func13(c) as c1")
      .select('a, 'a1, Func0('a) as 'a2, 'c, 'c1, new Func13("abc")('c) as 'c2, func13('c) as 'c3)

    util.verifyPlan(t)
  }

  @Test
  def testAggregateFunction(): Unit = {
    val varSum = new VarSumAggFunction
    util.addFunction("var_sum", varSum)
    val varSum3 = new VarSum3AggFunction(10L)
    util.addFunction("var_sum3", varSum3)

    val t = table.groupBy('b)
      .select("b, var_sum(a) as a1, var_sum3(a) as a2")
      .groupBy('a1, 'a2)
      .select('a1, 'a2, varSum('b) as 'b1, varSum3('b) as 'b2,
        new VarSum3AggFunction(100L)('b) as 'b3, varSum('b) as 'b4, varSum3('b) as 'b5)

    util.verifyPlan(t)
  }

  @Test
  def testTableAggregateFunction(): Unit = {
    val tableAggFunc1 = new EmptyTableAggFunc
    util.addFunction("table_agg_func1", tableAggFunc1)
    val tableAggFunc2 = new TableAggFunc(Int.MinValue + 1)
    util.addFunction("table_agg_func2", tableAggFunc2)

    val t = table
      .flatAggregate("table_agg_func1(a) as (a1, a2)")
      .select("*")
      .flatAggregate("table_agg_func2(a1) as (a3, a4)")
      .select("*")
      .flatAggregate(tableAggFunc1('a3) as('a5, 'a6))
      .select("*")
      .flatAggregate(tableAggFunc2('a6) as('a7, 'a8))
      .select("*")
      .flatAggregate(new TableAggFunc(Int.MinValue + 2)('a7) as('a9, 'a10))
      .select("*")

    util.verifyPlan(t)
  }

}
