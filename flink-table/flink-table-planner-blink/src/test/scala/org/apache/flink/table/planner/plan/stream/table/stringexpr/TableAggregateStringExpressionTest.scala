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
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableTestBase}
import org.junit.Test

class TableAggregateStringExpressionTest extends TableTestBase {

  @Test
  def testNonGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new EmptyTableAggFunc
    util.addFunction("top3", top3)
    util.addFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .flatAggregate(call("top3", 'a))
      .select(call("Func0", 'f0) as 'a, 'f1 as 'b)

    // String / Java API
    val resJava = t
      .flatAggregate("top3(a)")
      .select("Func0(f0) as a, f1 as b")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new EmptyTableAggFunc
    util.addFunction("top3", top3)
    util.addFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .groupBy('b % 5)
      .flatAggregate(call("top3", 'a))
      .select(call("Func0", 'f0) as 'a, 'f1 as 'b)

    // String / Java API
    val resJava = t
      .groupBy("b % 5")
      .flatAggregate("top3(a)")
      .select("Func0(f0) as a, f1 as b")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testAliasNonGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new EmptyTableAggFunc
    util.addFunction("top3", top3)
    util.addFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .flatAggregate(top3('a) as ('d, 'e))
      .select('*)

    // String / Java API
    val resJava = t
      .flatAggregate("top3(a) as (d, e)")
      .select("*")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testAliasGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new EmptyTableAggFunc
    util.addFunction("top3", top3)
    util.addFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .groupBy('b)
      .flatAggregate(top3('a) as ('d, 'e))
      .select('*)

    // String / Java API
    val resJava = t
      .groupBy("b")
      .flatAggregate("top3(a) as (d, e)")
      .select("*")

    verifyTableEquals(resJava, resScala)
  }
}
