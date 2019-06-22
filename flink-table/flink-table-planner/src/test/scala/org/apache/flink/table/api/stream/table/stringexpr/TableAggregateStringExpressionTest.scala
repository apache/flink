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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.Func0
import org.apache.flink.table.utils.{TableTestBase, Top3WithMapView}
import org.junit.Test

class TableAggregateStringExpressionTest extends TableTestBase {

  @Test
  def testNonGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .flatAggregate(top3('a))
      .select(Func0('f0) as 'a, 'f1 as 'b)

    // String / Java API
    val resJava = t
      .flatAggregate("top3(a)")
      .select("Func0(f0) as a, f1 as b")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testGroupedTableAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .groupBy('b % 5)
      .flatAggregate(top3('a))
      .select(Func0('f0) as 'a, 'f1 as 'b)

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
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)

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
  def testWithKeysAfterAlias(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .groupBy('b)
      .flatAggregate(top3('a) as ('d, 'e) withKeys 'd)
      .select('*)

    // String / Java API
    val resJava1 = t
      .groupBy("b")
      .flatAggregate("top3(a) as (d, e) withKeys d")
      .select("*")

    val resJava2 = t
      .groupBy("b")
      .flatAggregate("top3(a) as (d, e) withKeys(d)")
      .select("*")

    verifyTableEquals(resJava1, resScala)
    verifyTableEquals(resJava2, resScala)
  }

  @Test
  def testWithKeysWithoutAlias(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new Top3WithMapView
    util.tableEnv.registerFunction("top3", top3)
    util.tableEnv.registerFunction("Func0", Func0)

    // Expression / Scala API
    val resScala = t
      .groupBy('b)
      .flatAggregate(top3('a) withKeys 'f0)
      .select('*)

    // String / Java API
    val resJava1 = t
      .groupBy("b")
      .flatAggregate("top3(a) withKeys f0")
      .select("*")

    val resJava2 = t
      .groupBy("b")
      .flatAggregate("top3(a) withKeys(f0)")
      .select("*")

    verifyTableEquals(resJava1, resScala)
    verifyTableEquals(resJava2, resScala)
  }
}
