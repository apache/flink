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
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.TableTestBase

import org.junit.Test

class GroupWindowStringExpressionTest extends TableTestBase {

  @Test
  def testRowTimeSlide(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string, 'rowtime.rowtime)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Slide over 4.hours every 2.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select(
        'string,
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start,
        'w.end)

    // String / Java API
    val resJava = t
      .window(Slide.over("4.hours").every("2.hours").on("rowtime").as("w"))
      .groupBy("w, string")
      .select(
        "string, " +
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w)," +
        "end(w)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testRowTimeTumble(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, Long, String)]('int, 'long, 'rowtime.rowtime, 'string)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Tumble over 4.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select(
        'string,
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start,
        'w.end)

    // String / Java API
    val resJava = t
      .window(Tumble.over("4.hours").on("rowtime").as("w"))
      .groupBy("w, string")
      .select(
        "string, " +
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w)," +
        "end(w)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testRowTimeSession(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string, 'rowtime.rowtime)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Session withGap 4.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select(
        'string,
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start)

    // String / Java API
    val resJava = t
      .window(Session.withGap("4.hours").on("rowtime").as("w"))
      .groupBy("w, string")
      .select(
        "string, " +
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w)"
      )

    verifyTableEquals(resJava, resScala)
  }
  @Test
  def testProcTimeSlide(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string, 'proctime.proctime)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Slide over 4.hours every 2.hours on 'proctime as 'w)
      .groupBy('w)
      .select(
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start,
        'w.end)

    // String / Java API
    val resJava = t
      .window(Slide.over("4.hours").every("2.hours").on("proctime").as("w"))
      .groupBy("w")
      .select(
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w)," +
        "end(w)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testProcTimeTumble(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long,'string, 'proctime.proctime)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Tumble over 4.hours on 'proctime as 'w)
      .groupBy('w)
      .select(
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start,
        'w.end)

    // String / Java API
    val resJava = t
      .window(Tumble.over("4.hours").on("proctime").as("w"))
      .groupBy("w")
      .select(
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w)," +
        "end(w)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testProcTimeSession(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string, 'proctime.proctime)

    val myCountFun = new CountAggFunction
    util.tableEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .window(Session withGap 4.hours on 'proctime as 'w)
      .groupBy('w)
      .select(
        myCountFun('string),
        'int.sum,
        weightAvgFun('long, 'int),
        weightAvgFun('int, 'int) * 2,
        'w.start,
        'w.end)

    // String / Java API
    val resJava = t
      .window(Session.withGap("4.hours").on("proctime").as("w"))
      .groupBy("w")
      .select(
        "myCountFun(string), " +
        "int.sum, " +
        "weightAvgFun(long, int), " +
        "weightAvgFun(int, int) * 2, " +
        "start(w), " +
        "end(w)"
      )

    verifyTableEquals(resJava, resScala)
  }
}
