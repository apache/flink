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
package org.apache.flink.table.api.scala.stream.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.api.java.{Slide => JSlide}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test


class GroupWindowStringExpressionTest extends TableTestBase {

  @Test
  def testJavaScalaTableAPIEquality(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string, 'rowtime.rowtime)

    val myCountFun = new CountAggFunction
    util.tEnv.registerFunction("myCountFun", myCountFun)
    val weightAvgFun = new WeightedAvg
    util.tEnv.registerFunction("weightAvgFun", weightAvgFun)

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
      .window(JSlide.over("4.hours").every("2.hours").on("rowtime").as("w"))
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
}
