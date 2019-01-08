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
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class AggregateStringExpressionTest extends TableTestBase {

  @Test
  def testGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string)

    val weightAvgFun = new WeightedAvg
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)

    // Expression / Scala API
    val resScala = t
      .groupBy('string)
      .select('int.count as 'cnt, weightAvgFun('long, 'int))

    // String / Java API
    val resJava = t
      .groupBy("string")
      .select("int.count as cnt, weightAvgFun(long, int)")

    verifyTableEquals(resJava, resScala)
  }

  @Test
  def testNonGroupedAggregate(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('int, 'long, 'string)

    // Expression / Scala API
    val resScala = t.select('int.count as 'cnt, 'long.sum)

    // String / Java API
    val resJava = t.select("int.count as cnt, long.sum")

    verifyTableEquals(resJava, resScala)
  }
}
