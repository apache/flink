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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.planner.utils.{CountMinMax, TableTestBase}

import org.junit.Test

class AggregateTest extends TableTestBase {

  @Test
  def testGroupDistinctAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('a.sum.distinct, 'c.count.distinct)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupDistinctAggregateWithUDAGG(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val weightedAvg = new WeightedAvg

    val resultTable = table
      .groupBy('c)
      .select(call(weightedAvg, 'a, 'b).distinct(), call(weightedAvg, 'a, 'b))

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregate() = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('a.count)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithConstant1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithConstant2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('a as 'a, 'b % 3 as 'd, 'c as 'c)
      .groupBy('d)
      .select('c.min, 'a.avg)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithAverage(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.cast(BasicTypeInfo.DOUBLE_TYPE_INFO).avg)

    util.verifyPlan(resultTable)
  }

  @Test
  def testDistinctAggregateOnTumbleWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Tumble over 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum)

    util.verifyPlan(result)
  }

  @Test
  def testMultiDistinctAggregateSameFieldOnHopWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Slide over 1.hour every 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum.distinct, 'a.max.distinct)

    util.verifyPlan(result)
  }

  @Test
  def testDistinctAggregateWithGroupingOnSessionWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Session withGap 15.minute on 'rowtime as 'w)
      .groupBy('a, 'w)
      .select('a, 'a.count, 'c.count.distinct)

    util.verifyPlan(result)
  }

  @Test
  def testSimpleAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b)
      .aggregate(testAgg('a))
      .select('b, 'f0, 'f1)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectStar(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b)
      .aggregate(testAgg('a))
      .select('*)

    util.verifyPlan(resultTable)
  }

  @Test
  def testAggregateWithScalarResult(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .aggregate('a.count)
      .select('b, 'TMP_0)

    util.verifyPlan(resultTable)
  }

  @Test
  def testAggregateWithAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b)
      .aggregate(testAgg('a) as ('x, 'y, 'z))
      .select('b, 'x, 'y)

    util.verifyPlan(resultTable)
  }
}
