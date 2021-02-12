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
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithRetract
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class OverAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  val table: Table = streamUtil.addDataStream[(Int, String, Long)]("MyTable",
    'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testScalarFunctionsOnOverWindow(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract
    val plusOne = Func1

    val result = table
      .window(Over partitionBy 'b orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select(
        plusOne('a.sum over 'w as 'wsum) as 'd,
        ('a.count over 'w).exp(),
        (call(weightedAvg, 'c, 'a) over 'w) + 1,
        "AVG:".toExpr + (call(weightedAvg, 'c, 'a) over 'w),
        array(call(weightedAvg, 'c, 'a) over 'w, 'a.count over 'w))
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'b orderBy 'proctime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, call(weightedAvg, 'c, 'a) over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'a orderBy 'proctime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, call(weightedAvg, 'c, 'a) over 'w as 'myAvg)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRangeOver(): Unit = {
    val result = table
      .window(Over orderBy 'proctime preceding 10.second as 'w)
      .select('a, 'c.count over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over orderBy 'proctime preceding 2.rows as 'w)
      .select('c, 'a.count over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeUnboundedPartitionedRangeOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w)

    val result2 = table
      .window(Over partitionBy 'c orderBy 'proctime as 'w)
      .select('a, 'c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w)

    verifyTableEquals(result, result2)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW following CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val result = table
      .window(
        Over orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c, 'a.count over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'b orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'b.count over 'w, call(weightedAvg, 'c, 'a) over 'w as 'wAvg)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'a orderBy 'rowtime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, 'c.avg over 'w, call(weightedAvg, 'c, 'a) over 'w as 'wAvg)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver(): Unit = {
    val result = table
      .window(Over orderBy 'rowtime preceding 10.second as 'w)
      .select('a, 'c.count over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over orderBy 'rowtime preceding 2.rows as 'w)
      .select('c, 'a.count over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_RANGE following
         CURRENT_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w as 'wAvg)

    val result2 = table
      .window(Over partitionBy 'c orderBy 'rowtime as 'w)
      .select('a, 'c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w as 'wAvg)

    verifyTableEquals(result, result2)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedDistinctWithPartitionedRangeOver(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('c,
        'a.count.distinct over 'w,
        'a.sum.distinct over 'w,
        ('a.cast(DataTypes.FLOAT) as 'a).avg.distinct over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedDistinctWithPartitionedRangeOver(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
      .select('c,
        'a.count.distinct over 'w,
        'a.sum.distinct over 'w,
        ('a.cast(DataTypes.FLOAT) as 'a).avg.distinct over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeBoundedDistinctWithPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c,
        'a.count.distinct over 'w,
        'a.sum.distinct over 'w,
        ('a.cast(DataTypes.FLOAT) as 'a).avg.distinct over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedDistinctWithPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_ROW following
         CURRENT_ROW as 'w)
      .select('c,
        'a.count.distinct over 'w,
        'a.sum.distinct over 'w,
        ('a.cast(DataTypes.FLOAT) as 'a).avg.distinct over 'w)

    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedPartitionedRowsOver(): Unit = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_ROW following
         CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w, call(weightedAvg, 'c, 'a) over 'w as 'wAvg)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val result = table
      .window(
        Over orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)
    streamUtil.verifyExecPlan(result)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val result = table
      .window(Over orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
      .select('c, 'a.count over 'w)

    streamUtil.verifyExecPlan(result)
  }
}


