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
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableTestBase}

import org.junit.Test

import java.sql.Timestamp

class GroupWindowTest extends TableTestBase {

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'string, 'int.count)
      .window(Slide over 20.millis every 10.millis on 'proctime as 'w2)
      .groupBy('w2)
      .select('string.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1",'long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, call(weightedAvg, 'long, 'int))
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.millis every 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSlidingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, call(weightedAvg, 'long, 'int))
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSessionGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, call(weightedAvg, 'long, 'int))
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.millis every 50.millis on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testSlidingWindowWithUDAF(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String, Int, Int)](
      "T1",
      'long,
      'int,
      'string,
      'int2,
      'int3,
      'proctime.proctime)

    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .select(call(weightAvgFun, 'long, 'int))

    util.verifyPlan(windowedTable)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 10.millis every 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 3.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('w.end as 'we1, 'string, 'int.count as 'cnt, 'w.start as 'ws, 'w.end as 'we2)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testTumbleWindowWithDuplicateAggsAndProps(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)](
      "T1", 'rowtime.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.sum + 1 as 's1, 'int.sum + 3 as 's2, 'w.start as 'x, 'w.start as 'x2,
        'w.end as 'x3, 'w.end)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String, Long)](
      "T1", 'rowtime.rowtime, 'a, 'b, 'c)

    val windowedTable = table
      .window(Tumble over 15.minutes on 'rowtime as 'w)
      .groupBy('w)
      .select('c.varPop, 'c.varSamp, 'c.stddevPop, 'c.stddevSamp, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testWindowAggregateWithDifferentWindows(): Unit = {
    // This test ensures that the LogicalWindowTableAggregate node's digest contains the window
    // specs. This allows the planner to make the distinction between similar aggregations using
    // different windows (see FLINK-15577).
    val util = streamTestUtil()
    val table = util.addTableSource[(Timestamp, Long, Int)]('ts.rowtime, 'a, 'b)
    val emptyFunc = new EmptyTableAggFunc

    val tableWindow1hr = table
      .window(Slide over 1.hour every 1.hour on 'ts as 'w1)
      .groupBy('w1)
      .flatAggregate(emptyFunc('a, 'b))
      .select(1)

    val tableWindow2hr = table
      .window(Slide over 2.hour every 1.hour on 'ts as 'w1)
      .groupBy('w1)
      .flatAggregate(emptyFunc('a, 'b))
      .select(1)

    val unionTable = tableWindow1hr.unionAll(tableWindow2hr)

    util.verifyPlan(unionTable)
  }
}
