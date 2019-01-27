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

package org.apache.flink.table.plan.batch.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.
  JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class GroupWindowTest extends TableTestBase {

  //===============================================================================================
  // Common test
  //===============================================================================================

  @Test(expected = classOf[RuntimeException])
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testLongEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.hours on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testTimestampEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.hours on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  //===============================================================================================
  // Sliding Windows
  //===============================================================================================

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testLongEventTimeSlidingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Slide over 1.hour every 10.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testTimestampEventTimeSlidingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Slide over 1.hour every 10.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  //===============================================================================================
  // Session Windows
  //===============================================================================================

  @Test(expected = classOf[RuntimeException])
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testEventTimeSessionGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testLongEventTimeSessionGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 30.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[RuntimeException])
  def testTimestampEventTimeSessionGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 30.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String, Long)]('rowtime, 'a, 'b, 'c)

    val windowedTable = table
      .window(Tumble over 15.minutes on 'rowtime as 'w)
      .groupBy('w)
      .select('c.varPop, 'c.varSamp, 'c.stddevPop, 'c.stddevSamp, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }
}
