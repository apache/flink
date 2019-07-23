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

package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Slide, TableException, Tumble}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

import java.sql.Timestamp

class GroupWindowTest extends TableTestBase {

  //===============================================================================================
  // Common test
  //===============================================================================================

  @Test(expected = classOf[TableException])
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    util.verifyPlan(windowedTable)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)
    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[TableException])
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testLongEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.hours on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    util.verifyPlan(windowedTable)
  }

  @Test
  def testTimestampEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Timestamp, Int, String)]('ts, 'int, 'string)

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
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

  @Test(expected = classOf[TableException])
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    util.verifyPlan(windowedTable)
  }

}
