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

package org.apache.flink.table.api.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.TableTestBase

import org.junit.Test

class GroupWindowValidationTest extends TableTestBase {

  //===============================================================================================
  // Common test
  //===============================================================================================

  @Test(expected = classOf[ValidationException])
  def testGroupByWithoutWindowAlias(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowTimeRef(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
      .window(Slide over 5.milli every 1.milli on 'int as 'w2) // 'Int  does not exist in input.
      .groupBy('w2)
      .select('string)
  }

  //===============================================================================================
  // Tumbling Windows
  //===============================================================================================

  @Test(expected = classOf[ValidationException])
  def testInvalidProcessingTimeDefinition(): Unit = {
    val util = batchTestUtil()
    // proctime is not allowed
    util.addTable[(Long, Int, String)]('long.proctime, 'int, 'string)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidProcessingTimeDefinition2(): Unit = {
    val util = batchTestUtil()
    // proctime is not allowed
    util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidEventTimeDefinition(): Unit = {
    val util = batchTestUtil()
    // definition must not extend schema
    util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testTumblingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Tumble over 2.minutes on 'rowtime as 'w)
      .groupBy('w, 'long)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }

  @Test(expected = classOf[ValidationException])
  def testAllTumblingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Tumble over 2.minutes on 'rowtime as 'w)
      .groupBy('w)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }

  //===============================================================================================
  // Sliding Windows
  //===============================================================================================

  @Test(expected = classOf[ValidationException])
  def testSlidingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Slide over 2.minutes every 1.minute on 'rowtime as 'w)
      .groupBy('w, 'long)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }

  @Test(expected = classOf[ValidationException])
  def testAllSlidingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Slide over 2.minutes every 1.minute on 'long as 'w)
      .groupBy('w)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }

  @Test(expected = classOf[ValidationException])
  def testSessionGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Session withGap 2.minutes on 'rowtime as 'w)
      .groupBy('w, 'long)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }

  @Test(expected = classOf[ValidationException])
  def testAllSessionGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    table
      .window(Session withGap 2.minutes on 'rowtime as 'w)
      .groupBy('w)
      // invalid function arguments
      .select(myWeightedAvg('int, 'string))
  }
}
