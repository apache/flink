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
package org.apache.flink.table.planner.plan.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Session, Slide, Tumble, ValidationException, _}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class GroupWindowValidationTest extends TableTestBase {

  // ===============================================================================================
  // Common test
  // ===============================================================================================

  @Test
  def testGroupByWithoutWindowAlias(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Tumble.over(5.milli).on('long).as('w))
            .groupBy('string)
            .select('string, 'int.count))
  }

  @Test
  def testInvalidRowTimeRef(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Tumble.over(5.milli).on('long).as('w))
            .groupBy('w, 'string)
            .select('string, 'int.count)
            .window(
              Slide.over(5.milli).every(1.milli).on('int).as('w2)
            ) // 'Int  does not exist in input.
            .groupBy('w2)
            .select('string))
  }

  // ===============================================================================================
  // Tumbling Windows
  // ===============================================================================================

  @Test
  def testInvalidProcessingTimeDefinition(): Unit = {
    val util = batchTestUtil()
    // proctime is not allowed
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.addTableSource[(Int, String)]('proctime, 'int, 'string))
  }

  @Test
  def testInvalidProcessingTimeDefinition2(): Unit = {
    val util = batchTestUtil()
    // proctime is not allowed
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.addTableSource[(Long, Int, String)]('long, 'int, 'string, 'proctime))
  }

  @Test
  def testInvalidEventTimeDefinition(): Unit = {
    val util = batchTestUtil()
    // definition must not extend schema
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.addTableSource[(Long, Int, String)]('long, 'int, 'string, 'rowtime))
  }

  @Test
  def testTumblingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Tumble.over(2.minutes).on('rowtime).as('w))
            .groupBy('w, 'long)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }

  @Test
  def testAllTumblingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Tumble.over(2.minutes).on('rowtime).as('w))
            .groupBy('w)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }

  // ===============================================================================================
  // Sliding Windows
  // ===============================================================================================

  @Test
  def testSlidingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Slide.over(2.minutes).every(1.minute).on('rowtime).as('w))
            .groupBy('w, 'long)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }

  @Test
  def testAllSlidingGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Slide.over(2.minutes).every(1.minute).on('long).as('w))
            .groupBy('w)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }

  @Test
  def testSessionGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Session.withGap(2.minutes).on('rowtime).as('w))
            .groupBy('w, 'long)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }

  @Test
  def testAllSessionGroupWindowWithInvalidUdAggArgs(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .window(Session.withGap(2.minutes).on('rowtime).as('w))
            .groupBy('w)
            // invalid function arguments
            .select(myWeightedAvg('int, 'string)))
  }
}
