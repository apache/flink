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
package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class GroupWindowValidationTest extends TableTestBase {

  @Test
  def testInvalidWindowProperty(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('long, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .groupBy('string)
          // property in non windowed table
          .select('string, 'string.start))
      .hasMessageContaining("Window properties can only be used on windowed tables.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testGroupByWithoutWindowAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .window(Tumble.over(5.milli).on('long).as('w))
          .groupBy('string)
          .select('string, 'int.count))
      .hasMessageContaining("GroupBy must contain exactly one window alias.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidRowTimeRef(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .window(Tumble.over(5.milli).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count)
          .window(
            Slide.over(5.milli).every(1.milli).on('int).as('w2)
          ) // 'Int  does not exist in input.
          .groupBy('w2)
          .select('string))
      .hasMessageContaining("Cannot resolve field [int]")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidTumblingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .window(Tumble.over($"WRONG").on($"rowtime").as("w")) // string is not a valid interval
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining("A tumble window expects a size value literal")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidTumblingSizeType(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // row interval is not valid for session windows
          .window(Tumble.over(10).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining(
        "Tumbling window expects a size literal of a day-time interval or BIGINT type.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testTumbleUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .window(Tumble.over(2.hours).on('rowtime).as('w))
          .groupBy('w, 'string)
          // invalid UDAGG args
          .select('string, call(weightedAvg, 'string, 'int)))
      .hasMessageContaining("Invalid function call:\nmyWeightedAvg(STRING, INT)")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidSlidingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // field reference is not a valid interval
          .window(Slide.over($"WRONG").every($"WRONG").on($"rowtime").as("w"))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining("A sliding window expects a size value literal")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidSlidingSlide(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // row and time intervals may not be mixed
          .window(Slide.over(12.rows).every(1.minute).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining("A sliding window expects the same type of size and slide.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidSlidingSizeType(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // row interval is not valid for session windows
          .window(Slide.over(10).every(10.milli).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining(
        "A sliding window expects a size literal of a day-time interval or BIGINT type.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testSlideUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          .window(Slide.over(2.hours).every(30.minutes).on('rowtime).as('w))
          .groupBy('w, 'string)
          // invalid UDAGG args
          .select('string, call(weightedAvg, 'string, 'int)))
      .hasMessageContaining("Invalid function call:\nmyWeightedAvg(STRING, INT)")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidSessionGap(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // row interval is not valid for session windows
          .window(Session.withGap(10.rows).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining("A session window expects a gap literal of a day-time interval type.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidSessionGapType(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // row interval is not valid for session windows
          .window(Session.withGap(10).on('rowtime).as('w))
          .groupBy('w, 'string)
          .select('string, 'int.count))
      .hasMessageContaining("A session window expects a gap literal of a day-time interval type.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWindowAlias1(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // expression instead of a symbol
          .window(Session.withGap(100.milli).on('long).as(concat("A", "B")))
          .groupBy(concat("A", "B"))
          .select('string, 'int.count))
      .hasMessageContaining("Only unresolved reference supported for alias of a group window.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWindowAlias2(): Unit = {
    val util = streamTestUtil()
    val table = util.addDataStream[(Long, Int, String)]("T1", 'rowtime.rowtime, 'int, 'string)

    assertThatThrownBy(
      () =>
        table
          // field name "string" is already present
          .window(Session.withGap(100.milli).on('rowtime).as('string))
          .groupBy('string)
          .select('string, 'int.count))
      .hasMessageContaining("Cannot resolve field [string]")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testSessionUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table =
      util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'string, 'rowtime.rowtime)

    assertThatThrownBy(
      () =>
        table
          .window(Session.withGap(2.hours).on('rowtime).as('w))
          .groupBy('w, 'string)
          // invalid UDAGG args
          .select('string, call(weightedAvg, 'string, 'int)))
      .hasMessageContaining("Invalid function call:\nmyWeightedAvg(STRING, INT)")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWindowPropertyOnRowCountsTumblingWindow(): Unit = {
    val util = streamTestUtil()
    val table =
      util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'string, 'proctime.proctime)

    assertThatThrownBy(
      () =>
        table
          .window(Tumble.over(2.rows).on('proctime).as('w))
          .groupBy('w, 'string)
          // invalid start/end on rows-count window
          .select('string, 'w.start, 'w.end))
      .hasMessageContaining("Window start and Window end cannot be selected " +
        "for a row-count tumble window.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWindowPropertyOnRowCountsSlidingWindow(): Unit = {
    val util = streamTestUtil()
    val table =
      util.addDataStream[(Long, Int, String)]("T1", 'long, 'int, 'string, 'proctime.proctime)

    assertThatThrownBy(
      () =>
        table
          .window(Slide.over(10.rows).every(5.rows).on('proctime).as('w))
          .groupBy('w, 'string)
          // invalid start/end on rows-count window
          .select('string, 'w.start, 'w.end))
      .hasMessageContaining("Window start and Window end cannot be selected for a " +
        "row-count slide window.")
      .isInstanceOf[ValidationException]
  }
}
