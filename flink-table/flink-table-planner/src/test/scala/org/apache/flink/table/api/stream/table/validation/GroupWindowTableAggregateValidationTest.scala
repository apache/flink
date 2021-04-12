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
package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.{TableTestBase, Top3}

import org.junit.Test

class GroupWindowTableAggregateValidationTest extends TableTestBase {

  val top3 = new Top3
  val weightedAvg = new WeightedAvgWithMerge

  val util = streamTestUtil()
  val table = util.addTable[(Long, Int, String)](
    'long, 'int, 'string, 'rowtime.rowtime, 'proctime.proctime)

  @Test
  def testGroupByWithoutWindowAlias(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("GroupBy must contain exactly one window alias.")

    table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('string)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidRowTimeRef(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Cannot resolve field [int], input field list:[string, EXPR$0].")

    table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
      .window(Slide over 5.milli every 1.milli on 'int as 'w2) // 'Int  does not exist in input.
      .groupBy('string, 'w2)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidTumblingSize(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A tumble window expects a size value literal")

    table
      .window(Tumble over "WRONG" on 'rowtime as 'w) // string is not a valid interval
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidTumblingSizeType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Tumbling window expects a size literal of a day-time interval or BIGINT type.")

    table
      // row interval is not valid for session windows
      .window(Tumble over 10 on 'rowtime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testTumbleUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long) \nExpected: (int)")

    table
      .window(Tumble over 2.hours on 'rowtime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('long)) // invalid args
      .select('string, 'f0)
  }

  @Test
  def testInvalidSlidingSize(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A sliding window expects a size value literal")

    table
      // field reference is not a valid interval
      .window(Slide over "WRONG" every "WRONG" on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidSlidingSlide(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A sliding window expects the same type of size and slide.")

    table
      // row and time intervals may not be mixed
      .window(Slide over 12.rows every 1.minute on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidSlidingSizeType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A sliding window expects a size literal of a day-time interval or BIGINT type.")

    table
      // row and time intervals may not be mixed
      .window(Slide over 10 every 10.milli  on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testSlideUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long) \nExpected: (int)")

    table
      .window(Slide over 2.hours every 30.minutes on 'rowtime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('long)) // invalid args
      .select('string, 'f0)
  }

  @Test
  def testInvalidSessionGap(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A session window expects a gap literal of a day-time interval type.")

    table
      // row interval is not valid for session windows
      .window(Session withGap 10.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidSessionGapType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A session window expects a gap literal of a day-time interval type.")

    table
      // row interval is not valid for session windows
      .window(Session withGap 10 on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidWindowAliasWithExpression(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Only unresolved reference supported for alias of a " +
      "group window.")

    table
      // expression instead of a symbol
      .window(Session withGap 100.milli on 'proctime as concat("A", "B"))
      .groupBy(concat("A", "B"))
      .flatAggregate(top3('int))
      .select('f0, 'f1)
  }

  @Test
  def testInvalidWindowAlias(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [string], input field list:[f0, f1].")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Session withGap 100.milli on 'long as 'string)
      .groupBy('string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1)
  }

  @Test
  def testSessionUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.Long) \nExpected: (int)")

    table
      .window(Session withGap 2.hours on 'rowtime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('long)) // invalid args
      .select('string, 'f1)
  }

  @Test
  def testInvalidWindowPropertyOnRowCountsWindow(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Window start and Window end cannot be selected " +
      "for a row-count tumble window.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'w.start, 'w.end) // invalid start/end on rows-count window
  }

  @Test
  def testInvalidAggregateInSelection(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Aggregate functions cannot be used in the select " +
      "right after the flatAggregate.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('string, 'f0.count)
  }

  @Test
  def testInvalidStarInSelection(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Can not use * for window aggregate!")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .flatAggregate(top3('int))
      .select('*)
  }
}
