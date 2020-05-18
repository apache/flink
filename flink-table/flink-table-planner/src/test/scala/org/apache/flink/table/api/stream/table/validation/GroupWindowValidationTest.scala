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
import org.apache.flink.table.utils.{CountMinMax, TableTestBase}

import org.junit.Test

class GroupWindowValidationTest extends TableTestBase {

  @Test
  def testInvalidWindowProperty(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Window properties can only be used on windowed tables.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .select('string, 'string.start) // property in non windowed table
  }

  @Test
  def testGroupByWithoutWindowAlias(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("GroupBy must contain exactly one window alias.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidRowTimeRef(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [int]")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
      .window(Slide over 5.milli every 1.milli on 'int as 'w2) // 'Int  does not exist in input.
      .groupBy('w2)
      .select('string)
  }

  @Test
  def testInvalidTumblingSize(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A tumble window expects a size value literal")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over "WRONG" on 'long as 'w) // string is not a valid interval
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidTumblingSizeType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Tumbling window expects a size literal of a day-time interval or BIGINT type.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row interval is not valid for session windows
      .window(Tumble over 10 on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testTumbleUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.String, java.lang.Integer) \nExpected: (int, int), (long, int), " +
      "(long, int, int, java.lang.String)")

    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over 2.hours on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test
  def testInvalidSlidingSize(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A sliding window expects a size value literal")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // field reference is not a valid interval
      .window(Slide over "WRONG" every "WRONG" on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidSlidingSlide(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("A sliding window expects the same type of size and slide.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row and time intervals may not be mixed
      .window(Slide over 12.rows every 1.minute on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidSlidingSizeType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A sliding window expects a size literal of a day-time interval or BIGINT type.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row interval is not valid for session windows
      .window(Slide over 10 every 10.milli  on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testSlideUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.String, java.lang.Integer) \nExpected: (int, int), (long, int), " +
      "(long, int, int, java.lang.String)")

    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Slide over 2.hours every 30.minutes on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test
  def testInvalidSessionGap(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A session window expects a gap literal of a day-time interval type.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row interval is not valid for session windows
      .window(Session withGap 10.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidSessionGapType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "A session window expects a gap literal of a day-time interval type.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row interval is not valid for session windows
      .window(Session withGap 10 on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test
  def testInvalidWindowAlias1(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Only unresolved reference supported for alias of a " +
      "group window.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // expression instead of a symbol
      .window(Session withGap 100.milli on 'long as concat("A", "B"))
      .groupBy(concat("A", "B"))
      .select('string, 'int.count)
  }

  @Test
  def testInvalidWindowAlias2(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [string]")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // field name "string" is already present
      .window(Session withGap 100.milli on 'long as 'string)
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test
  def testSessionUdAggWithInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters do not match any signature. \n" +
      "Actual: (java.lang.String, java.lang.Integer) \nExpected: (int, int), (long, int), " +
      "(long, int, int, java.lang.String)")

    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    table
      .window(Session withGap 2.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test
  def testInvalidWindowPropertyOnRowCountsTumblingWindow(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Window start and Window end cannot be selected " +
      "for a row-count tumble window.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
    .window(Tumble over 2.rows on 'proctime as 'w)
    .groupBy('w, 'string)
    .select('string, 'w.start, 'w.end) // invalid start/end on rows-count window
  }

  @Test
  def testInvalidWindowPropertyOnRowCountsSlidingWindow(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Window start and Window end cannot be selected for a " +
      "row-count slide window.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
    .window(Slide over 10.rows every 5.rows on 'proctime as 'w)
    .groupBy('w, 'string)
    .select('string, 'w.start, 'w.end) // invalid start/end on rows-count window
  }

  @Test
  def testInvalidAggregateInSelection(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Aggregate functions cannot be used in the select " +
      "right after the aggregate.")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)
    val testAgg = new CountMinMax

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .aggregate(testAgg('int))
      .select('string, 'f0.count)
  }

  @Test
  def testInvalidStarInSelection(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Can not use * for window aggregate!")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)
    val testAgg = new CountMinMax

    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('string, 'w)
      .aggregate(testAgg('int))
      .select('*)
  }
}
