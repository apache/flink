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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class GroupWindowValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowProperty(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .groupBy('string)
      .select('string, 'string.start) // property in non windowed table
  }

  @Test(expected = classOf[ValidationException])
  def testGroupByWithoutWindowAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowTimeRef(): Unit = {
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

  @Test(expected = classOf[ValidationException])
  def testInvalidTumblingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Tumble over "WRONG" on 'long as 'w) // string is not a valid interval
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testTumbleUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over 2.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Slide over "WRONG" every "WRONG" on 'long as 'w) // string is not a valid interval
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSlide(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row and time intervals may not be mixed
      .window(Slide over 12.rows every 1.minute on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testSlideUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Slide over 2.hours every 30.minutes on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSessionGap(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // row interval is not valid for session windows
      .window(Session withGap 10.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      .window(Session withGap 100.milli on 'long as 1 + 1) // expression instead of a symbol
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    table
      // field name "string" is already present
      .window(Session withGap 100.milli on 'long as 'string)
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testSessionUdAggWithInvalidArgs(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvgWithMerge
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    table
      .window(Session withGap 2.hours on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('string, 'int)) // invalid UDAGG args
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowPropertyOnRowCountsTumblingWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
    .window(Tumble over 2.rows on 'proctime as 'w)
    .groupBy('w, 'string)
    .select('string, 'w.start, 'w.end) // invalid start/end on rows-count window
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowPropertyOnRowCountsSlidingWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    table
    .window(Slide over 10.rows every 5.rows on 'proctime as 'w)
    .groupBy('w, 'string)
    .select('string, 'w.start, 'w.end) // invalid start/end on rows-count window
  }
}
