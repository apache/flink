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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.{OverAgg0, WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.WindowReference
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.junit.{Ignore, Test}

class GroupWindowTest extends TableTestBase {

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testOverAggregation(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val overAgg = new OverAgg0
    table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select(overAgg('long, 'int))
  }

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

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'string, 'int.count)
      .window(Slide over 20.milli every 10.milli on 'proctime as 'w2)
      .groupBy('w2)
      .select('string.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "string", "int", "proctime")
          ),
          term("groupBy", "string"),
          term(
            "window",
            TumblingGroupWindow(
              WindowReference("w1"),
              'proctime,
              50.milli)),
          term("select", "string", "COUNT(int) AS TMP_1", "proctime('w1) AS TMP_0")
        ),
        term("select", "string", "TMP_0 AS proctime")
      ),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w2"),
          'proctime,
          20.milli,
          10.milli)),
      term("select", "COUNT(string) AS TMP_2")
    )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "proctime")
      ),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'proctime,
          50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "proctime")
      ),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(WindowReference("w"), 'proctime, 2.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'long,
          5.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'rowtime,
          5.milli)),
      term("select", "string", "WeightedAvgWithMerge(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "proctime")
      ),
      term("groupBy", "string"),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w"),
          'proctime,
          50.milli,
          50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "proctime")
      ),
      term("groupBy", "string"),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w"),
          'proctime,
          2.rows,
          1.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 8.milli, 10.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w"),
          'long,
          8.milli,
          10.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 8.milli, 10.milli)),
      term("select", "string", "WeightedAvgWithMerge(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'long, 7.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'rowtime, 7.milli)),
      term("select", "string", "WeightedAvgWithMerge(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.milli on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "proctime")
      ),
      term("groupBy", "string"),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'proctime,
          50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "proctime")
      ),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'proctime,
          2.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "rowtime")
      ),
      term("window", TumblingGroupWindow(WindowReference("w"), 'rowtime, 5.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "long")
      ),
      term(
        "window",
        TumblingGroupWindow(
          WindowReference("w"),
          'long,
          5.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "proctime")
      ),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w"),
          'proctime,
          50.milli,
          50.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "proctime")
      ),
      term(
        "window",
        SlidingGroupWindow(
          WindowReference("w"),
          'proctime,
          2.rows,
          1.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "rowtime")
      ),
      term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 8.milli, 10.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
//  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "long")
      ),
      term("window", SlidingGroupWindow(WindowReference("w"), 'long, 8.milli, 10.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int", "long")
      ),
      term(
        "window",
        SessionGroupWindow(
          WindowReference("w"),
          'long,
          7.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'rowtime, 5.milli)),
      term("select",
        "string",
        "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1",
        "end('w) AS TMP_2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlidingWindowWithUDAF(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String, Int, Int)](
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
      .select(weightAvgFun('long, 'int))

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          streamTableNode(0),
          term("groupBy", "string, int2, int3"),
          term("window", SlidingGroupWindow(WindowReference("w"), 'proctime,  2.rows, 1.rows)),
          term(
            "select",
            "string",
            "int2",
            "int3",
            "WeightedAvg(long, int) AS TMP_0")
        ),
        term("select","TMP_0")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", SlidingGroupWindow(WindowReference("w"), 'rowtime, 10.milli, 5.milli)),
      term("select",
        "string",
        "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1",
        "end('w) AS TMP_2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 3.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('w.end as 'we1, 'string, 'int.count as 'cnt, 'w.start as 'ws, 'w.end as 'we2)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(0),
        term("groupBy", "string"),
        term("window", SessionGroupWindow(WindowReference("w"), 'long, 3.milli)),
        term("select",
          "string",
          "COUNT(int) AS TMP_1",
          "end('w) AS TMP_0",
          "start('w) AS TMP_2")
      ),
      term("select", "TMP_0 AS we1", "string", "TMP_1 AS cnt", "TMP_2 AS ws", "TMP_0 AS we2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowWithDuplicateAggsAndProps(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.sum + 1 as 's1, 'int.sum + 3 as 's2, 'w.start as 'x, 'w.start as 'x2,
        'w.end as 'x3, 'w.end)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(0),
        term("groupBy", "string"),
        term("window", TumblingGroupWindow(WindowReference("w"), 'long, 5.millis)),
        term("select",
          "string",
          "SUM(int) AS TMP_0",
          "start('w) AS TMP_1",
          "end('w) AS TMP_2")
      ),
      term("select",
        "string",
        "+(TMP_0, 1) AS s1",
        "+(TMP_0, 3) AS s2",
        "TMP_1 AS x",
        "TMP_1 AS x2",
        "TMP_2 AS x3",
        "TMP_2")
    )

    util.verifyTable(windowedTable, expected)
  }
}
