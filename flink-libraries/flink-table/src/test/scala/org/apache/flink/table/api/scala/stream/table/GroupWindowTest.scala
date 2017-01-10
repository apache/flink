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
import org.apache.flink.table.api.scala._
import org.apache.flink.table._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.expressions.{RowtimeAttribute, WindowReference}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.junit.{Ignore, Test}

class GroupWindowTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowProperty(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
    .groupBy('string)
    .select('string, 'string.start) // property in non windowed table
  }

  @Test(expected = classOf[TableException])
  def testInvalidRowtime1(): Unit = {
    val util = streamTestUtil()
    // rowtime attribute must not be a field name
    util.addTable[(Long, Int, String)]('rowtime, 'long, 'int, 'string)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .select('string, 'int as 'rowtime) // rowtime attribute must not be an alias
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime3(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table.as('rowtime, 'myint, 'mystring) // rowtime attribute must not be an alias
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtime4(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      // only rowtime is a valid time attribute in a stream environment
      .window(Tumble over 50.milli on 'string as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidTumblingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Tumble over "WRONG" as 'w) // string is not a valid interval
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSize(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Slide over "WRONG" every "WRONG" as 'w) // string is not a valid interval
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSlidingSlide(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Slide over 12.rows every 1.minute as 'w) // row and time intervals may not be mixed
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidSessionGap(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Session withGap 10.rows as 'w) // row interval is not valid for session windows
      .groupBy('w, 'string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Session withGap 100.milli as 1 + 1) // expression instead of a symbol
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowAlias2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    table
      .window(Session withGap 100.milli as 'string) // field name "string" is already present
      .groupBy('string)
      .select('string, 'int.count)
  }

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 50.milli as 'w1)
      .groupBy('w1, 'string)
      .select('string, 'int.count)
      .window(Slide over 20.milli every 10.milli as 'w2)
      .groupBy('w2)
      .select('string.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "string", "int")
          ),
          term("groupBy", "string"),
          term(
            "window",
            ProcessingTimeTumblingGroupWindow(
              Some(WindowReference("w1")),
              50.milli)),
          term("select", "string", "COUNT(int) AS TMP_0")
        ),
        term("select", "string")
      ),
      term(
        "window",
        ProcessingTimeSlidingGroupWindow(
          Some(WindowReference("w2")),
          20.milli, 10.milli)),
      term("select", "COUNT(string) AS TMP_2")
    )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 50.milli as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        ProcessingTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        ProcessingTimeTumblingGroupWindow(
          Some(WindowReference("w")), 2.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        EventTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(),
          5.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamAggregate
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        EventTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 2.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 50.milli every 50.milli as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        ProcessingTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          50.milli, 50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        ProcessingTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          2.rows, 1.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        EventTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 8.milli, 10.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamAggregate
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      streamTableNode(0),
      term("groupBy", "string"),
      term(
        "window",
        EventTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 2.rows, 1.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        EventTimeSessionGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 7.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 50.milli as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term(
        "window",
        ProcessingTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          50.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        ProcessingTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          2.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        EventTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 5.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamAggregate
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        EventTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 2.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }


  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 50.milli every 50.milli as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        ProcessingTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          50.milli, 50.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        ProcessingTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          2.rows, 1.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        EventTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 8.milli, 10.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamAggregate
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        EventTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 2.rows, 1.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "int")
      ),
      term(
        "window",
        EventTimeSessionGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(), 7.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term("window",
        EventTimeTumblingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(),
          5.milli)),
      term("select",
        "string",
        "COUNT(int) AS TMP_0",
        "start(WindowReference(w)) AS TMP_1",
        "end(WindowReference(w)) AS TMP_2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "string", "int")
      ),
      term("groupBy", "string"),
      term("window",
        EventTimeSlidingGroupWindow(
          Some(WindowReference("w")),
          RowtimeAttribute(),
          10.milli,
          5.milli)),
      term("select",
        "string",
        "COUNT(int) AS TMP_0",
        "start(WindowReference(w)) AS TMP_1",
        "end(WindowReference(w)) AS TMP_2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 3.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('w.end as 'we1, 'string, 'int.count as 'cnt, 'w.start as 'ws, 'w.end as 'we2)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "string", "int")
        ),
        term("groupBy", "string"),
        term("window",
          EventTimeSessionGroupWindow(
            Some(WindowReference("w")),
            RowtimeAttribute(),
            3.milli)),
        term("select",
          "string",
          "COUNT(int) AS TMP_1",
          "end(WindowReference(w)) AS TMP_0",
          "start(WindowReference(w)) AS TMP_2")
      ),
      term("select", "TMP_0 AS we1", "string", "TMP_1 AS cnt", "TMP_2 AS ws", "TMP_0 AS we2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowWithDuplicateAggsAndProps(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.sum + 1 as 's1, 'int.sum + 3 as 's2, 'w.start as 'x, 'w.start as 'x2,
        'w.end as 'x3, 'w.end)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "string", "int")
        ),
        term("groupBy", "string"),
        term("window",
          EventTimeTumblingGroupWindow(
            Some(WindowReference("w")),
            RowtimeAttribute(),
            5.millis)),
        term("select",
          "string",
          "SUM(int) AS TMP_0",
          "start(WindowReference(w)) AS TMP_1",
          "end(WindowReference(w)) AS TMP_2")
      ),
      term("select",
        "string",
        "+(CAST(AS(TMP_0, 'TMP_3')), CAST(1)) AS s1",
        "+(CAST(AS(TMP_0, 'TMP_4')), CAST(3)) AS s2",
        "TMP_1 AS x",
        "TMP_1 AS x2",
        "TMP_2 AS x3",
        "TMP_2 AS TMP_5")
    )

    util.verifyTable(windowedTable, expected)
  }
}
