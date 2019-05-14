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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Session, Slide, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class GroupWindowTableAggregateTest extends TableTestBase {

  val util = streamTestUtil()
  val table = util.addTable[(Long, Int, Long, Long)]('a, 'b, 'c, 'd.rowtime, 'e.proctime)
  val emptyFunc = new EmptyTableAggFunc

  @Test
  def testMultiWindow(): Unit = {
    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w1)
      .groupBy('w1, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w1.proctime as 'proctime, 'c, 'f0, 'f1 + 1 as 'f1)
      .window(Slide over 20.milli every 10.milli on 'proctime as 'w2)
      .groupBy('w2)
      .flatAggregate(emptyFunc('f0))
      .select('w2.start, 'f1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowTableAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(0),
              term("select", "a", "b", "c", "e")
            ),
            term("groupBy", "c"),
            term("window", "TumblingGroupWindow('w1, 'e, 50.millis)"),
            term("select", "c", "EmptyTableAggFunc(a, b) AS (f0, f1)", "proctime('w1) AS TMP_0")
          ),
          term("select", "TMP_0 AS proctime", "f0")
        ),
        term("window", "SlidingGroupWindow('w2, 'proctime, 20.millis, 10.millis)"),
        term("select", "EmptyTableAggFunc(f0) AS (f0, f1)", "start('w2) AS TMP_1")
      ),
      term("select", "TMP_1", "f1")
    )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {

    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w1)
      .groupBy('w1, 'b % 5 as 'bb)
      .flatAggregate(emptyFunc('a, 'b) as ('x, 'y))
      .select('w1.proctime as 'proctime, 'bb, 'x + 1, 'y)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "e", "MOD(b, 5) AS bb")
          ),
          term("groupBy", "bb"),
          term("window", "TumblingGroupWindow('w1, 'e, 50.millis)"),
          term("select", "bb", "EmptyTableAggFunc(a, b) AS (f0, f1)", "proctime('w1) AS TMP_0")
        ),
        term("select", "PROCTIME(TMP_0) AS proctime", "bb", "+(f0, 1) AS _c2", "f1 AS y")
      )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Tumble over 2.rows on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "c", "e")
        ),
        term("groupBy", "c"),
        term("window", "TumblingGroupWindow('w, 'e, 2.rows)"),
        term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "c", "d")
        ),
        term("groupBy", "c"),
        term("window", "TumblingGroupWindow('w, 'd, 5.millis)"),
        term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w.proctime as 'proctime, 'c, 'f0, 'f1 + 1)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "c", "e")
          ),
          term("groupBy", "c"),
          term("window", "SlidingGroupWindow('w, 'e, 50.millis, 50.millis)"),
          term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)", "proctime('w) AS TMP_0")
        ),
        term("select", "PROCTIME(TMP_0) AS proctime", "c", "f0", "+(f1, 1) AS _c3")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'e as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "c", "e")
        ),
        term("groupBy", "c"),
        term("window", "SlidingGroupWindow('w, 'e, 2.rows, 1.rows)"),
        term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "c", "d")
        ),
        term("groupBy", "c"),
        term("window", "SlidingGroupWindow('w, 'd, 8.millis, 10.millis)"),
        term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Session withGap 7.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('c, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "c", "d")
        ),
        term("groupBy", "c"),
        term("window", "SessionGroupWindow('w, 'd, 7.millis)"),
        term("select",  "c", "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "e")
        ),
        term("window", "TumblingGroupWindow('w, 'e, 50.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Tumble over 2.rows on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "e")
        ),
        term("window", "TumblingGroupWindow('w, 'e, 2.rows)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "d")
        ),
        term("window", "TumblingGroupWindow('w, 'd, 5.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )


    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 50.milli every 50.milli on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "e")
        ),
        term("window", "SlidingGroupWindow('w, 'e, 50.millis, 50.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'e as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "e")
        ),
        term("window", "SlidingGroupWindow('w, 'e, 2.rows, 1.rows)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "d")
        ),
        term("window", "SlidingGroupWindow('w, 'd, 8.millis, 10.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "d")
        ),
        term("window", "SlidingGroupWindow('w, 'd, 8.millis, 10.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val windowedTable = table
      .window(Session withGap 7.milli on 'd as 'w)
      .groupBy('w)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamGroupWindowTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a", "b", "d")
        ),
        term("window", "SessionGroupWindow('w, 'd, 7.millis)"),
        term("select",  "EmptyTableAggFunc(a, b) AS (f0, f1)")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "c", "d")
          ),
          term("groupBy", "c"),
          term("window", "TumblingGroupWindow('w, 'd, 5.millis)"),
          term("select",
            "c", "EmptyTableAggFunc(a, b) AS (f0, f1)", "start('w) AS TMP_0", "end('w) AS TMP_1")
        ),
        term("select", "f0", "+(f1, 1) AS _c1", "TMP_0", "TMP_1")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "c", "d")
          ),
          term("groupBy", "c"),
          term("window", "SlidingGroupWindow('w, 'd, 10.millis, 5.millis)"),
          term("select",
            "c", "EmptyTableAggFunc(a, b) AS (f0, f1)", "start('w) AS TMP_0", "end('w) AS TMP_1")
        ),
        term("select", "f0", "+(f1, 1) AS _c1", "TMP_0", "TMP_1")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val windowedTable = table
      .window(Session withGap 3.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w.end as 'we1, 'f0, 'f1 + 1, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "c", "d")
          ),
          term("groupBy", "c"),
          term("window", "SessionGroupWindow('w, 'd, 3.millis)"),
          term("select",
            "c", "EmptyTableAggFunc(a, b) AS (f0, f1)", "end('w) AS TMP_0", "start('w) AS TMP_1")
        ),
        term("select", "TMP_0 AS we1", "f0", "+(f1, 1) AS _c2", "TMP_1", "TMP_0")
      )

    util.verifyTable(windowedTable, expected)
  }
}
