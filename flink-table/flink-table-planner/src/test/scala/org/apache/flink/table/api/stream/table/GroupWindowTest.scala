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
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.WindowReference
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.{Ignore, Test}

class GroupWindowTest extends TableTestBase {

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
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
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
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
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
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
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

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String, Long)]('rowtime.rowtime, 'a, 'b, 'c)

    val windowedTable = table
      .window(Tumble over 15.minutes on 'rowtime as 'w)
      .groupBy('w)
      .select('c.varPop, 'c.varSamp, 'c.stddevPop, 'c.stddevSamp, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "c", "rowtime",
              "*(c, c) AS $f2", "*(c, c) AS $f3", "*(c, c) AS $f4", "*(c, c) AS $f5")
          ),
          term("window", TumblingGroupWindow('w, 'rowtime, 900000.millis)),
          term("select",
            "SUM($f2) AS $f0",
            "SUM(c) AS $f1",
            "COUNT(c) AS $f2",
            "SUM($f3) AS $f3",
            "SUM($f4) AS $f4",
            "SUM($f5) AS $f5",
            "start('w) AS TMP_4",
            "end('w) AS TMP_5")
        ),
        term("select",
          "CAST(/(-($f0, /(*($f1, $f1), $f2)), $f2)) AS TMP_0",
          "CAST(/(-($f3, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null, -($f2, 1)))) AS TMP_1",
          "CAST(POWER(/(-($f4, /(*($f1, $f1), $f2)), $f2), 0.5)) AS TMP_2",
          "CAST(POWER(/(-($f5, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null, -($f2, 1))), 0.5)) " +
            "AS TMP_3",
          "TMP_4",
          "TMP_5")
      )

    util.verifyTable(windowedTable, expected)
  }
}
