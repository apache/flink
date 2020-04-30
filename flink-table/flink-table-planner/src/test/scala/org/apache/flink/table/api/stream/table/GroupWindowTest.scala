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
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Ignore, Test}

class GroupWindowTest extends TableTestBase {

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w1)
      .groupBy('w1, 'string)
      .select('w1.proctime as 'proctime, 'string, 'int.count)
      .window(Slide over 20.millis every 10.millis on 'proctime as 'w2)
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
            streamTableNode(table),
            term("select", "int", "string", "proctime")
          ),
          term("groupBy", "string"),
          term("window", "TumblingGroupWindow('w1, 'proctime, 50.millis)"),
          term("select", "string", "COUNT(int) AS EXPR$1", "proctime('w1) AS EXPR$0")
        ),
        term("select", "EXPR$0 AS proctime", "string")
      ),
      term("window", "SlidingGroupWindow('w2, 'proctime, 20.millis, 10.millis)"),
      term("select", "COUNT(string) AS EXPR$0")
    )
    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "proctime")
      ),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'proctime, 50.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
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
        streamTableNode(table),
        term("select", "int", "string", "proctime")
      ),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'proctime, 2)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'rowtime, 5.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.millis every 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "proctime")
      ),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'proctime, 50.millis, 50.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
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
        streamTableNode(table),
        term("select", "int", "string", "proctime")
      ),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'proctime, 2, 1)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'rowtime, 8.millis, 10.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'long, 8.millis, 10.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'rowtime, 8.millis, 10.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'long, 7.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowWithUdAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val weightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, weightedAvg('long, 'int))

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'rowtime, 7.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Tumble over 50.millis on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "proctime")
      ),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'proctime, 50.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
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
        streamTableNode(table),
        term("select", "int", "proctime")
      ),
      term("window", "TumblingGroupWindow('w, 'proctime, 2)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "rowtime")
      ),
      term("window", "TumblingGroupWindow('w, 'rowtime, 5.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "long")
      ),
      term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllProcessingTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'proctime.proctime)

    val windowedTable = table
      .window(Slide over 50.millis every 50.millis on 'proctime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "proctime")
      ),
      term("window", "SlidingGroupWindow('w, 'proctime, 50.millis, 50.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
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
        streamTableNode(table),
        term("select", "int", "proctime")
      ),
      term("window", "SlidingGroupWindow('w, 'proctime, 2, 1)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "rowtime")
      ),
      term("window", "SlidingGroupWindow('w, 'rowtime, 8.millis, 10.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
//  @Ignore // see comments in DataStreamGroupWindowAggregate
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "SlidingGroupWindow('w, 'long, 8.millis, 10.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "SessionGroupWindow('w, 'long, 7.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTumbleWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'rowtime, 5.millis)"),
      term("select",
        "string",
        "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1",
        "end('w) AS EXPR$2")
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
          streamTableNode(table),
          term("groupBy", "string, int2, int3"),
          term("window", "SlidingGroupWindow('w, 'proctime, 2, 1)"),
          term(
            "select",
            "string",
            "int2",
            "int3",
            "WeightedAvg(long, int) AS EXPR$0")
        ),
        term("select","EXPR$0")
      )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSlideWindowStartEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Slide over 10.millis every 5.millis on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "int", "string", "rowtime")
      ),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'rowtime, 10.millis, 5.millis)"),
      term("select",
        "string",
        "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1",
        "end('w) AS EXPR$2")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testSessionWindowStartWithTwoEnd(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('long.rowtime, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 3.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('w.end as 'we1, 'string, 'int.count as 'cnt, 'w.start as 'ws, 'w.end as 'we2)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(table),
        term("groupBy", "string"),
        term("window", "SessionGroupWindow('w, 'long, 3.millis)"),
        term("select",
          "string",
          "COUNT(int) AS EXPR$1",
          "end('w) AS EXPR$0",
          "start('w) AS EXPR$2")
      ),
      term("select", "EXPR$0 AS we1", "string", "EXPR$1 AS cnt", "EXPR$2 AS ws", "EXPR$0 AS we2")
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
        streamTableNode(table),
        term("groupBy", "string"),
        term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
        term("select",
          "string",
          "SUM(int) AS EXPR$0",
          "start('w) AS EXPR$1",
          "end('w) AS EXPR$2")
      ),
      term("select",
        "string",
        "+(EXPR$0, 1) AS s1",
        "+(EXPR$0, 3) AS s2",
        "EXPR$1 AS x",
        "EXPR$1 AS x2",
        "EXPR$2 AS x3",
        "EXPR$2")
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
            streamTableNode(table),
            term("select", "rowtime", "c", "*(c, c) AS $f2")
          ),
          term("window", "TumblingGroupWindow('w, 'rowtime, 900000.millis)"),
          term("select",
            "SUM($f2) AS $f0",
            "SUM(c) AS $f1",
            "COUNT(c) AS $f2",
            "start('w) AS EXPR$4",
            "end('w) AS EXPR$5")
        ),
        term("select",
          "/(-($f0, /(*($f1, $f1), $f2)), $f2) AS EXPR$0",
          "/(-($f0, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null:BIGINT, -($f2, 1))) AS EXPR$1",
          "CAST(POWER(/(-($f0, /(*($f1, $f1), $f2)), $f2), 0.5:DECIMAL(2, 1))) AS EXPR$2",
          "CAST(POWER(/(-($f0, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null:BIGINT, " +
            "-($f2, 1))), 0.5:DECIMAL(2, 1))) AS EXPR$3",
          "EXPR$4",
          "EXPR$5")
      )

    util.verifyTable(windowedTable, expected)
  }
}
