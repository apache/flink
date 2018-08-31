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

package org.apache.flink.table.api.batch.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.WindowReference
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class GroupWindowTest extends TableTestBase {

  //===============================================================================================
  // Common test
  //===============================================================================================

  @Test
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'long, 2.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'long, 5.milli)),
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'long, 5.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", TumblingGroupWindow(WindowReference("w"), 'long, 5.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window", TumblingGroupWindow(WindowReference("w"), 'long, 2.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testLongEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.hours on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'ts, 2.hours)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTimestampEventTimeTumblingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.hours on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", TumblingGroupWindow(WindowReference("w"), 'ts, 2.hours)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  //===============================================================================================
  // Sliding Windows
  //===============================================================================================

  @Test
  def testEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window",
        SlidingGroupWindow(WindowReference("w"), 'long, 8.milli, 10.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window",
        SlidingGroupWindow(WindowReference("w"), 'long, 2.rows, 1.rows)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window",
           SlidingGroupWindow(WindowReference("w"), 'long, 8.milli, 10.milli)),
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.milli every 10.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window",
        SlidingGroupWindow(WindowReference("w"), 'long, 8.milli, 10.milli)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "int", "long")
      ),
      term("window",
        SlidingGroupWindow(WindowReference("w"), 'long, 2.rows, 1.rows)),
      term("select", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testLongEventTimeSlidingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Slide over 1.hour every 10.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SlidingGroupWindow(WindowReference("w"), 'ts, 1.hour, 10.minutes)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTimestampEventTimeSlidingGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Slide over 1.hour every 10.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SlidingGroupWindow(WindowReference("w"), 'ts, 1.hour, 10.minutes)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  //===============================================================================================
  // Session Windows
  //===============================================================================================

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'long, 7.milli)),
      term("select", "string", "COUNT(int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'long, 7.milli)),
      term("select", "string", "myWeightedAvg(long, int) AS TMP_0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testLongEventTimeSessionGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 30.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'ts, 30.minutes)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testTimestampEventTimeSessionGroupWindowWithProperties(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Timestamp, Int, String)]('ts, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 30.minutes on 'ts as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(0),
      term("groupBy", "string"),
      term("window", SessionGroupWindow(WindowReference("w"), 'ts, 30.minutes)),
      term("select", "string", "COUNT(int) AS TMP_0",
        "start('w) AS TMP_1", "end('w) AS TMP_2", "rowtime('w) AS TMP_3")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String, Long)]('rowtime, 'a, 'b, 'c)

    val windowedTable = table
      .window(Tumble over 15.minutes on 'rowtime as 'w)
      .groupBy('w)
      .select('c.varPop, 'c.varSamp, 'c.stddevPop, 'c.stddevSamp, 'w.start, 'w.end)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
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
