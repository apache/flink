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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

import java.sql.Timestamp

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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'long, 2)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "TumblingGroupWindow('w, 'long, 5.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
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
        batchTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "TumblingGroupWindow('w, 'long, 2)"),
      term("select", "COUNT(int) AS EXPR$0")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'ts, 7200000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "TumblingGroupWindow('w, 'ts, 7200000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'long, 8.millis, 10.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'long, 2, 1)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'long, 8.millis, 10.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 8.millis every 10.millis on 'long as 'w)
      .groupBy('w)
      .select('int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "SlidingGroupWindow('w, 'long, 8.millis, 10.millis)"),
      term("select", "COUNT(int) AS EXPR$0")
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
        batchTableNode(table),
        term("select", "long", "int")
      ),
      term("window", "SlidingGroupWindow('w, 'long, 2, 1)"),
      term("select", "COUNT(int) AS EXPR$0")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'ts, 3600000.millis, 600000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SlidingGroupWindow('w, 'ts, 3600000.millis, 600000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
      .window(Session withGap 7.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'long, 7.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0")
    )

    util.verifyTable(windowedTable, expected)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTimeWithUdAgg(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]('long, 'int, 'string)

    val myWeightedAvg = new WeightedAvgWithMerge

    val windowedTable = table
      .window(Session withGap 7.millis on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, myWeightedAvg('long, 'int))

    val expected = unaryNode(
      "DataSetWindowAggregate",
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'long, 7.millis)"),
      term("select", "string", "myWeightedAvg(long, int) AS EXPR$0")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'ts, 1800000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
      batchTableNode(table),
      term("groupBy", "string"),
      term("window", "SessionGroupWindow('w, 'ts, 1800000.millis)"),
      term("select", "string", "COUNT(int) AS EXPR$0",
        "start('w) AS EXPR$1", "end('w) AS EXPR$2", "rowtime('w) AS EXPR$3")
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
            batchTableNode(table),
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
          "CAST(POWER(/(-($f0, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null:BIGINT, -($f2, 1))), " +
            "0.5:DECIMAL(2, 1))) AS EXPR$3",
          "EXPR$4",
          "EXPR$5")
      )

    util.verifyTable(windowedTable, expected)
  }
}
