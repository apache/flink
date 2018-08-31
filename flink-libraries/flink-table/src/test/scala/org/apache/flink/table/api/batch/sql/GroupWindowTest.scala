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

package org.apache.flink.table.api.batch.sql

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class GroupWindowTest extends TableTestBase {

  @Test
  def testNonPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM T GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"

    val expected =
      unaryNode(
        "DataSetWindowAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "ts, a, b")
        ),
        term("window", TumblingGroupWindow('w$, 'ts, 7200000.millis)),
        term("select", "SUM(a) AS sumA, COUNT(b) AS cntB")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT " +
        "  TUMBLE_START(ts, INTERVAL '4' MINUTE), " +
        "  TUMBLE_END(ts, INTERVAL '4' MINUTE), " +
        "  TUMBLE_ROWTIME(ts, INTERVAL '4' MINUTE), " +
        "  c, " +
        "  SUM(a) AS sumA, " +
        "  MIN(b) AS minB " +
        "FROM T " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          batchTableNode(0),
          term("groupBy", "c"),
          term("window", TumblingGroupWindow('w$, 'ts, 240000.millis)),
          term("select", "c, SUM(a) AS sumA, MIN(b) AS minB, " +
            "start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime")
        ),
        term("select", "CAST(w$start) AS EXPR$0, CAST(w$end) AS EXPR$1, " +
          "w$rowtime AS EXPR$2, c, sumA, minB")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTumbleWindowWithUdAgg() = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val weightedAvg = new WeightedAvgWithMerge
    util.tableEnv.registerFunction("weightedAvg", weightedAvg)

    val sql = "SELECT weightedAvg(b, a) AS wAvg " +
      "FROM T " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"

    val expected =
      unaryNode(
        "DataSetWindowAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "ts, b, a")
        ),
        term("window", TumblingGroupWindow('w$, 'ts, 240000.millis)),
        term("select", "weightedAvg(b, a) AS wAvg")
      )

    util.verifySql(sql, expected)
  }

  @Test
  def testNonPartitionedHopWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '90' MINUTE)"

    val expected =
      unaryNode(
        "DataSetWindowAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "ts, a, b")
        ),
        term("window",
          SlidingGroupWindow('w$, 'ts, 5400000.millis, 900000.millis)),
        term("select", "SUM(a) AS sumA, COUNT(b) AS cntB")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testPartitionedHopWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

    val sqlQuery =
      "SELECT " +
        "  c, " +
        "  HOP_END(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_START(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_ROWTIME(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  AVG(b) AS avgB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), d, c"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          batchTableNode(0),
          term("groupBy", "c, d"),
          term("window",
            SlidingGroupWindow('w$, 'ts, 10800000.millis, 3600000.millis)),
          term("select", "c, d, SUM(a) AS sumA, AVG(b) AS avgB, " +
            "start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime")
        ),
        term("select", "c, CAST(w$end) AS EXPR$1, CAST(w$start) AS EXPR$2, " +
          "w$rowtime AS EXPR$3, sumA, avgB")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testNonPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT COUNT(*) AS cnt FROM T GROUP BY SESSION(ts, INTERVAL '30' MINUTE)"

    val expected =
      unaryNode(
        "DataSetWindowAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "ts")
        ),
        term("window", SessionGroupWindow('w$, 'ts, 1800000.millis)),
        term("select", "COUNT(*) AS cnt")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Int, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

    val sqlQuery =
      "SELECT " +
        "  c, d, " +
        "  SESSION_START(ts, INTERVAL '12' HOUR), " +
        "  SESSION_END(ts, INTERVAL '12' HOUR), " +
        "  SESSION_ROWTIME(ts, INTERVAL '12' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  MIN(b) AS minB " +
        "FROM T " +
        "GROUP BY SESSION(ts, INTERVAL '12' HOUR), c, d"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          batchTableNode(0),
          term("groupBy", "c, d"),
          term("window", SessionGroupWindow('w$, 'ts, 43200000.millis)),
          term("select", "c, d, SUM(a) AS sumA, MIN(b) AS minB, " +
            "start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime")
        ),
        term("select", "c, d, CAST(w$start) AS EXPR$2, CAST(w$end) AS EXPR$3, " +
          "w$rowtime AS EXPR$4, sumA, minB")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testWindowEndOnly(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT " +
        "  TUMBLE_END(ts, INTERVAL '4' MINUTE)" +
        "FROM T " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "ts, c")
          ),
          term("groupBy", "c"),
          term("window", TumblingGroupWindow('w$, 'ts, 240000.millis)),
          term("select", "c, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime")
        ),
        term("select", "CAST(w$end) AS EXPR$0")
      )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testExpressionOnWindowHavingFunction() = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "HAVING " +
        "  SUM(a) > 0 AND " +
        "  QUARTER(HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "ts, a")
          ),
          term("window", SlidingGroupWindow('w$, 'ts, 60000.millis, 900000.millis)),
          term("select",
            "COUNT(*) AS EXPR$0",
            "SUM(a) AS $f1",
            "start('w$) AS w$start",
            "end('w$) AS w$end, " +
            "rowtime('w$) AS w$rowtime")
        ),
        term("select", "EXPR$0", "CAST(w$start) AS EXPR$1"),
        term("where",
          "AND(>($f1, 0), " +
            "=(EXTRACT(FLAG(QUARTER), CAST(w$start)), 1))")
      )

    util.verifySql(sql, expected)
  }

  @Test
  def testDecomposableAggFunctions() = {
    val util = batchTestUtil()
    util.addTable[(Int, String, Long, Timestamp)]("MyTable", 'a, 'b, 'c, 'rowtime)

    val sql =
      "SELECT " +
        "  VAR_POP(c), VAR_SAMP(c), STDDEV_POP(c), STDDEV_SAMP(c), " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetWindowAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "rowtime", "c",
              "*(c, c) AS $f2", "*(c, c) AS $f3", "*(c, c) AS $f4", "*(c, c) AS $f5")
          ),
          term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
          term("select",
            "SUM($f2) AS $f0",
            "SUM(c) AS $f1",
            "COUNT(c) AS $f2",
            "SUM($f3) AS $f3",
            "SUM($f4) AS $f4",
            "SUM($f5) AS $f5",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime")
        ),
        term("select",
          "CAST(/(-($f0, /(*($f1, $f1), $f2)), $f2)) AS EXPR$0",
          "CAST(/(-($f3, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null, -($f2, 1)))) AS EXPR$1",
          "CAST(POWER(/(-($f4, /(*($f1, $f1), $f2)), $f2), 0.5)) AS EXPR$2",
          "CAST(POWER(/(-($f5, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null, -($f2, 1))), 0.5)) " +
            "AS EXPR$3",
          "CAST(w$start) AS EXPR$4",
          "CAST(w$end) AS EXPR$5")
      )

    util.verifySql(sql, expected)
  }
}
