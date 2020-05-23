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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class GroupWindowTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  private val table = streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testTumbleFunction() = {
    streamUtil.tableEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT " +
        "  COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "rowtime", "c", "a")
          ),
          term("window", "TumblingGroupWindow('w$, 'rowtime, 900000.millis)"),
          term("select",
            "COUNT(*) AS EXPR$0",
              "weightedAvg(c, a) AS wAvg",
              "start('w$) AS w$start",
              "end('w$) AS w$end",
              "rowtime('w$) AS w$rowtime",
              "proctime('w$) AS w$proctime")
        ),
        term("select", "EXPR$0", "wAvg", "w$start AS EXPR$2", "w$end AS EXPR$3")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testHoppingFunction() = {
    streamUtil.tableEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  HOP_START(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR), " +
        "  HOP_END(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "proctime", "c", "a")
          ),
          term("window", "SlidingGroupWindow('w$, 'proctime, 3600000.millis, 900000.millis)"),
          term("select",
            "COUNT(*) AS EXPR$0",
              "weightedAvg(c, a) AS wAvg",
              "start('w$) AS w$start",
              "end('w$) AS w$end",
              "proctime('w$) AS w$proctime")
        ),
        term("select", "EXPR$0", "wAvg", "w$start AS EXPR$2", "w$end AS EXPR$3")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testSessionFunction() = {
    streamUtil.tableEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT " +
        "  COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  SESSION_START(proctime, INTERVAL '15' MINUTE), " +
        "  SESSION_END(proctime, INTERVAL '15' MINUTE) " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime, INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "proctime", "c", "a")
          ),
          term("window", "SessionGroupWindow('w$, 'proctime, 900000.millis)"),
          term("select",
            "COUNT(*) AS EXPR$0",
            "weightedAvg(c, a) AS wAvg",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "proctime('w$) AS w$proctime")
        ),
        term("select", "EXPR$0", "wAvg", "w$start AS EXPR$2", "w$end AS EXPR$3")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testExpressionOnWindowAuxFunction() = {
    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE) + INTERVAL '1' MINUTE " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "rowtime")
          ),
          term("window", "TumblingGroupWindow('w$, 'rowtime, 900000.millis)"),
          term("select",
            "COUNT(*) AS EXPR$0",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime",
            "proctime('w$) AS w$proctime")
        ),
        term("select", "EXPR$0", "+(w$end, 60000:INTERVAL MINUTE) AS EXPR$1")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testExpressionOnWindowHavingFunction() = {
    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  HOP_START(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "FROM MyTable " +
        "GROUP BY HOP(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "HAVING " +
        "  SUM(a) > 0 AND " +
        "  QUARTER(HOP_START(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "rowtime, a")
          ),
          term("window", "SlidingGroupWindow('w$, 'rowtime, 60000.millis, 900000.millis)"),
          term("select",
            "COUNT(*) AS EXPR$0",
            "SUM(a) AS $f1",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime",
            "proctime('w$) AS w$proctime")
        ),
        term("select", "EXPR$0", "w$start AS EXPR$1"),
        term("where",
          "AND(>($f1, 0), " +
            "=(EXTRACT(FLAG(QUARTER), w$start), 1:BIGINT))")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testMultiWindowSqlWithAggregation() = {
    val sql =
      s"""SELECT
          TUMBLE_ROWTIME(zzzzz, INTERVAL '0.004' SECOND),
          TUMBLE_END(zzzzz, INTERVAL '0.004' SECOND),
          COUNT(`a`) AS `a`
        FROM (
          SELECT
            COUNT(`a`) AS `a`,
            TUMBLE_ROWTIME(rowtime, INTERVAL '0.002' SECOND) AS `zzzzz`
          FROM MyTable
          GROUP BY TUMBLE(rowtime, INTERVAL '0.002' SECOND)
        )
        GROUP BY TUMBLE(zzzzz, INTERVAL '0.004' SECOND)"""

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            unaryNode(
              "DataStreamGroupWindowAggregate",
              unaryNode(
                "DataStreamCalc",
                streamTableNode(table),
                term("select", "rowtime, a")
              ),
              term("window", "TumblingGroupWindow('w$, 'rowtime, 2.millis)"),
              term("select",
                "COUNT(a) AS a",
                "start('w$) AS w$start",
                "end('w$) AS w$end",
                "rowtime('w$) AS w$rowtime",
                "proctime('w$) AS w$proctime")
            ),
            term("select", "w$rowtime AS $f2")
          ),
          term("window", "TumblingGroupWindow('w$, '$f2, 4.millis)"),
          term("select",
            "COUNT(*) AS a",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime",
            "proctime('w$) AS w$proctime")
        ),
        term("select", "w$rowtime AS EXPR$0", "w$end AS EXPR$1", "a")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testDecomposableAggFunctions() = {

    val sql =
      "SELECT " +
        "  VAR_POP(c), VAR_SAMP(c), STDDEV_POP(c), STDDEV_SAMP(c), " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
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
          term("window", "TumblingGroupWindow('w$, 'rowtime, 900000.millis)"),
          term("select",
            "SUM($f2) AS $f0",
            "SUM(c) AS $f1",
            "COUNT(c) AS $f2",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime",
            "proctime('w$) AS w$proctime")
        ),
        term("select",
          "/(-($f0, /(*($f1, $f1), $f2)), $f2) AS EXPR$0",
          "/(-($f0, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null:BIGINT, -($f2, 1))) AS EXPR$1",
          "CAST(POWER(/(-($f0, /(*($f1, $f1), $f2)), $f2), 0.5:DECIMAL(2, 1))) AS EXPR$2",
          "CAST(POWER(/(-($f0, /(*($f1, $f1), $f2)), CASE(=($f2, 1), null:BIGINT, -($f2, 1))), " +
            "0.5:DECIMAL(2, 1))) AS EXPR$3",
          "w$start AS EXPR$4",
          "w$end AS EXPR$5")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testReturnTypeInferenceForWindowAgg() = {

    val innerQuery =
      """
        |SELECT
        | CASE a WHEN 1 THEN 1 ELSE 99 END AS correct,
        | rowtime
        |FROM MyTable
      """.stripMargin

    val sql =
      "SELECT " +
        "  sum(correct) as s, " +
        "  avg(correct) as a, " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as wStart " +
        s"FROM ($innerQuery) " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "rowtime", "CASE(=(a, 1), 1, 99) AS $f1")
          ),
          term("window", "TumblingGroupWindow('w$, 'rowtime, 900000.millis)"),
          term("select",
            "SUM($f1) AS s",
            "AVG($f1) AS a",
            "start('w$) AS w$start",
            "end('w$) AS w$end",
            "rowtime('w$) AS w$rowtime",
            "proctime('w$) AS w$proctime")
        ),
        term("select", "CAST(s) AS s", "CAST(a) AS a", "w$start AS wStart")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testWindowAggregateWithDifferentWindows() = {
    // This test ensures that the LogicalWindowAggregate and FlinkLogicalWindowAggregate nodes'
    // digests contain the window specs. This allows the planner to make the distinction between
    // similar aggregations using different windows (see FLINK-15577).
    val sql =
      """
        |WITH window_1h AS (
        |    SELECT 1
        |    FROM MyTable
        |    GROUP BY HOP(`rowtime`, INTERVAL '1' HOUR, INTERVAL '1' HOUR)
        |),
        |
        |window_2h AS (
        |    SELECT 1
        |    FROM MyTable
        |    GROUP BY HOP(`rowtime`, INTERVAL '1' HOUR, INTERVAL '2' HOUR)
        |)
        |
        |(SELECT * FROM window_1h)
        |UNION ALL
        |(SELECT * FROM window_2h)
        |""".stripMargin

    val expected =
      binaryNode(
        "DataStreamUnion",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(table),
              term("select", "rowtime")
            ),
            // This window is the 1hr window
            term("window", "SlidingGroupWindow('w$, 'rowtime, 3600000.millis, 3600000.millis)"),
            term("select")
          ),
          term("select", "1 AS EXPR$0")
        ),
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(table),
              term("select", "rowtime")
            ),
            // This window is the 2hr window
            term("window", "SlidingGroupWindow('w$, 'rowtime, 7200000.millis, 3600000.millis)"),
            term("select")
          ),
          term("select", "1 AS EXPR$0")
        ),
        term("all", "true"),
        term("union all", "EXPR$0")
      )

    streamUtil.verifySql(sql, expected)
  }
}
