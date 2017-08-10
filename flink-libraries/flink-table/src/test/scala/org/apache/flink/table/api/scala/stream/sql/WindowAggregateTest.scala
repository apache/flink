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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.{OverAgg0, WeightedAvgWithMerge}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class WindowAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testOverAggregation(): Unit = {
    streamUtil.addFunction("overAgg", new OverAgg0)

    val sqlQuery = "SELECT overAgg(c, a) FROM MyTable"
    streamUtil.tEnv.sql(sqlQuery)
  }

  @Test
  def testGroupbyWithoutWindow() = {
    val sql = "SELECT COUNT(a) FROM MyTable GROUP BY b"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "b", "a")
          ),
          term("groupBy", "b"),
          term("select", "b", "COUNT(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testTumbleFunction() = {
    streamUtil.tEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT " +
        "  COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "rowtime", "c", "a")
        ),
        term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
        term("select",
          "COUNT(*) AS EXPR$0, " +
            "weightedAvg(c, a) AS wAvg, " +
            "start('w$) AS w$start, " +
            "end('w$) AS w$end")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testHoppingFunction() = {
    streamUtil.tEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  HOP_START(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR), " +
        "  HOP_END(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)"
    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "proctime", "c", "a")
        ),
        term("window", SlidingGroupWindow('w$, 'proctime, 3600000.millis, 900000.millis)),
        term("select",
          "COUNT(*) AS EXPR$0, " +
            "weightedAvg(c, a) AS wAvg, " +
            "start('w$) AS w$start, " +
            "end('w$) AS w$end")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testSessionFunction() = {
    streamUtil.tEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sql =
      "SELECT " +
        "  COUNT(*), weightedAvg(c, a) AS wAvg, " +
        "  SESSION_START(proctime, INTERVAL '15' MINUTE), " +
        "  SESSION_END(proctime, INTERVAL '15' MINUTE) " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime, INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "proctime", "c", "a")
        ),
        term("window", SessionGroupWindow('w$, 'proctime, 900000.millis)),
        term("select",
          "COUNT(*) AS EXPR$0, " +
            "weightedAvg(c, a) AS wAvg, " +
            "start('w$) AS w$start, " +
            "end('w$) AS w$end")
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
            streamTableNode(0),
            term("select", "rowtime")
          ),
          term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
          term("select", "COUNT(*) AS EXPR$0", "start('w$) AS w$start", "end('w$) AS w$end")
        ),
        term("select", "EXPR$0", "DATETIME_PLUS(w$end, 60000) AS $f1")
      )

    streamUtil.verifySql(sql, expected)
  }

  @Test(expected = classOf[TableException])
  def testTumbleWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testHopWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testSessionWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testVariableWindowSize() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, c * INTERVAL '1' MINUTE)"
    streamUtil.verifySql(sql, "n/a")
  }

  @Test(expected = classOf[ValidationException])
  def testWindowUdAggInvalidArgs(): Unit = {
    streamUtil.tEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, weightedAvg(a, b) AS wAvg " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(proctime(), INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
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
            streamTableNode(0),
            term("select", "rowtime, a")
          ),
          term("window", SlidingGroupWindow('w$, 'rowtime, 60000.millis, 900000.millis)),
          term("select",
            "COUNT(*) AS EXPR$0",
            "SUM(a) AS $f1",
            "start('w$) AS w$start",
            "end('w$) AS w$end")
        ),
        term("select", "EXPR$0", "w$start"),
        term("where",
          "AND(>($f1, 0), " +
            "=(+(/INT(-(EXTRACT_DATE(FLAG(MONTH), /INT(Reinterpret(w$start), 86400000)), 1), " +
              "3), 1), 1))")
      )

    streamUtil.verifySql(sql, expected)
  }
}
