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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.{EventTimeTumblingGroupWindow, ProcessingTimeSessionGroupWindow, ProcessingTimeSlidingGroupWindow}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class WindowAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  @Test
  def testTumbleFunction() = {

    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  TUMBLE_START(rowtime(), INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime(), INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime(), INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "1970-01-01 00:00:00 AS $f0")
          ),
          term("window", EventTimeTumblingGroupWindow('w$, 'rowtime, 900000.millis)),
          term("select", "COUNT(*) AS EXPR$0, start('w$) AS w$start, end('w$) AS w$end")
        ),
        term("select", "EXPR$0, CAST(w$start) AS w$start, CAST(w$end) AS w$end")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testHoppingFunction() = {
    val sql =
      "SELECT COUNT(*), " +
        "  HOP_START(proctime(), INTERVAL '15' MINUTE, INTERVAL '1' HOUR), " +
        "  HOP_END(proctime(), INTERVAL '15' MINUTE, INTERVAL '1' HOUR) " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime(), INTERVAL '15' MINUTE, INTERVAL '1' HOUR)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "1970-01-01 00:00:00 AS $f0")
          ),
          term("window", ProcessingTimeSlidingGroupWindow('w$,
            3600000.millis, 900000.millis)),
          term("select", "COUNT(*) AS EXPR$0, start('w$) AS w$start, end('w$) AS w$end")
        ),
        term("select", "EXPR$0, CAST(w$start) AS w$start, CAST(w$end) AS w$end")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testSessionFunction() = {
    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  SESSION_START(proctime(), INTERVAL '15' MINUTE), " +
        "  SESSION_END(proctime(), INTERVAL '15' MINUTE) " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime(), INTERVAL '15' MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "1970-01-01 00:00:00 AS $f0")
          ),
          term("window", ProcessingTimeSessionGroupWindow('w$, 900000.millis)),
          term("select", "COUNT(*) AS EXPR$0, start('w$) AS w$start, end('w$) AS w$end")
        ),
        term("select", "EXPR$0, CAST(w$start) AS w$start, CAST(w$end) AS w$end")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test(expected = classOf[TableException])
  def testTumbleWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(proctime(), INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testHopWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime(), INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testSessionWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime(), INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testVariableWindowSize() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime(), c * INTERVAL '1' MINUTE)"
    streamUtil.verifySql(sql, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testMultiWindow() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY " +
      "FLOOR(rowtime() TO HOUR), FLOOR(rowtime() TO MINUTE)"
    val expected = ""
    streamUtil.verifySql(sql, expected)
  }

  @Test(expected = classOf[TableException])
  def testInvalidWindowExpression() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(localTimestamp TO HOUR)"
    val expected = ""
    streamUtil.verifySql(sql, expected)
  }

}
