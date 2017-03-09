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
import org.apache.flink.table.plan.logical.{EventTimeTumblingGroupWindow, ProcessingTimeTumblingGroupWindow}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class WindowAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  @Test
  def testNonPartitionedTumbleWindow() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(rowtime() TO HOUR)"
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
          term("window", EventTimeTumblingGroupWindow(Some('w$), 'rowtime, 3600000.millis)),
          term("select", "COUNT(*) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testPartitionedTumbleWindow1() = {
    val sql = "SELECT a, COUNT(*) FROM MyTable GROUP BY a, FLOOR(rowtime() TO MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS $f1")
          ),
          term("groupBy", "a"),
          term("window", EventTimeTumblingGroupWindow(Some('w$), 'rowtime, 60000.millis)),
          term("select", "a", "COUNT(*) AS EXPR$1")
        ),
        term("select", "a", "EXPR$1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testPartitionedTumbleWindow2() = {
    val sql = "SELECT a, SUM(c), b FROM MyTable GROUP BY a, FLOOR(rowtime() TO SECOND), b"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS $f1, b, c")
          ),
          term("groupBy", "a, b"),
          term("window", EventTimeTumblingGroupWindow(Some('w$), 'rowtime, 1000.millis)),
          term("select", "a", "b", "SUM(c) AS EXPR$1")
        ),
        term("select", "a", "EXPR$1", "b")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcessingTime() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(proctime() TO HOUR)"
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
          term("window", ProcessingTimeTumblingGroupWindow(Some('w$), 3600000.millis)),
          term("select", "COUNT(*) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    streamUtil.verifySql(sql, expected)
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

  @Test
  def testUnboundPartitionedProcessingWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRow() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRow() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }
}
