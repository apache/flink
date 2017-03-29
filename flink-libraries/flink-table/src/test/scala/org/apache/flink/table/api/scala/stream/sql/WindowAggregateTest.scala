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
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class WindowAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testNonPartitionedProcessingTimeBoundedWindow() = {

    val sqlQuery = "SELECT a, Count(c) OVER (ORDER BY proctime  " +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS countA " +
      "FROM MyTable"
      val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("range", "BETWEEN 10000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(c) AS w0$o0")
        ),
        term("select", "a", "w0$o0 AS $1")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testPartitionedProcessingTimeBoundedWindow() = {

    val sqlQuery =
      "SELECT a, " +
      "  AVG(c) OVER (PARTITION BY a ORDER BY proctime " +
      "    RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) AS avgA " +
      "FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy","a"),
          term("orderBy", "proctime"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(c) AS w0$o0", "$SUM0(c) AS w0$o1")
        ),
        term("select", "a", "/(CASE(>(w0$o0, 0)", "CAST(w0$o1), null), w0$o0) AS avgA")
      )

    streamUtil.verifySql(sqlQuery, expected)
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
          term("select", "1970-01-01 00:00:00 AS $f0", "c", "a")
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
          term("select", "1970-01-01 00:00:00 AS $f0", "c", "a")
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
          term("select", "1970-01-01 00:00:00 AS $f0", "c", "a")
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
  def testUnboundPartitionedProcessingWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRow() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          streamTableNode(0),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRow() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          streamTableNode(0),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundNonPartitionedEventTimeWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUnboundPartitionedEventTimeWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0)", "CAST(w0$o1), null) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testBoundPartitionedRowTimeWindowWithRow() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
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
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN 5 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testBoundNonPartitionedRowTimeWindowWithRow() = {
    val sql = "SELECT " +
        "c, " +
        "count(a) OVER (ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
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
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN 5 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testBoundPartitionedRowTimeWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN 1000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testBoundNonPartitionedRowTimeWindowWithRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) as cnt1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN 1000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

 @Test
  def testBoundNonPartitionedProcTimeWindowWithRowRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
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
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testBoundPartitionedProcTimeWindowWithRowRange() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
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
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

}
