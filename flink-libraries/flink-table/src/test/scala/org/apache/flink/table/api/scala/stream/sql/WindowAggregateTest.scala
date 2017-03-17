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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.stream.utils.{
  StreamingWithStateTestBase,
  StreamITCase,
  StreamTestData
}
import org.apache.flink.types.Row
import scala.collection.mutable
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.junit.Assert._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


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
  
   @Test
  def testMaxAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS maxC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
        .assignTimestampsAndWatermarks(new ProcTimeTimestamp())
        .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,2",
      "3,3",
      "3,4",
      "3,5",
      "4,6",
      "4,7",
      "4,8",
      "4,9",
      "5,10",
      "5,11",
      "5,12",
      "5,13",
      "5,14")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMinAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MIN(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS minC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
        .assignTimestampsAndWatermarks(new ProcTimeTimestamp())
        .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,1",
      "3,3",
      "3,3",
      "3,3",
      "4,6",
      "4,6",
      "4,6",
      "4,6",
      "5,10",
      "5,10",
      "5,10",
      "5,10",
      "5,10")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, SUM(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS sumC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
        .assignTimestampsAndWatermarks(new ProcTimeTimestamp())
        .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,3",
      "3,12",
      "3,3",
      "3,7",
      "4,13",
      "4,21",
      "4,30",
      "4,6",
      "5,10",
      "5,21",
      "5,33",
      "5,46",
      "5,60")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, AVG(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS avgC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
         .assignTimestampsAndWatermarks(new ProcTimeTimestamp())
         .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,1",
      "3,3",
      "3,3",
      "3,4",
      "4,6",
      "4,6",
      "4,7",
      "4,7",
      "5,10",
      "5,10",
      "5,11",
      "5,11",
      "5,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
  
}

class ProcTimeTimestamp
      extends AssignerWithPunctuatedWatermarks[(Int, Long, Int, String, Long)] {

    override def checkAndGetNextWatermark(
      lastElement: (Int, Long, Int, String, Long),
      extractedTimestamp: Long): Watermark = {
      null
    }

    override def extractTimestamp(
      element: (Int, Long, Int, String, Long),
      previousElementTimestamp: Long): Long = {
      System.currentTimeMillis()
    }
  }

