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

package org.apache.flink.table.plan

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.plan.TimeIndicatorConversionTest.{ScalarFunc, TableFunc}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Tests for [[org.apache.flink.table.calcite.RelTimeIndicatorConverter]].
  */
class TimeIndicatorConversionTest extends TableTestBase {

  @Test
  def testSimpleMaterialization(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t
      .select('rowtime.floor(TimeIntervalUnit.DAY) as 'rowtime, 'long)
      .filter('long > 0)
      .select('rowtime)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(t),
      term("select", "FLOOR(CAST(rowtime)", "FLAG(DAY)) AS rowtime"),
      term("where", ">(long, 0)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testSelectAll(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t.select('*)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(t),
      term("select", "rowtime", "long", "int",
        "PROCTIME(proctime) AS proctime")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFilteringOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .filter('rowtime > "1990-12-02 12:11:11".toTimestamp)
      .select('rowtime)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(t),
      term("select", "rowtime"),
      term("where", ">(CAST(rowtime), 1990-12-02 12:11:11:TIMESTAMP(3))")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testGroupingOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t
      .groupBy('rowtime)
      .select('long.count)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "CAST(rowtime) AS rowtime", "long")
        ),
        term("groupBy", "rowtime"),
        term("select", "rowtime", "COUNT(long) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .groupBy('long)
      .select('rowtime.min)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "CAST(rowtime) AS rowtime", "long")
        ),
        term("groupBy", "long"),
        term("select", "long", "MIN(rowtime) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)
    val func = new TableFunc

    val result = t.joinLateral(func('rowtime, 'proctime, "") as 's).select('rowtime, 'proctime, 's)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(t),
        term("invocation",
          s"${func.functionIdentifier}(CAST($$0):TIMESTAMP(3) NOT NULL, PROCTIME($$3), '')"),
        term("correlate", s"table(TableFunc(CAST(rowtime), PROCTIME(proctime), ''))"),
        term("select", "rowtime", "long", "int", "proctime", "s"),
        term("rowType", "RecordType(TIME ATTRIBUTE(ROWTIME) rowtime, BIGINT long, INTEGER int, " +
          "TIME ATTRIBUTE(PROCTIME) proctime, VARCHAR(65536) s)"),
        term("joinType", "INNER")
      ),
      term("select", "rowtime", "PROCTIME(proctime) AS proctime", "s")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .window(Tumble over 100.millis on 'rowtime as 'w)
      .groupBy('w, 'long)
      .select('w.end as 'rowtime, 'long, 'int.sum)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(t),
        term("groupBy", "long"),
        term("window", "TumblingGroupWindow('w, 'rowtime, 100.millis)"),
        term("select", "long", "SUM(int) AS EXPR$1", "end('w) AS EXPR$0")
      ),
      term("select", "EXPR$0 AS rowtime", "long", "EXPR$1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testUnion(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = t.unionAll(t).select('rowtime)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "rowtime")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "rowtime")
      ),
      term("all", "true"),
      term("union all", "rowtime")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .window(Tumble over 100.millis on 'rowtime as 'w)
      .groupBy('w, 'long)
      .select('w.rowtime as 'newrowtime, 'long, 'int.sum as 'int)
      .window(Tumble over 1.second on 'newrowtime as 'w2)
      .groupBy('w2, 'long)
      .select('w2.end, 'long, 'int.sum)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowAggregate",
            streamTableNode(t),
            term("groupBy", "long"),
            term("window", "TumblingGroupWindow('w, 'rowtime, 100.millis)"),
            term("select", "long", "SUM(int) AS EXPR$1", "rowtime('w) AS EXPR$0")
          ),
          term("select", "EXPR$0 AS newrowtime", "long", "EXPR$1 AS int")
        ),
        term("groupBy", "long"),
        term("window", "TumblingGroupWindow('w2, 'newrowtime, 1000.millis)"),
        term("select", "long", "SUM(int) AS EXPR$1", "end('w2) AS EXPR$0")
      ),
      term("select", "EXPR$0", "long", "EXPR$1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testGroupingOnProctime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sqlQuery("SELECT COUNT(long) FROM MyTable GROUP BY proctime")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "PROCTIME(proctime) AS proctime", "long")
        ),
        term("groupBy", "proctime"),
        term("select", "proctime", "COUNT(long) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testAggregationOnProctime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sqlQuery("SELECT MIN(proctime) FROM MyTable GROUP BY long")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "long", "PROCTIME(proctime) AS proctime")
        ),
        term("groupBy", "long"),
        term("select", "long", "MIN(proctime) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindowSql(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery(
      "SELECT TUMBLE_END(rowtime, INTERVAL '0.1' SECOND) AS `rowtime`, `long`, " +
        "SUM(`int`) FROM MyTable " +
        "GROUP BY `long`, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(t),
        term("groupBy", "long"),
        term("window", "TumblingGroupWindow('w$, 'rowtime, 100.millis)"),
        term("select",
          "long",
          "SUM(int) AS EXPR$2",
          "start('w$) AS w$start",
          "end('w$) AS w$end",
          "rowtime('w$) AS w$rowtime",
          "proctime('w$) AS w$proctime")
      ),
      term("select", "w$end AS rowtime", "long", "EXPR$2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindowWithAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery("SELECT MIN(rowtime), long FROM MyTable " +
      "GROUP BY long, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "long", "rowtime", "CAST(rowtime) AS rowtime0")
        ),
        term("groupBy", "long"),
        term("window", "TumblingGroupWindow('w$, 'rowtime, 100.millis)"),
        term("select", "long", "MIN(rowtime0) AS EXPR$0")
      ),
      term("select", "EXPR$0", "long")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMaterializeRightSideOfTemporalTableJoin(): Unit = {
    val util = streamTestUtil()

    val proctimeOrders = util.addTable[(Long, String)](
      "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime)

    val proctimeRatesHistory = util.addTable[(String, Int)](
      "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

    val proctimeRates = proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency)

    val result = proctimeOrders
      .joinLateral(proctimeRates('o_proctime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate", $"currency", $"proctime").as("converted_amount")
      .window(Tumble over 1.second on 'proctime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    val expected =
      unaryAnyNode(
        unaryAnyNode(
          unaryNode(
            "DataStreamCalc",
            anySubtree(),
            term(
              "select",
              "*(o_amount, rate) AS converted_amount",
              "currency",
              "PROCTIME(proctime) AS proctime")
          )
        )
      )

    util.verifyTable(result, expected)
  }

  @Test
  def testDoNotMaterializeLeftSideOfTemporalTableJoin(): Unit = {
    val util = streamTestUtil()

    val proctimeOrders = util.addTable[(Long, String)](
      "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime)

    val proctimeRatesHistory = util.addTable[(String, Int)](
      "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

    val proctimeRates = proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency)

    val result = proctimeOrders
      .joinLateral(proctimeRates('o_proctime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate", $"currency", $"o_proctime").as("converted_amount")
      .window(Tumble over 1.second on 'o_proctime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    val expected =
      unaryAnyNode(
        unaryAnyNode(
          unaryNode(
            "DataStreamCalc",
            anySubtree(),
            term(
              "select",
              "*(o_amount, rate) AS converted_amount",
              "currency",
              "o_proctime")
          )
        )
      )

    util.verifyTable(result, expected)
  }

  @Test
  def testMaterializeLeftRowtimeWithProcessingTimeTemporalTableJoin(): Unit = {
    val util = streamTestUtil()

    val proctimeOrders = util.addTable[(Long, String)](
      "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime, 'o_rowtime.rowtime)

    val proctimeRatesHistory = util.addTable[(String, Int)](
      "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

    val proctimeRates = proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency)

    val result = proctimeOrders
      .joinLateral(proctimeRates('o_proctime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate", $"currency", $"o_proctime", $"o_rowtime")
      .as("converted_amount")
      .window(Tumble over 1.second on 'o_rowtime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    val expected =
      unaryAnyNode(
        unaryAnyNode(
          unaryNode(
            "DataStreamCalc",
            anySubtree(),
            term(
              "select",
              "*(o_amount, rate) AS converted_amount",
              "currency",
              "CAST(o_rowtime) AS o_rowtime")
          )
        )
      )

    util.verifyTable(result, expected)
  }

  @Test
  def testMatchRecognizeRowtimeMaterialization(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)](
      "RowtimeTicker",
      'rowtime.rowtime,
      'symbol,
      'price)
    util.addFunction("func", new ScalarFunc)

    val query =
      s"""
         |SELECT
         |  *
         |FROM RowtimeTicker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY rowtime
         |  MEASURES
         |    MATCH_ROWTIME() as matchRowtime,
         |    func(MATCH_ROWTIME()) as funcRowtime,
         |    A.rowtime as noLongerRowtime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |)
         |""".stripMargin

    val expected = unaryNode(
      "DataStreamMatch",
      streamTableNode(t),
      term("partitionBy", "symbol"),
      term("orderBy", "rowtime ASC"),
      term("measures",
        "FINAL(MATCH_ROWTIME()) AS matchRowtime",
        "FINAL(func(CAST(MATCH_ROWTIME()))) AS funcRowtime",
        "FINAL(CAST(A.rowtime)) AS noLongerRowtime"
      ),
      term("rowsPerMatch", "ONE ROW PER MATCH"),
      term("after", "SKIP TO NEXT ROW"),
      term("pattern", "'A'"),
      term("define", "{A=>(PREV(A.$2, 0), 0)}")
    )

    util.verifySql(query, expected)
  }

  @Test
  def testMatchRecognizeProctimeMaterialization(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)](
      "ProctimeTicker",
      'rowtime.rowtime,
      'symbol,
      'price,
      'proctime.proctime)
    util.addFunction("func", new ScalarFunc)

    val query =
      s"""
         |SELECT
         |  *
         |FROM ProctimeTicker
         |MATCH_RECOGNIZE (
         |  PARTITION BY symbol
         |  ORDER BY rowtime
         |  MEASURES
         |    MATCH_PROCTIME() as matchProctime,
         |    func(MATCH_PROCTIME()) as funcProctime,
         |    A.proctime as noLongerProctime
         |  ONE ROW PER MATCH
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.price > 0
         |)
         |""".stripMargin

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamMatch",
        streamTableNode(t),
        term("partitionBy", "symbol"),
        term("orderBy", "rowtime ASC"),
        term("measures",
          "FINAL(MATCH_PROCTIME()) AS matchProctime",
          "FINAL(func(PROCTIME(MATCH_PROCTIME()))) AS funcProctime",
          "FINAL(PROCTIME(A.proctime)) AS noLongerProctime"
        ),
        term("rowsPerMatch", "ONE ROW PER MATCH"),
        term("after", "SKIP TO NEXT ROW"),
        term("pattern", "'A'"),
        term("define", "{A=>(PREV(A.$2, 0), 0)}")
      ),
      term("select",
        "symbol",
        "PROCTIME(matchProctime) AS matchProctime",
        "funcProctime",
        "noLongerProctime"
      )
    )

    util.verifySql(query, expected)
  }
}

object TimeIndicatorConversionTest {

  class TableFunc extends TableFunction[String] {
    val t = new Timestamp(0L)
    def eval(time1: Long, time2: Timestamp, string: String): Unit = {
      collect(time1.toString + time2.after(t) + string)
    }
  }

  class ScalarFunc extends ScalarFunction {
    val t = new Timestamp(0L)
    def eval(time: Timestamp): String = {
      time.toString
    }
  }
}
