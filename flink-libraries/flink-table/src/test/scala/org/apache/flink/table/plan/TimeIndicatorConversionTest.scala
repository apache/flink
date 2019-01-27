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
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.plan.TimeIndicatorConversionTest.TableFunc
import org.apache.flink.table.util.TableTestBase
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

    util.verifyPlan(result)
  }

  @Test
  def testSelectAll(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t.select('*)

    util.verifyPlan(result)
  }

  @Test
  def testFilteringOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .filter('rowtime > "1990-12-02 12:11:11".toTimestamp)
      .select('rowtime)

    util.verifyPlan(result)
  }

  @Test
  def testGroupingOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t
      .groupBy('rowtime)
      .select('long.count)

    util.verifyPlan(result)
  }

  @Test
  def testAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .groupBy('long)
      .select('rowtime.min)

    util.verifyPlan(result)
  }

  @Test
  def testTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)
    val func = new TableFunc

    val result = t.join(func('rowtime, 'proctime, "") as 's).select('rowtime, 'proctime, 's)

    util.verifyPlan(result)
  }

  @Test
  def testWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .window(Tumble over 100.millis on 'rowtime as 'w)
      .groupBy('w, 'long)
      .select('w.end as 'rowtime, 'long, 'int.sum)

    util.verifyPlan(result)
  }

  @Test
  def testUnion(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = t.unionAll(t).select('rowtime)

    util.verifyPlan(result)
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

    util.verifyPlan(result)
  }

  @Test
  def testGroupingOnProctime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sqlQuery("SELECT COUNT(long) FROM MyTable GROUP BY proctime")

    util.verifyPlan(result)
  }

  @Test
  def testAggregationOnProctime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sqlQuery("SELECT MIN(proctime) FROM MyTable GROUP BY long")

    util.verifyPlan(result)
  }

  @Test
  def testWindowSql(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery(
      "SELECT TUMBLE_END(rowtime, INTERVAL '0.1' SECOND) AS `rowtime`, `long`, " +
        "SUM(`int`) FROM MyTable " +
        "GROUP BY `long`, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    util.verifyPlan(result)
  }

  @Test
  def testWindowWithAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery("SELECT MIN(rowtime), long FROM MyTable " +
      "GROUP BY long, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    util.verifyPlan(result)
  }

  @Test
  def testWindowWithAggregationOnRowtimeWithHaving(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery(
      "SELECT MIN(rowtime), long FROM MyTable " +
      "GROUP BY long, TUMBLE(rowtime, INTERVAL '1' SECOND) " +
        "HAVING QUARTER(TUMBLE_END(rowtime, INTERVAL '1' SECOND))=1")

    util.verifyPlan(result)
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
      .join(proctimeRates('o_proctime), "currency = o_currency")
      .select("o_amount * rate, currency, proctime").as("converted_amount")
      .window(Tumble over 1.second on 'proctime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    util.verifyPlan(result)
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
      .join(proctimeRates('o_proctime), "currency = o_currency")
      .select("o_amount * rate, currency, o_proctime").as("converted_amount")
      .window(Tumble over 1.second on 'o_proctime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    util.verifyPlan(result)
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
      .join(proctimeRates('o_proctime), "currency = o_currency")
      .select("o_amount * rate, currency, o_proctime, o_rowtime").as("converted_amount")
      .window(Tumble over 1.second on 'o_rowtime as 'w)
      .groupBy('w, 'currency)
      .select('converted_amount.sum)

    util.verifyPlan(result)
  }
}

object TimeIndicatorConversionTest {

  class TableFunc extends TableFunction[String] {
    val t = new Timestamp(0L)
    def eval(time1: Long, time2: Timestamp, string: String): Unit = {
      collect(time1.toString + time2.after(t) + string)
    }
  }
}
