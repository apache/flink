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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.hamcrest.Matchers.containsString
import org.junit.Test

import java.sql.Timestamp

class TemporalJoinTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()

  private val orders = util.addDataStream[(Long, String)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime.rowtime)

  private val ratesHistory = util.addDataStream[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime.rowtime)

  util.addFunction(
    "Rates",
    ratesHistory.createTemporalTableFunction($"rowtime", $"currency"))

  private val proctimeOrders = util.addDataStream[(Long, String)](
    "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime)

  private val proctimeRatesHistory = util.addDataStream[(String, Int)](
    "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

  util.addFunction(
    "ProctimeRates",
    proctimeRatesHistory.createTemporalTableFunction($"proctime", $"currency"))

  @Test
  def testSimpleJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o, " +
      "LATERAL TABLE (Rates(o.o_rowtime)) AS r " +
      "WHERE currency = o_currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleProctimeJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM ProctimeOrders AS o, " +
      "LATERAL TABLE (ProctimeRates(o.o_proctime)) AS r " +
      "WHERE currency = o_currency"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinOnQueryLeft(): Unit = {
    val orders = util.tableEnv.sqlQuery("SELECT * FROM Orders WHERE o_amount > 1000")
    util.tableEnv.createTemporaryView("Orders2", orders)

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders2 AS o, " +
      "LATERAL TABLE (Rates(o.o_rowtime)) AS r " +
      "WHERE currency = o_currency"

    util.verifyPlan(sqlQuery)
  }

  /**
    * Test versioned joins with more complicated query.
    * Important thing here is that we have complex OR join condition
    * and there are some columns that are not being used (are being pruned).
    */
  @Test
  def testComplexJoin(): Unit = {
    val util = streamTestUtil()
    util.addDataStream[(String, Int)]("Table3", 't3_comment, 't3_secondary_key)
    util.addDataStream[(Timestamp, String, Long, String, Int)](
      "Orders", 'o_rowtime.rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)

    util.addDataStream[(Timestamp, String, String, Int, Int)](
      "RatesHistory", 'rowtime.rowtime, 'comment, 'currency, 'rate, 'secondary_key)
    val rates = util.tableEnv
      .sqlQuery("SELECT * FROM RatesHistory WHERE rate > 110")
      .createTemporalTableFunction($"rowtime", $"currency")
    util.addTemporarySystemFunction("Rates", rates)

    val sqlQuery =
      "SELECT * FROM " +
        "(SELECT " +
        "o_amount * rate as rate, " +
        "secondary_key as secondary_key " +
        "FROM Orders AS o, " +
        "LATERAL TABLE (Rates(o_rowtime)) AS r " +
        "WHERE currency = o_currency OR secondary_key = o_secondary_key), " +
        "Table3 " +
        "WHERE t3_secondary_key = secondary_key"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(containsString("Cannot generate a valid execution plan"))

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o, " +
      "LATERAL TABLE (Rates(TIMESTAMP '2016-06-27 10:10:42.123')) AS r " +
      "WHERE currency = o_currency"

    util.verifyExplain(sqlQuery)
  }

  @Test
  def testTemporalTableFunctionScan(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(containsString("Cannot generate a valid execution plan"))

    val sqlQuery = "SELECT * FROM LATERAL TABLE (Rates(TIMESTAMP '2016-06-27 10:10:42.123'))"

    util.verifyExplain(sqlQuery)
  }
}
