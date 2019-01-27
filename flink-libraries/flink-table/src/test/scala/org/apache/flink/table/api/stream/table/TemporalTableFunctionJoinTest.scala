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
package org.apache.flink.table.api.stream.table

import java.sql.Timestamp
import org.apache.flink.api.scala._
import org.apache.flink.table.api.functions.TemporalTableFunction
import org.apache.flink.table.api.{TableException, TableSchema, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.Test

class TemporalTableFunctionJoinTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()

  val orders = util.addTable[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime.rowtime)

  val ratesHistory = util.addTable[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime.rowtime)

  val rates = util.addFunction(
    "Rates",
    ratesHistory.createTemporalTableFunction('rowtime, 'currency))

  val proctimeOrders = util.addTable[(Long, String)](
    "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime)

  val proctimeRatesHistory = util.addTable[(String, Int)](
    "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

  val proctimeRates = proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency)

  @Test
  def testSimpleJoin(): Unit = {
    val result = orders
      .join(rates('o_rowtime), "currency = o_currency")
      .select("o_amount * rate").as("rate")

    util.verifyPlan(result)
  }

  @Test
  def testSimpleProctimeJoin(): Unit = {
    val result = proctimeOrders
      .join(proctimeRates('o_proctime), "currency = o_currency")
      .select("o_amount * rate").as("rate")

    util.verifyPlan(result)
  }

  /**
    * Test versioned joins with more complicated query.
    * Important thing here is that we have complex OR join condition
    * and there are some columns that are not being used (are being pruned).
    */
  @Test
  def testComplexJoin(): Unit = {
    val util = streamTestUtil()
    val thirdTable = util.addTable[(String, Int)]("ThirdTable", 't3_comment, 't3_secondary_key)
    val orders = util.addTable[(Timestamp, String, Long, String, Int)](
      "Orders", 'o_rowtime.rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)

    val ratesHistory = util.addTable[(Timestamp, String, String, Int, Int)](
      "RatesHistory", 'rowtime.rowtime, 'comment, 'currency, 'rate, 'secondary_key)
    val rates = ratesHistory
      .filter('rate > 110L)
      .createTemporalTableFunction('rowtime, 'currency)
    util.addFunction("Rates", rates)

    val result = orders
      .join(rates('o_rowtime))
      .filter('currency === 'o_currency || 'secondary_key === 'o_secondary_key)
      .select('o_amount * 'rate, 'secondary_key).as('rate, 'secondary_key)
      .join(thirdTable, 't3_secondary_key === 'secondary_key)

    util.verifyPlan(result)
  }

  @Test
  def testTemporalTableFunctionOnTopOfQuery(): Unit = {
    val filteredRatesHistory = ratesHistory
      .filter('rate > 100)
      .select('currency, 'rate * 2, 'rowtime)
      .as('currency, 'rate, 'rowtime)

    val filteredRates = filteredRatesHistory.createTemporalTableFunction('rowtime, 'currency)
    util.addFunction("FilteredRates",filteredRates)

    val result = orders
      .join(filteredRates('o_rowtime), "currency = o_currency")
      .select("o_amount * rate")
      .as('rate)

    util.verifyPlan(result)
  }

  @Test
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(containsString("Cannot generate a valid execution plan"))

    val result = orders
      .join(rates(
        java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")),
            "o_currency = currency")
      .select("o_amount * rate")

    util.explain(result)
  }

  @Test
  def testTemporalTableFunctionScan(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Cannot translate a query with an unbounded table function call")

    val result = rates(java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123"))
    util.explain(result)
  }

  @Test
  def testProcessingTimeIndicatorVersion(): Unit = {
    assertRatesFunction(proctimeRatesHistory.getSchema, proctimeRates, true)
  }

  @Test
  def testValidStringFieldReference(): Unit = {
    val rates = ratesHistory.createTemporalTableFunction("rowtime", "currency")
    assertRatesFunction(ratesHistory.getSchema, rates)
  }

  private def assertRatesFunction(
      expectedSchema: TableSchema,
      rates: TemporalTableFunction,
      proctime: Boolean = false): Unit = {
    assertEquals("currency", rates.getPrimaryKey)
    assertTrue(rates.getTimeAttribute.isInstanceOf[ResolvedFieldReference])
    assertEquals(
      if (proctime) "proctime" else "rowtime",
      rates.getTimeAttribute.asInstanceOf[ResolvedFieldReference].name)
    assertArrayEquals(
      expectedSchema.getColumnNames.asInstanceOf[Array[Object]],
      rates.getResultType(null, null).getFieldNames.asInstanceOf[Array[Object]])
    assertArrayEquals(
      expectedSchema.getTypes.asInstanceOf[Array[Object]],
      rates.getResultType(null, null).getFieldTypes.asInstanceOf[Array[Object]])
  }
}
