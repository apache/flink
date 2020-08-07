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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.{Expression, FieldReferenceExpression}
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo.{PROCTIME_INDICATOR, ROWTIME_INDICATOR}

import org.hamcrest.Matchers.{equalTo, startsWith}
import org.junit.Assert.{assertEquals, assertThat}
import org.junit.Test

import java.sql.Timestamp

class TemporalTableJoinTest extends TableTestBase {

  val util: TableTestUtil = streamTestUtil()

  val orders: Table = util.addDataStream[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime.rowtime)

  val ratesHistory: Table = util.addDataStream[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime.rowtime)

  val rates: TemporalTableFunction = ratesHistory.createTemporalTableFunction('rowtime, 'currency)
  util.addFunction("Rates", rates)

  val proctimeOrders: Table = util.addDataStream[(Long, String)](
    "ProctimeOrders", 'o_amount, 'o_currency, 'o_proctime.proctime)

  val proctimeRatesHistory: Table = util.addDataStream[(String, Int)](
    "ProctimeRatesHistory", 'currency, 'rate, 'proctime.proctime)

  val proctimeRates: TemporalTableFunction =
    proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency)

  @Test
  def testSimpleJoin(): Unit = {
    val result = orders
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyPlan(result)
  }

  @Test
  def testSimpleJoin2(): Unit = {
    val resultJava = orders
      .joinLateral(call("Rates", $"o_rowtime"), $"currency" === $"o_currency")
      .select($"o_amount" * $"rate").as("rate")

    util.verifyPlan(resultJava)
  }

  @Test
  def testSimpleProctimeJoin(): Unit = {
    val result = proctimeOrders
      .joinLateral(proctimeRates('o_proctime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

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
    val thirdTable = util.addDataStream[(String, Int)]("ThirdTable", 't3_comment, 't3_secondary_key)
    val orders = util.addDataStream[(Timestamp, String, Long, String, Int)](
      "Orders", 'rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)
      .as("o_rowtime", "o_comment", "o_amount", "o_currency", "o_secondary_key")

    val ratesHistory = util.addDataStream[(Timestamp, String, String, Int, Int)](
      "RatesHistory", 'rowtime, 'comment, 'currency, 'rate, 'secondary_key)
    val rates = ratesHistory
      .filter('rate > 110L)
      .createTemporalTableFunction('rowtime, 'currency)
    util.addFunction("Rates", rates)

    val result = orders
      .joinLateral(rates('o_rowtime))
      .filter('currency === 'o_currency || 'secondary_key === 'o_secondary_key)
      .select('o_amount * 'rate, 'secondary_key).as("rate", "secondary_key")
      .join(thirdTable, 't3_secondary_key === 'secondary_key)

    util.verifyPlan(result)
  }

  @Test
  def testTemporalTableFunctionOnTopOfQuery(): Unit = {
    val filteredRatesHistory = ratesHistory
      .filter('rate > 100)
      .select('currency, 'rate * 2, 'rowtime)
      .as("currency", "rate", "rowtime")

    val filteredRates = filteredRatesHistory.createTemporalTableFunction('rowtime, 'currency)
    util.addFunction("FilteredRates", filteredRates)

    val result = orders
      .joinLateral(filteredRates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate")
      .as("rate")

    util.verifyPlan(result)
  }

  @Test
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(startsWith("Unsupported argument"))

    val result = orders
      .joinLateral(rates(
        java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")),
        'o_currency === 'currency)
      .select($"o_amount" * $"rate")

    util.verifyPlan(result)
  }

  @Test
  def testProcessingTimeIndicatorVersion(): Unit = {
    assertRatesFunction(proctimeRatesHistory.getSchema, proctimeRates, true)
  }

  @Test
  def testValidStringFieldReference(): Unit = {
    val rates = ratesHistory.createTemporalTableFunction($"rowtime", $"currency")
    assertRatesFunction(ratesHistory.getSchema, rates)
  }

  private def assertRatesFunction(
      expectedSchema: TableSchema,
      inputRates: TemporalTableFunction,
      proctime: Boolean = false): Unit = {
    val rates = inputRates.asInstanceOf[TemporalTableFunctionImpl]
    assertThat(rates.getPrimaryKey,
      equalTo[Expression](new FieldReferenceExpression("currency", DataTypes.STRING(), 0, 0)))

    val (timeFieldName, timeFieldType) =
      if (proctime) {
        ("proctime", fromLegacyInfoToDataType(PROCTIME_INDICATOR))
      }
      else {
        ("rowtime", fromLegacyInfoToDataType(ROWTIME_INDICATOR))
      }

    assertThat(rates.getTimeAttribute,
      equalTo[Expression](new FieldReferenceExpression(timeFieldName, timeFieldType, 0, 2)))

    assertEquals(
      expectedSchema.toRowType,
      rates.getResultType)
  }

}

