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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, TableSchema, ValidationException}
import org.apache.flink.table.expressions.{Expression, FieldReferenceExpression}
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin._
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo.{PROCTIME_INDICATOR, ROWTIME_INDICATOR}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils._
import org.hamcrest.Matchers.{equalTo, startsWith}
import org.junit.Assert.{assertEquals, assertThat}
import org.junit.Test

class TemporalTableJoinTest extends TableTestBase {

  val util: TableTestUtil = streamTestUtil()

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
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyTable(result, getExpectedSimpleJoinPlan())

    val resultJava = orders
      .joinLateral(call("Rates", $"o_rowtime"), $"currency" === $"o_currency")
      .select($"o_amount" * $"rate").as("rate")

    util.verifyTable(resultJava, getExpectedSimpleJoinPlan())
  }

  @Test
  def testSimpleProctimeJoin(): Unit = {
    val result = proctimeOrders
      .joinLateral(proctimeRates('o_proctime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyTable(result, getExpectedSimpleProctimeJoinPlan())
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
      .joinLateral(rates('o_rowtime))
      .filter('currency === 'o_currency || 'secondary_key === 'o_secondary_key)
      .select('o_amount * 'rate, 'secondary_key).as("rate", "secondary_key")
      .join(thirdTable, 't3_secondary_key === 'secondary_key)

    util.verifyTable(result, binaryNode(
      "DataStreamJoin",
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamTemporalTableJoin",
          streamTableNode(orders),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(ratesHistory),
            term("select", "rowtime, comment, currency, rate, secondary_key"),
            term("where", ">(rate, 110:BIGINT)")
          ),
          term(
            "where",
            "AND(" +
              s"${TEMPORAL_JOIN_CONDITION.getName}(o_rowtime, rowtime, currency), " +
              "OR(=(currency, o_currency), =(secondary_key, o_secondary_key)))"),
          term(
            "join",
            "o_rowtime",
            "o_comment",
            "o_amount",
            "o_currency",
            "o_secondary_key",
            "rowtime",
            "comment",
            "currency",
            "rate",
            "secondary_key"),
          term("joinType", "InnerJoin")
        ),
        term("select", "*(o_amount, rate) AS rate", "secondary_key")
      ),
      streamTableNode(thirdTable),
      term("where", "=(t3_secondary_key, secondary_key)"),
      term("join", "rate, secondary_key, t3_comment, t3_secondary_key"),
      term("joinType", "InnerJoin")
    ))
  }

  @Test
  def testTemporalTableFunctionOnTopOfQuery(): Unit = {
    val filteredRatesHistory = ratesHistory
      .filter('rate > 100)
      .select('currency, 'rate * 2, 'rowtime)
      .as("currency", "rate", "rowtime")

    val filteredRates = util.addFunction(
      "FilteredRates",
      filteredRatesHistory.createTemporalTableFunction('rowtime, 'currency))

    val result = orders
      .joinLateral(filteredRates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate")
      .as("rate")

    util.verifyTable(result, getExpectedTemporalTableFunctionOnTopOfQueryPlan())
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

    util.printTable(result)
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

  def getExpectedSimpleJoinPlan(): String = {
    unaryNode(
      "DataStreamCalc",
      binaryNode(
        "DataStreamTemporalTableJoin",
        streamTableNode(orders),
        streamTableNode(ratesHistory),
        term("where",
          "AND(" +
            s"${TEMPORAL_JOIN_CONDITION.getName}(o_rowtime, rowtime, currency), " +
            "=(currency, o_currency))"),
        term("join", "o_amount", "o_currency", "o_rowtime", "currency", "rate", "rowtime"),
        term("joinType", "InnerJoin")
      ),
      term("select", "*(o_amount, rate) AS rate")
    )
  }

  def getExpectedSimpleProctimeJoinPlan(): String = {
    unaryNode(
      "DataStreamCalc",
      binaryNode(
        "DataStreamTemporalTableJoin",
        streamTableNode(proctimeOrders),
        streamTableNode(proctimeRatesHistory),
        term("where",
          "AND(" +
            s"${TEMPORAL_JOIN_CONDITION.getName}(o_proctime, currency), " +
            "=(currency, o_currency))"),
        term("join", "o_amount", "o_currency", "o_proctime", "currency", "rate", "proctime"),
        term("joinType", "InnerJoin")
      ),
      term("select", "*(o_amount, rate) AS rate")
    )
  }

  def getExpectedTemporalTableFunctionOnTopOfQueryPlan(): String = {
    unaryNode(
      "DataStreamCalc",
      binaryNode(
        "DataStreamTemporalTableJoin",
        streamTableNode(orders),
        unaryNode(
          "DataStreamCalc",
          streamTableNode(ratesHistory),
          term("select", "currency", "*(rate, 2) AS rate", "rowtime"),
          term("where", ">(rate, 100)")),
        term("where",
          "AND(" +
            s"${TEMPORAL_JOIN_CONDITION.getName}(o_rowtime, rowtime, currency), " +
            "=(currency, o_currency))"),
        term("join", "o_amount", "o_currency", "o_rowtime", "currency", "rate", "rowtime"),
        term("joinType", "InnerJoin")
      ),
      term("select", "*(o_amount, rate) AS rate")
    )
  }
}

