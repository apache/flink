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

package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.Test

import java.sql.Timestamp

class TemporalTableJoinValidationTest extends TableTestBase {

  val util: TableTestUtil = streamTestUtil()

  val orders: Table = util.addDataStream[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime.rowtime)

  val ordersProctime: Table = util.addDataStream[(Long, String)](
    "OrdersProctime", 'o_amount, 'o_currency, 'o_rowtime.proctime)

  val ordersWithoutTimeAttribute: Table = util.addDataStream[(Long, String, Timestamp)](
    "OrdersWithoutTimeAttribute", 'o_amount, 'o_currency, 'o_rowtime)

  val ratesHistory: Table = util.addDataStream[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime.rowtime)

  val ratesHistoryWithoutTimeAttribute: Table = util.addDataStream[(String, Int, Timestamp)](
    "ratesHistoryWithoutTimeAttribute", 'currency, 'rate, 'rowtime)

  @Test
  def testInvalidFieldReference(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [foobar]")

    ratesHistory.createTemporalTableFunction('rowtime, 'foobar)
  }

  @Test
  def testInvalidStringFieldReference(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [foobar]")

    ratesHistory.createTemporalTableFunction($"rowtime", $"foobar")
  }

  @Test
  def testNonTimeIndicatorOnRightSide(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Non rowtime timeAttribute [TIMESTAMP(3)] used to create TemporalTableFunction")

    val rates = ratesHistoryWithoutTimeAttribute.createTemporalTableFunction('rowtime, 'currency)

    val result = orders
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyExplain(result)
  }

  @Test
  def testNonTimeIndicatorOnLeftSide(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Non rowtime timeAttribute [TIMESTAMP(3)] passed as the argument to TemporalTableFunction")

    val rates = ratesHistory.createTemporalTableFunction('rowtime, 'currency)

    val result = ordersWithoutTimeAttribute
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyExplain(result)
  }

  @Test
  def testMixedTimeIndicators(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Non rowtime timeAttribute [TIMESTAMP_LTZ(3) *PROCTIME*] passed as the argument " +
        "to TemporalTableFunction")

    val rates = ratesHistory.createTemporalTableFunction('rowtime, 'currency)

    val result = ordersProctime
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyExplain(result)
  }
}
