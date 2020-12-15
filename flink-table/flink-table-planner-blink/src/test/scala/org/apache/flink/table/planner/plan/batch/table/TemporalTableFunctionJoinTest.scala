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
package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.hamcrest.Matchers.containsString
import org.junit.Test

import java.sql.Timestamp

class TemporalTableFunctionJoinTest extends TableTestBase {

  val util: TableTestUtil = batchTestUtil()

  val orders = util.addDataStream[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'rowtime)

  val ratesHistory = util.addDataStream[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime)

  val rates = ratesHistory.createTemporalTableFunction('rowtime, 'currency)
  util.addFunction("Rates", rates)

  @Test
  def testSimpleJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Cannot generate a valid execution plan for the given query")

    val result = orders
      .as("o_amount", "o_currency", "o_rowtime")
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.verifyExecPlan(result)
  }

  @Test
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      containsString("Cannot generate a valid execution plan"))

    val result = orders
      .as("o_amount", "o_currency", "o_rowtime")
      .joinLateral(
        rates(java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")),
        'o_currency === 'currency)
      .select($"o_amount" * $"rate")

    util.verifyExecPlan(result)
  }

}
