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
package org.apache.flink.table.api.batch.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils._
import org.hamcrest.Matchers.startsWith
import org.junit.Test

class TemporalTableJoinTest extends TableTestBase {

  val util: TableTestUtil = batchTestUtil()

  val orders = util.addTable[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime)

  val ratesHistory = util.addTable[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime)

  val rates = util.addFunction(
    "Rates",
    ratesHistory.createTemporalTableFunction('rowtime, 'currency))

  @Test
  def testSimpleJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Cannot generate a valid execution plan for the given query")

    val result = orders
      .joinLateral(rates('o_rowtime), 'currency === 'o_currency)
      .select($"o_amount" * $"rate").as("rate")

    util.printTable(result)
  }

  @Test
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(startsWith("Unsupported argument"))

    val result = orders
      .joinLateral(
        rates(java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")),
        'o_currency === 'currency)
      .select($"o_amount" * $"rate")

    util.printTable(result)
  }
}
