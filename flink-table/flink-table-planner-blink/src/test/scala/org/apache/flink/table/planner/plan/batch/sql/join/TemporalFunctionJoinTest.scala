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
package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}

import org.hamcrest.Matchers.containsString
import org.junit.Test

import java.sql.Timestamp

class TemporalFunctionJoinTest extends TableTestBase {

  val util: BatchTableTestUtil = batchTestUtil()

  val orders = util.addDataStream[(Long, String, Timestamp)](
    "Orders", 'o_amount, 'o_currency, 'o_rowtime)

  val ratesHistory = util.addDataStream[(String, Int, Timestamp)](
    "RatesHistory", 'currency, 'rate, 'rowtime)

  val rates = util.addTemporarySystemFunction(
    "Rates",
    ratesHistory.createTemporalTableFunction($"rowtime", $"currency"))

  @Test
  def testSimpleJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Cannot generate a valid execution plan for the given query")

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o, " +
      "LATERAL TABLE (Rates(o_rowtime)) AS r " +
      "WHERE currency = o_currency"

    util.verifyExplain(sqlQuery)
  }

  /**
    * Test temporal table joins with more complicated query.
    * Important thing here is that we have complex OR join condition
    * and there are some columns that are not being used (are being pruned).
    */
  @Test(expected = classOf[TableException])
  def testComplexJoin(): Unit = {
    val util = batchTestUtil()
    util.addDataStream[(String, Int)]("Table3", 't3_comment, 't3_secondary_key)
    util.addDataStream[(Timestamp, String, Long, String, Int)](
      "Orders", 'o_rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)

    val ratesHistory = util.addDataStream[(Timestamp, String, String, Int, Int)](
      "RatesHistory", 'rowtime, 'comment, 'currency, 'rate, 'secondary_key)
    val rates = ratesHistory.createTemporalTableFunction($"rowtime", $"currency")
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

    util.verifyExplain(sqlQuery)
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

    val sqlQuery = "SELECT * FROM LATERAL TABLE (Rates(TIMESTAMP '2016-06-27 10:10:42.123'))";

    util.verifyExplain(sqlQuery)
  }
}
