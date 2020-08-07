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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.plan.logical.rel.LogicalTemporalTableJoin.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.utils.TableTestUtil.{binaryNode, streamTableNode, term, unaryNode}
import org.apache.flink.table.utils._

import org.hamcrest.Matchers.startsWith
import org.junit.Test

import java.sql.Timestamp

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

  val proctimeRates = util.addFunction(
    "ProctimeRates",
    proctimeRatesHistory.createTemporalTableFunction('proctime, 'currency))

  @Test
  def testSimpleJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o, " +
      "LATERAL TABLE (Rates(o.o_rowtime)) AS r " +
      "WHERE currency = o_currency"

    util.verifySql(sqlQuery, getExpectedSimpleJoinPlan())
  }

  @Test
  def testSimpleProctimeJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM ProctimeOrders AS o, " +
      "LATERAL TABLE (ProctimeRates(o.o_proctime)) AS r " +
      "WHERE currency = o_currency"

    util.verifySql(sqlQuery, getExpectedSimpleProctimeJoinPlan())
  }

  @Test
  def testJoinOnQueryLeft(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM (SELECT * FROM Orders WHERE o_amount > 1000) AS o, " +
      "LATERAL TABLE (Rates(o.o_rowtime)) AS r " +
      "WHERE currency = o_currency"

    val expected = unaryNode(
      "DataStreamCalc",
      binaryNode(
        "DataStreamTemporalTableJoin",
        unaryNode("DataStreamCalc",
          streamTableNode(orders),
          term("select", "o_amount, o_currency, o_rowtime"),
          term("where", ">(o_amount, 1000)")),
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
    util.verifySql(sqlQuery, expected)
  }

  /**
    * Test versioned joins with more complicated query.
    * Important thing here is that we have complex OR join condition
    * and there are some columns that are not being used (are being pruned).
    */
  @Test
  def testComplexJoin(): Unit = {
    val util = streamTestUtil()
    val thirdTable = util.addTable[(String, Int)]("Table3", 't3_comment, 't3_secondary_key)
    val orders = util.addTable[(Timestamp, String, Long, String, Int)](
      "Orders", 'o_rowtime.rowtime, 'o_comment, 'o_amount, 'o_currency, 'o_secondary_key)

    val ratesHistory = util.addTable[(Timestamp, String, String, Int, Int)](
      "RatesHistory", 'rowtime.rowtime, 'comment, 'currency, 'rate, 'secondary_key)
    val rates = ratesHistory
      .filter('rate > 110L)
      .createTemporalTableFunction('rowtime, 'currency)
    util.addFunction("Rates", rates)

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

    util.verifySql(sqlQuery, binaryNode(
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
  def testUncorrelatedJoin(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(startsWith("Cannot generate a valid execution plan"))

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o, " +
      "LATERAL TABLE (Rates(TIMESTAMP '2016-06-27 10:10:42.123')) AS r " +
      "WHERE currency = o_currency"

    util.printSql(sqlQuery)
  }

  @Test
  def testTemporalTableFunctionScan(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(startsWith("Cannot generate a valid execution plan"))

    val sqlQuery = "SELECT * FROM LATERAL TABLE (Rates(TIMESTAMP '2016-06-27 10:10:42.123'))"

    util.printSql(sqlQuery)
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
