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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

/**
 * Test temporal join in batch mode.
 *
 * <p> Flink only supports lookup join in batch mode, the others Temporal join is not supported yet.
 */
class TemporalJoinTest extends TableTestBase {

  val util: BatchTableTestUtil = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTable(
      """
        |CREATE TABLE Orders (
        | o_amount INT,
        | o_currency STRING,
        | o_rowtime TIMESTAMP(3),
        | o_proctime as PROCTIME(),
        | WATERMARK FOR o_rowtime AS o_rowtime
        |) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesHistory (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime
        |) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesHistoryWithPK (
        | currency STRING,
        | rate INT,
        | rowtime TIMESTAMP(3),
        | WATERMARK FOR rowtime AS rowtime,
        | PRIMARY KEY(currency) NOT ENFORCED
        |) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesOnly (
        | currency STRING,
        | rate INT,
        | proctime AS PROCTIME()
        |) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      " CREATE VIEW rates_last_row_rowtime as SELECT currency, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
        "   FROM RatesHistory" +
        "  ) T " +
        "  WHERE rowNum = 1")

    util.addTable(
      " CREATE VIEW rates_last_row_proctime as SELECT currency, rate, proctime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum " +
        "   FROM RatesOnly" +
        "  ) T" +
        "  WHERE rowNum = 1")

    util.addTable("CREATE VIEW rates_last_value AS SELECT currency, LAST_VALUE(rate) AS rate " +
      "FROM RatesHistory " +
      "GROUP BY currency ")
  }

  @Test(expected = classOf[TableException])
  def testSimpleJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "RatesHistoryWithPK FOR SYSTEM_TIME AS OF o.o_rowtime as r " +
      "on o.o_currency = r.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleRowtimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_rowtime " +
      "FOR SYSTEM_TIME AS OF o.o_rowtime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleProctimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "rates_last_row_proctime " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleViewProcTimeJoin(): Unit = {

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "rates_last_value " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyExecPlan(sqlQuery)
  }
}
