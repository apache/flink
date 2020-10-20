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
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'true'
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
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'true'
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
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      """
        |CREATE TABLE RatesOnly (
        | currency STRING,
        | rate INT,
        | proctime AS PROCTIME()
        |) WITH (
        | 'connector' = 'COLLECTION',
        | 'is-bounded' = 'true'
        |)
      """.stripMargin)

    util.addTable(
      " CREATE VIEW DeduplicatedView as SELECT currency, rate, rowtime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rowtime DESC) AS rowNum " +
        "   FROM RatesHistory" +
        "  ) T " +
        "  WHERE rowNum = 1")

    util.addTable(
      " CREATE VIEW latestView as SELECT currency, rate, proctime FROM " +
        "  (SELECT *, " +
        "          ROW_NUMBER() OVER (PARTITION BY currency ORDER BY proctime DESC) AS rowNum " +
        "   FROM RatesOnly" +
        "  ) T" +
        "  WHERE rowNum = 1")

    util.addTable("CREATE VIEW latest_rates AS SELECT currency, LAST_VALUE(rate) AS rate " +
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

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleRowtimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "DeduplicatedView " +
      "FOR SYSTEM_TIME AS OF o.o_rowtime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleProctimeVersionedViewJoin(): Unit = {
    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "latestView " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSimpleViewProcTimeJoin(): Unit = {

    val sqlQuery = "SELECT " +
      "o_amount * rate as rate " +
      "FROM Orders AS o JOIN " +
      "latest_rates " +
      "FOR SYSTEM_TIME AS OF o.o_proctime as r1 " +
      "on o.o_currency = r1.currency"

    util.verifyPlan(sqlQuery)
  }
}
