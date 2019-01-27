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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class ExpressionReductionRulesBatchExecTest extends TableTestBase {

  @Test
  def testReduceCalcExpressionForBatchSQL(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable WHERE a>(1+7)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceProjectExpressionForBatchSQL(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceFilterExpressionForBatchSQL(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT * FROM MyTable WHERE a>(1+7)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceConstantUdfForBatchSQL(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new ConstantFunc
    util.tableEnv.registerFunction("constant_fun", func)
    util.tableEnv.getConfig.getConf.setString(func.BASE_VALUE_KEY, "1000")

    val sqlQuery = "SELECT a, b, c, constant_fun() as f, constant_fun(500) as f500 " +
      "FROM MyTable WHERE a > constant_fun() AND b < constant_fun(500) AND constant_fun() = 1000"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceIllegalConstantUdfForBatchSQL(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("constant_fun", new IllegalConstantFunc)

    val sqlQuery =
      """
        | SELECT a,
        |  constant_fun('getMetricGroup') as f1,
        |  constant_fun('getCachedFile') as f2,
        |  constant_fun('getNumberOfParallelSubtasks') as f3,
        |  constant_fun('getIndexOfThisSubtask') as f4,
        |  constant_fun('getIntCounter') as f5,
        |  constant_fun('getLongCounter') as f6,
        |  constant_fun('getDoubleCounter') as f7,
        |  constant_fun('getHistogram') as f8
        | FROM MyTable
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceCalcExpressionForBatchTableAPI(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .where('a > (1 + 7))
      .select((3 + 4).toExpr + 6,
        (11 === 1) ? ("a", "b"),
        " STRING ".trim,
        "test" + "string",
        "1990-10-14 23:00:00.123".toTimestamp + 10.days + 1.second,
        1.isNull,
        "TEST".like("%EST"),
        2.5.toExpr.floor(),
        true.cast(DataTypes.STRING) + "X")

    util.verifyPlan(result)
  }

  @Test
  def testReduceProjectExpressionForBatchTableAPI(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .select((3 + 4).toExpr + 6,
        (11 === 1) ? ("a", "b"),
        " STRING ".trim,
        "test" + "string",
        "1990-10-14 23:00:00.123".toTimestamp + 10.days + 1.second,
        1.isNull,
        "TEST".like("%EST"),
        2.5.toExpr.floor(),
        true.cast(DataTypes.STRING) + "X")

    util.verifyPlan(result)
  }

  @Test
  def testReduceFilterExpressionForBatchTableAPI(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .where('a > (1 + 7))

    util.verifyPlan(result)
  }

  @Test
  def testReduceConstantUdfForBatchTableAPI(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new ConstantFunc
    util.tableEnv.getConfig.getConf.setString(func.BASE_VALUE_KEY, "1000")

    val result = table
      .where('a > func() && 'b < func(500) && func() === 1000)
      .select('a, 'b, 'c, func() as 'f, func(500) as 'f500)

    util.verifyPlan(result)
  }

  @Test
  def testReduceIllegalConstantUdfForBatchTableAPI(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new IllegalConstantFunc

    val result = table.select('a,
      func("getMetricGroup") as 'f1,
      func("getCachedFile") as 'f2,
      func("getNumberOfParallelSubtasks") as 'f3,
      func("getIndexOfThisSubtask") as 'f4,
      func("getIntCounter") as 'f5,
      func("getLongCounter") as 'f6,
      func("getDoubleCounter") as 'f7,
      func("getHistogram") as 'f8
    )

    util.verifyPlan(result)
  }

  @Test
  def testNestedTablesReductionBatch(): Unit = {
    val util = batchTestUtil()

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val newTable = util.tableEnv.sqlQuery("SELECT 1 + 1 + a AS a FROM MyTable")

    util.tableEnv.registerTable("NewTable", newTable)

    val sqlQuery = "SELECT a FROM NewTable"

    util.verifyPlan(sqlQuery)
  }
}
