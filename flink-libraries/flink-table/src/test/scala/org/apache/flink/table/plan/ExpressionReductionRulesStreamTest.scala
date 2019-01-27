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
import org.apache.flink.table.api.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.junit.{Ignore, Test}

class ExpressionReductionRulesStreamTest extends TableTestBase {

  @Test
  def testReduceCalcExpressionForStreamSQL(): Unit = {
    val util = streamTestUtil()
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
  def testReduceProjectExpressionForStreamSQL(): Unit = {
    val util = streamTestUtil()
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
  def testReduceFilterExpressionForStreamSQL(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT * FROM MyTable WHERE a>(1+7)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotReduceTimeFunctionWithProjectForStreamSQL(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sql =
      """
        |SELECT a+b, t1, t2, t3, t4, t5
        | FROM (
        |   SELECT a, b, c,
        |     CURRENT_TIMESTAMP as t1,
        |     CURRENT_TIME as t2,
        |     LOCALTIME as t3,
        |     LOCALTIMESTAMP as t4,
        |     CURRENT_DATE as t5
        |   FROM MyTable)
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testReduceConstantUdfForStreamSQL(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new ConstantFunc
    util.tableEnv.registerFunction("constant_fun", func)
    util.tableEnv.getConfig.getConf.setString(func.BASE_VALUE_KEY, "1000")

    val sqlQuery = "SELECT a, b, c, constant_fun() as f, constant_fun(500) as f500 " +
      "FROM MyTable WHERE a > constant_fun() AND b < constant_fun(500) AND constant_fun() = 1000"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReduceIllegalConstantUdfForStreamSQL(): Unit = {
    val util = streamTestUtil()
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
  def testReduceCalcExpressionForStreamTableAPI(): Unit = {
    val util = streamTestUtil()
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
  def testReduceProjectExpressionForStreamTableAPI(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result =  table
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
  def testReduceFilterExpressionForStreamTableAPI(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .where('a > (1 + 7))

    util.verifyPlan(result)
  }

  @Test
  def testReduceConstantUdfForStreamTableAPI(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new ConstantFunc
    util.tableEnv.getConfig.getConf.setString(func.BASE_VALUE_KEY, "1000")

    val result = table
      .where('a > func() && 'b < func(500) && func() === 1000)
      .select('a, 'b, 'c, func() as 'f, func(500) as 'f500)

    util.verifyPlan(result)
  }

  @Test
  def testReduceIllegalConstantUdfForStreamTableAPI(): Unit = {
    val util = streamTestUtil()
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
  def testNestedTablesReductionStream(): Unit = {
    val util = streamTestUtil()

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val newTable = util.tableEnv.sqlQuery("SELECT 1 + 1 + a AS a FROM MyTable")

    util.tableEnv.registerTable("NewTable", newTable)

    val sqlQuery = "SELECT a FROM NewTable"

    util.verifyPlan(sqlQuery)
  }

  // TODO this NPE is caused by Calcite, it shall pass when [CALCITE-1860] is fixed
  @Ignore
  def testReduceDeterministicUDF(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // if isDeterministic = true, will cause a Calcite NPE, which will be fixed in [CALCITE-1860]
    val result = table
      .select('a, 'b, 'c, DeterministicNullFunc() as 'd)
      .where("d.isNull")
      .select('a, 'b, 'c)

    util.verifyPlan(result)
  }

  @Test
  def testReduceNonDeterministicUDF(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .select('a, 'b, 'c, NonDeterministicNullFunc() as 'd)
      .where("d.isNull")
      .select('a, 'b, 'c)

    util.verifyPlan(result)
  }
}

object NonDeterministicNullFunc extends ScalarFunction {
  def eval(): String = null
  override def isDeterministic = false
}

object DeterministicNullFunc extends ScalarFunction {
  def eval(): String = null
  override def isDeterministic = true
}

class ConstantFunc extends ScalarFunction {
  val BASE_VALUE_KEY = "base.value"
  private var baseValue = Int.MinValue

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    val p = context.getJobParameter(BASE_VALUE_KEY, null)
    require(p != null)
    baseValue = p.toInt
  }

  def eval(): Int = baseValue

  def eval(add: Int): Int = baseValue + add
}

class IllegalConstantFunc extends ScalarFunction {
  var context: FunctionContext = _

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    this.context = context
  }

  // It's illegal that constant udf accesses context info
  // which will be initialized after job is submitted.
  def eval(methodName: String): Int = {
    try {
      methodName match {
        case "getMetricGroup" => try {
          context.getMetricGroup
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 1
        }
        case "getCachedFile" => try {
          context.getCachedFile("/tmp")
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 2
        }
        case "getNumberOfParallelSubtasks" => try {
          context.getNumberOfParallelSubtasks()
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 3
        }
        case "getIndexOfThisSubtask" => try {
          context.getIndexOfThisSubtask()
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 4
        }
        case "getIntCounter" => try {
          context.getIntCounter("name")
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 5
        }
        case "getLongCounter" => try {
          context.getLongCounter("name")
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 6
        }
        case "getDoubleCounter" => try {
          context.getDoubleCounter("name")
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 7
        }
        case "getHistogram" => try {
          context.getHistogram("name")
          Int.MaxValue
        } catch {
          case _: UnsupportedOperationException => 8
        }
        case _ => 1000
      }
    }
  }
}
