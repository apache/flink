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

package org.apache.flink.api.scala.batch.table

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.codegen.CodeGenException
import org.apache.flink.api.table.expressions.Null
import org.apache.flink.api.table.{Row, TableEnvironment, Types, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ExpressionsITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((5, 10)).toTable(tEnv, 'a, 'b)
      .select('a - 5, 'a + 5, 'a / 2, 'a * 2, 'a % 2, -'a, 3.toExpr + 'a)

    val expected = "0,10,2,10,1,-5,8"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLogic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((5, true)).toTable(tEnv, 'a, 'b)
      .select('b && true, 'b && false, 'b || false, !'b)

    val expected = "true,false,true,false"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testComparisons(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((5, 5, 4)).toTable(tEnv, 'a, 'b, 'c)
      .select('a > 'c, 'a >= 'b, 'a < 'c, 'a.isNull, 'a.isNotNull, 12.toExpr <= 'a)

    val expected = "true,true,false,false,true,false"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCaseInsensitiveForAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((3, 5.toByte)).toTable(tEnv, 'a, 'b)
      .groupBy("a").select("a, a.count As cnt")

    val expected = "3,1"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNullLiteral(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((1, 0)).toTable(tEnv, 'a, 'b)
      .select(
        'a,
        'b,
        Null(Types.INT),
        Null(Types.STRING) === "")

    try {
      val ds = t.toDataSet[Row]
      if (!config.getNullCheck) {
        fail("Exception expected if null check is disabled.")
      }
      val results = ds.collect()
      val expected = "1,0,null,null"
      TestBaseUtils.compareResultAsText(results.asJava, expected)
    }
    catch {
      case e: CodeGenException =>
        if (config.getNullCheck) {
          throw e
        }
    }
  }

  @Test
  def testIf(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((5, true)).toTable(tEnv, 'a, 'b)
      .select(
        ('b && true).?("true", "false"),
        false.?("true", "false"),
        true.?(true.?(true.?(10, 4), 4), 4))

    val expected = "true,false,10"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testIfInvalidTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env.fromElements((5, true)).toTable(tEnv, 'a, 'b)
      .select(('b && true).?(5, "false"))

    val expected = "true,false,3,10"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = env
      .fromElements((
        BigDecimal("78.454654654654654").bigDecimal,
        BigDecimal("4E+9999").bigDecimal,
        Date.valueOf("1984-07-12"),
        Time.valueOf("14:34:24"),
        Timestamp.valueOf("1984-07-12 14:34:24")))
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c, 'd, 'e, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
        Date.valueOf("1984-07-12"), Time.valueOf("14:34:24"),
        Timestamp.valueOf("1984-07-12 14:34:24"))

    val expected = "78.454654654654654,4E+9999,1984-07-12,14:34:24,1984-07-12 14:34:24.0," +
      "11.2,11.2,1984-07-12,14:34:24,1984-07-12 14:34:24.0"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
