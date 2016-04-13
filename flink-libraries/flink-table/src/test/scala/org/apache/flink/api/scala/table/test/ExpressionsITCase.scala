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

package org.apache.flink.api.scala.table.test

import java.util.Date

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.{Literal, Null}
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ExpressionsITCase(
    mode: TestExecutionMode,
    config: TableConfigMode)
  extends TableProgramsTestBase(mode, config) {

  @Test
  def testArithmetic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((5, 10)).as('a, 'b)
      .select('a - 5, 'a + 5, 'a / 2, 'a * 2, 'a % 2, -'a)

    val expected = "0,10,2,10,1,-5"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLogic(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((5, true)).as('a, 'b)
      .select('b && true, 'b && false, 'b || false, !'b)

    val expected = "true,false,true,false"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testComparisons(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((5, 5, 4)).as('a, 'b, 'c)
      .select('a > 'c, 'a >= 'b, 'a < 'c, 'a.isNull, 'a.isNotNull)

    val expected = "true,true,false,false,true"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCaseInsensitiveForAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val t = env.fromElements((3, 5.toByte)).as('a, 'b)
      .groupBy("a").select("a, a.count As cnt")

    val expected = "3,1"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNullLiteral(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val t = env.fromElements((1, 0)).as('a, 'b)
      .select(
        'a,
        'b,
        Null(BasicTypeInfo.INT_TYPE_INFO),
        Null(BasicTypeInfo.STRING_TYPE_INFO) === "")

    val expected = if (getConfig.getNullCheck) {
      "1,0,null,null"
    } else {
      "1,0,-1,true"
    }
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // Date literals not yet supported
  @Ignore
  @Test
  def testDateLiteral(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val t = env.fromElements((0L, "test")).as('a, 'b)
      .select('a,
        Literal(new Date(0)).cast(BasicTypeInfo.STRING_TYPE_INFO),
        'a.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO))

    val expected = "0,1970-01-01 00:00:00.000,1970-01-01 00:00:00.000"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
