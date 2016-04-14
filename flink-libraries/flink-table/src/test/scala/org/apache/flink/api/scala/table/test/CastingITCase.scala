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
import org.apache.flink.api.table.{TableEnvironment, Row}
import org.apache.flink.api.table.codegen.CodeGenException
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CastingITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testNumericAutoCastInArithmetic(): Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, 1L, 1001.1)).toTable(tEnv)
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f, '_5 + 1.0d, '_6 + 1, '_7 + 1.0d, '_8 + '_1)

    val expected = "2,2,2,2.0,2.0,2.0,2.0,1002.1"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNumericAutoCastInComparison(): Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d)).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f)
      .filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d  && 'f > 1)

    val expected = "2,2,2,2,2.0,2.0"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Ignore // TODO support advanced String operations
  @Test
  def testAutoCastToString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, new Date(0))).toTable(tEnv)
      .select('_1 + "b", '_2 + "s", '_3 + "i", '_4 + "L", '_5 + "f", '_6 + "d", '_7 + "Date")

    val expected = "1b,1s,1i,1L,1.0f,1.0d,1970-01-01 00:00:00.000Date"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCasting(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements((1, 0.0, 1L, true))
      .toTable(tEnv)
      .select(
        // * -> String
        '_1.cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_2.cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_3.cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_4.cast(BasicTypeInfo.STRING_TYPE_INFO),
        // NUMERIC TYPE -> Boolean
        '_1.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO),
        '_2.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO),
        '_3.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO),
        // NUMERIC TYPE -> NUMERIC TYPE
        '_1.cast(BasicTypeInfo.DOUBLE_TYPE_INFO),
        '_2.cast(BasicTypeInfo.INT_TYPE_INFO),
        '_3.cast(BasicTypeInfo.SHORT_TYPE_INFO),
        // Boolean -> NUMERIC TYPE
        '_4.cast(BasicTypeInfo.DOUBLE_TYPE_INFO),
        // identity casting
        '_1.cast(BasicTypeInfo.INT_TYPE_INFO),
        '_2.cast(BasicTypeInfo.DOUBLE_TYPE_INFO),
        '_3.cast(BasicTypeInfo.LONG_TYPE_INFO),
        '_4.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO))

    val expected = "1,0.0,1,true," +
      "true,false,true," +
      "1.0,0,1," +
      "1.0," +
      "1,0.0,1,true\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCastFromString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("1", "true", "2.0"))
      .toTable(tEnv)
      .select(
        // String -> BASIC TYPE (not String, Date, Void, Character)
        '_1.cast(BasicTypeInfo.BYTE_TYPE_INFO),
        '_1.cast(BasicTypeInfo.SHORT_TYPE_INFO),
        '_1.cast(BasicTypeInfo.INT_TYPE_INFO),
        '_1.cast(BasicTypeInfo.LONG_TYPE_INFO),
        '_3.cast(BasicTypeInfo.DOUBLE_TYPE_INFO),
        '_3.cast(BasicTypeInfo.FLOAT_TYPE_INFO),
        '_2.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO))

    val expected = "1,1,1,1,2.0,2.0,true\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Ignore // Date types not supported yet
  @Test
  def testCastDateFromString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env.fromElements(("2011-05-03", "15:51:36", "2011-05-03 15:51:36.000", "1446473775"))
      .toTable(tEnv)
      .select(
        '_1.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_2.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_3.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_4.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO))

    val expected = "2011-05-03 00:00:00.000,1970-01-01 15:51:36.000,2011-05-03 15:51:36.000," +
      "1970-01-17 17:47:53.775\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Ignore // Date types not supported yet
  @Test
  def testCastDateToStringAndLong(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = env.fromElements(("2011-05-03 15:51:36.000", "1304437896000"))
    val t = ds.toTable(tEnv)
      .select('_1.cast(BasicTypeInfo.DATE_TYPE_INFO).as('f0),
        '_2.cast(BasicTypeInfo.DATE_TYPE_INFO).as('f1))
      .select('f0.cast(BasicTypeInfo.STRING_TYPE_INFO),
        'f0.cast(BasicTypeInfo.LONG_TYPE_INFO),
        'f1.cast(BasicTypeInfo.STRING_TYPE_INFO),
        'f1.cast(BasicTypeInfo.LONG_TYPE_INFO))

    val expected = "2011-05-03 15:51:36.000,1304437896000," +
      "2011-05-03 15:51:36.000,1304437896000\n"
    val result = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }
}
