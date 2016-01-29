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

import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CastingITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Ignore // String autocasting not yet supported
  @Test
  def testAutoCastToString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d, new Date(0))).toTable
      .select('_1 + "b", '_2 + "s", '_3 + "i", '_4 + "L", '_5 + "f", '_6 + "d", '_7 + "Date")

    val expected = "1b,1s,1i,1L,1.0f,1.0d,1970-01-01 00:00:00.000Date"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testNumericAutoCastInArithmetic(): Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d)).toTable
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f, '_5 + 1.0d, '_6 + 1)

    val expected = "2,2,2,2.0,2.0,2.0"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testNumericAutoCastInComparison(): Unit = {

    // don't test everything, just some common cast directions

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(
      (1: Byte, 1: Short, 1, 1L, 1.0f, 1.0d),
      (2: Byte, 2: Short, 2, 2L, 2.0f, 2.0d)).as('a, 'b, 'c, 'd, 'e, 'f)
      .filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d  && 'f > 1)

    val expected = "2,2,2,2,2.0,2.0"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testCastFromString: Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements(("1", "true", "2.0",
        "2011-05-03", "15:51:36", "2011-05-03 15:51:36.000", "1446473775"))
      .toTable
      .select(
        '_1.cast(BasicTypeInfo.BYTE_TYPE_INFO),
        '_1.cast(BasicTypeInfo.SHORT_TYPE_INFO),
        '_1.cast(BasicTypeInfo.INT_TYPE_INFO),
        '_1.cast(BasicTypeInfo.LONG_TYPE_INFO),
        '_3.cast(BasicTypeInfo.DOUBLE_TYPE_INFO),
        '_3.cast(BasicTypeInfo.FLOAT_TYPE_INFO),
        '_2.cast(BasicTypeInfo.BOOLEAN_TYPE_INFO),
        '_4.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_5.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_6.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO),
        '_7.cast(BasicTypeInfo.DATE_TYPE_INFO).cast(BasicTypeInfo.STRING_TYPE_INFO))

    val expected = "1,1,1,1,2.0,2.0,true," +
      "2011-05-03 00:00:00.000,1970-01-01 15:51:36.000,2011-05-03 15:51:36.000," +
      "1970-01-17 17:47:53.775\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[NotImplementedError])
  def testCastDateToStringAndLong {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(("2011-05-03 15:51:36.000", "1304437896000"))
    val t = ds.toTable
      .select('_1.cast(BasicTypeInfo.DATE_TYPE_INFO).as('f0),
        '_2.cast(BasicTypeInfo.DATE_TYPE_INFO).as('f1))
      .select('f0.cast(BasicTypeInfo.STRING_TYPE_INFO),
        'f0.cast(BasicTypeInfo.LONG_TYPE_INFO),
        'f1.cast(BasicTypeInfo.STRING_TYPE_INFO),
        'f1.cast(BasicTypeInfo.LONG_TYPE_INFO))

    val expected = "2011-05-03 15:51:36.000,1304437896000," +
      "2011-05-03 15:51:36.000,1304437896000\n"
    val result = t.toDataSet[Row].collect
    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }
}
