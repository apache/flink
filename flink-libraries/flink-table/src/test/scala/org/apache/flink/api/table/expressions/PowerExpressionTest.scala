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

package org.apache.flink.api.table.expressions

import java.lang.{Double => JDouble, Float => JFloat, Long => JLong, Integer => JInteger}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.utils.ExpressionTestBase
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, Types}
import org.junit.Test

class PowerExpressionTest extends ExpressionTestBase {

  override def testData = {
    val testData = new Row(5)
    testData.setField(0, 100.0f: JFloat)
    testData.setField(1, 100.0: JDouble)
    testData.setField(2, 100L: JLong)
    testData.setField(3, 100: JInteger)
    testData.setField(4, new JBigDecimal(100): JBigDecimal)
    testData
  }

  override def typeInfo = {
    new RowTypeInfo(Seq(
      Types.FLOAT,
      Types.DOUBLE,
      Types.LONG,
      Types.INT,
      Types.DECIMAL)
    ).asInstanceOf[TypeInformation[Any]]
  }

  @Test
  def testSqrtFloat(): Unit = {
    testSqlApi(
      "SQRT(f0)",
      "10.0")
  }

  @Test
  def testSqrtDouble(): Unit = {
    testSqlApi(
      "SQRT(f1)",
      "10.0")
  }

  @Test
  def testSqrtLong(): Unit = {
    testSqlApi(
      "SQRT(f2)",
      "10.0")
  }

  @Test
  def testSqrtInteger(): Unit = {
    testSqlApi(
      "SQRT(f3)",
      "10.0")
  }

  @Test
  def testSqrtBigDecimal(): Unit = {
    testSqlApi(
      "SQRT(f4)",
      "10.0")
  }

  @Test
  def testPowerFloatAndFloat(): Unit = {
    testSqlApi(
      "POWER(f0, f0)",
      "1.0E200")
  }

  @Test
  def testPowerFloatAndDouble(): Unit = {
    testSqlApi(
      "POWER(f0, f1)",
      "1.0E200")
  }

  @Test
  def testPowerFloatAndLng(): Unit = {
    testSqlApi(
      "POWER(f0, f2)",
      "1.0E200")
  }

  @Test
  def testPowerFloatAndInteger(): Unit = {
    testSqlApi(
      "POWER(f0, f3)",
      "1.0E200")
  }

  @Test
  def testPowerBigDecimalAndFloat(): Unit = {
    testSqlApi(
      "POWER(f4, f0)",
      "1.0E200")
  }

  @Test
  def testPowerBigDecimalAndDouble(): Unit = {
    testSqlApi(
      "POWER(f4, f1)",
      "1.0E200")
  }

  @Test
  def testPowerBigDecimalAndLong(): Unit = {
    testSqlApi(
      "POWER(f4, f2)",
      "1.0E200")
  }

  @Test
  def testPowerBigDecimalAndIntger(): Unit = {
    testSqlApi(
      "POWER(f4, f3)",
      "1.0E200")
  }

  @Test
  def testPowerLongAndLong(): Unit = {
    testSqlApi(
      "POWER(f2, f2)",
      "1.0E200")
  }
}
