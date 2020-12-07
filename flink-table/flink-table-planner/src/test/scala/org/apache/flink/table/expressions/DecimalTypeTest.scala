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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

class DecimalTypeTest extends ExpressionTestBase {

  @Test
  def testDecimalLiterals(): Unit = {
    // implicit double
    testAllApis(
      11.2,
      "11.2",
      "11.2",
      "11.2")

    // implicit double
    testAllApis(
      0.7623533651719233,
      "0.7623533651719233",
      "0.7623533651719233",
      "0.7623533651719233")

    // explicit decimal (with precision of 19)
    testAllApis(
      BigDecimal("1234567891234567891"),
      "1234567891234567891p",
      "1234567891234567891",
      "1234567891234567891")

    // explicit decimal (high precision, not SQL compliant)
    testTableApi(
      BigDecimal("123456789123456789123456789"),
      "123456789123456789123456789p",
      "123456789123456789123456789")

    // explicit decimal (high precision, not SQL compliant)
    testTableApi(
      BigDecimal("12.3456789123456789123456789"),
      "12.3456789123456789123456789p",
      "12.3456789123456789123456789")
  }

  @Test
  def testDecimalBorders(): Unit = {
    testAllApis(
      Double.MaxValue,
      Double.MaxValue.toString,
      Double.MaxValue.toString,
      Double.MaxValue.toString)

    testAllApis(
      Double.MinValue,
      Double.MinValue.toString,
      Double.MinValue.toString,
      Double.MinValue.toString)

    testAllApis(
      Double.MinValue.cast(Types.FLOAT),
      s"${Double.MinValue}.cast(FLOAT)",
      s"CAST(${Double.MinValue} AS FLOAT)",
      Float.NegativeInfinity.toString)

    testAllApis(
      Byte.MinValue.cast(Types.BYTE),
      s"(${Byte.MinValue}).cast(BYTE)",
      s"CAST(${Byte.MinValue} AS TINYINT)",
      Byte.MinValue.toString)

    testAllApis(
      Byte.MinValue.cast(Types.BYTE) - 1.cast(Types.BYTE),
      s"(${Byte.MinValue}).cast(BYTE) - (1).cast(BYTE)",
      s"CAST(${Byte.MinValue} AS TINYINT) - CAST(1 AS TINYINT)",
      Byte.MaxValue.toString)

    testAllApis(
      Short.MinValue.cast(Types.SHORT),
      s"(${Short.MinValue}).cast(SHORT)",
      s"CAST(${Short.MinValue} AS SMALLINT)",
      Short.MinValue.toString)

    testAllApis(
      Int.MinValue.cast(Types.INT) - 1,
      s"(${Int.MinValue}).cast(INT) - 1",
      s"CAST(${Int.MinValue} AS INT) - 1",
      Int.MaxValue.toString)

    testAllApis(
      Long.MinValue.cast(Types.LONG),
      s"(${Long.MinValue}L).cast(LONG)",
      s"CAST(${Long.MinValue} AS BIGINT)",
      Long.MinValue.toString)
  }

  @Test
  def testDecimalCasting(): Unit = {
    // from String
    testTableApi(
      "123456789123456789123456789".cast(Types.DECIMAL),
      "'123456789123456789123456789'.cast(DECIMAL)",
      "123456789123456789123456789")

    // from double
    testAllApis(
      'f3.cast(Types.DECIMAL),
      "f3.cast(DECIMAL)",
      "CAST(f3 AS DECIMAL)",
      "4.2")

    // to double
    testAllApis(
      'f0.cast(Types.DOUBLE),
      "f0.cast(DOUBLE)",
      "CAST(f0 AS DOUBLE)",
      "1.2345678912345679E8")

    // to int
    testAllApis(
      'f4.cast(Types.INT),
      "f4.cast(INT)",
      "CAST(f4 AS INT)",
      "123456789")

    // to long
    testAllApis(
      'f4.cast(Types.LONG),
      "f4.cast(LONG)",
      "CAST(f4 AS BIGINT)",
      "123456789")

    // to boolean (not SQL compliant)
    testTableApi(
      'f1.cast(Types.BOOLEAN),
      "f1.cast(BOOLEAN)",
      "true")

    testTableApi(
      'f5.cast(Types.BOOLEAN),
      "f5.cast(BOOLEAN)",
      "false")

    testTableApi(
      BigDecimal("123456789.123456789123456789").cast(Types.DOUBLE),
      "(123456789.123456789123456789p).cast(DOUBLE)",
      "1.2345678912345679E8")
  }

  @Test
  def testDecimalArithmetic(): Unit = {
    // implicit cast to decimal
    testAllApis(
      'f1 + 12,
      "f1 + 12",
      "f1 + 12",
      "123456789123456789123456801")

    // implicit cast to decimal
    testAllApis(
      12.toExpr + 'f1,
      "12 + f1",
      "12 + f1",
      "123456789123456789123456801")

    // implicit cast to decimal
    testAllApis(
      'f1 + 12.3,
      "f1 + 12.3",
      "f1 + 12.3",
      "123456789123456789123456801.3")

    // implicit cast to decimal
    testAllApis(
      12.3.toExpr + 'f1,
      "12.3 + f1",
      "12.3 + f1",
      "123456789123456789123456801.3")

    testAllApis(
      'f1 + 'f1,
      "f1 + f1",
      "f1 + f1",
      "246913578246913578246913578")

    testAllApis(
      'f1 - 'f1,
      "f1 - f1",
      "f1 - f1",
      "0")

    testAllApis(
      'f1 * 'f1,
      "f1 * f1",
      "f1 * f1",
      "15241578780673678546105778281054720515622620750190521")

    testAllApis(
      'f1 / 'f1,
      "f1 / f1",
      "f1 / f1",
      "1")

    testAllApis(
      'f1 % 'f1,
      "f1 % f1",
      "MOD(f1, f1)",
      "0")

    testAllApis(
      -'f0,
      "-f0",
      "-f0",
      "-123456789.123456789123456789")

    testAllApis(
      BigDecimal("1").toExpr / BigDecimal("3"),
      "1p / 3p",
      "CAST('1' AS DECIMAL) / CAST('3' AS DECIMAL)",
      "0.3333333333333333333333333333333333"
    )
  }

  @Test
  def testDecimalComparison(): Unit = {
    testAllApis(
      'f1 < 12,
      "f1 < 12",
      "f1 < 12",
      "false")

    testAllApis(
      'f1 > 12,
      "f1 > 12",
      "f1 > 12",
      "true")

    testAllApis(
      'f1 === 12,
      "f1 === 12",
      "f1 = 12",
      "false")

    testAllApis(
      'f5 === 0,
      "f5 === 0",
      "f5 = 0",
      "true")

    testAllApis(
      'f1 === BigDecimal("123456789123456789123456789"),
      "f1 === 123456789123456789123456789p",
      "f1 = CAST('123456789123456789123456789' AS DECIMAL)",
      "true")

    testAllApis(
      'f1 !== BigDecimal("123456789123456789123456789"),
      "f1 !== 123456789123456789123456789p",
      "f1 <> CAST('123456789123456789123456789' AS DECIMAL)",
      "false")

    testAllApis(
      'f4 < 'f0,
      "f4 < f0",
      "f4 < f0",
      "true")

    // TODO add all tests if FLINK-4070 is fixed
    testSqlApi(
      "12 < f1",
      "true")
  }

  // ----------------------------------------------------------------------------------------------

  def testData: Row = {
    val testData = new Row(6)
    testData.setField(0, BigDecimal("123456789.123456789123456789").bigDecimal)
    testData.setField(1, BigDecimal("123456789123456789123456789").bigDecimal)
    testData.setField(2, 42)
    testData.setField(3, 4.2)
    testData.setField(4, BigDecimal("123456789").bigDecimal)
    testData.setField(5, BigDecimal("0.000").bigDecimal)
    testData
  }

  def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.DECIMAL,
      Types.DECIMAL,
      Types.INT,
      Types.DOUBLE,
      Types.DECIMAL,
      Types.DECIMAL).asInstanceOf[TypeInformation[Any]]
  }

}
