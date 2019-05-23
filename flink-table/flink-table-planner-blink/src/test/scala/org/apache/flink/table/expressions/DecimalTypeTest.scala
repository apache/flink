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

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.table.typeutils.DecimalTypeInfo
import org.apache.flink.types.Row
import org.junit.Test

class DecimalTypeTest extends ExpressionTestBase {

  @Test
  def testDecimalLiterals(): Unit = {
    // implicit double
    testSqlApi(
      "11.2",
      "11.2")

    // implicit double
    testSqlApi(
      "0.7623533651719233",
      "0.7623533651719233")

    // explicit decimal (with precision of 19)
    testSqlApi(
      "1234567891234567891",
      "1234567891234567891")
  }

  @Test
  def testDecimalBorders(): Unit = {
    testSqlApi(
      Double.MaxValue.toString,
      Double.MaxValue.toString)

    testSqlApi(
      Double.MinValue.toString,
      Double.MinValue.toString)

    testSqlApi(
      s"CAST(${Double.MinValue} AS FLOAT)",
      Float.NegativeInfinity.toString)

    testSqlApi(
      s"CAST(${Byte.MinValue} AS TINYINT)",
      Byte.MinValue.toString)

    testSqlApi(
      s"CAST(${Byte.MinValue} AS TINYINT) - CAST(1 AS TINYINT)",
      Byte.MaxValue.toString)

    testSqlApi(
      s"CAST(${Short.MinValue} AS SMALLINT)",
      Short.MinValue.toString)

    testSqlApi(
      s"CAST(${Int.MinValue} AS INT) - 1",
      Int.MaxValue.toString)

    testSqlApi(
      s"CAST(${Long.MinValue} AS BIGINT)",
      Long.MinValue.toString)
  }

  @Test
  def testDecimalCasting(): Unit = {
    // from String
    testSqlApi(
      "CAST('123456789123456789123456789' AS DECIMAL(27, 0))",
      "123456789123456789123456789")

    // from double
    testSqlApi(
      "CAST(f3 AS DECIMAL)",
      "4")

    testSqlApi(
      "CAST(f3 AS DECIMAL(10,2))",
      "4.20"
    )

    // to double
    testSqlApi(
      "CAST(f0 AS DOUBLE)",
      "1.2345678912345679E8")

    // to int
    testSqlApi(
      "CAST(f4 AS INT)",
      "123456789")

    // to long
    testSqlApi(
      "CAST(f4 AS BIGINT)",
      "123456789")
  }

  @Test
  def testDecimalArithmetic(): Unit = {

    // note: calcite type inference:
    // Decimal+ExactNumeric => Decimal
    // Decimal+Double => Double.

    // implicit cast to decimal
    testSqlApi(
      "f1 + 12",
      "123456789123456789123456801")

    // implicit cast to decimal
    testSqlApi(
      "12 + f1",
      "123456789123456789123456801")

    testSqlApi(
      "f1 + 12.3",
      "123456789123456789123456801.3"
    )

    testSqlApi(
      "12.3 + f1",
      "123456789123456789123456801.3")

    testSqlApi(
      "f1 + f1",
      "246913578246913578246913578")

    testSqlApi(
      "f1 - f1",
      "0")

    testSqlApi(
      "f1 / f1",
      "1.00000000")

    testSqlApi(
      "MOD(f1, f1)",
      "0")

    testSqlApi(
      "-f0",
      "-123456789.123456789123456789")
  }

  @Test
  def testDecimalComparison(): Unit = {
    testSqlApi(
      "f1 < 12",
      "false")

    testSqlApi(
      "f1 > 12",
      "true")

    testSqlApi(
      "f1 = 12",
      "false")

    testSqlApi(
      "f5 = 0",
      "true")

    testSqlApi(
      "f1 = CAST('123456789123456789123456789' AS DECIMAL(30, 0))",
      "true")

    testSqlApi(
      "f1 <> CAST('123456789123456789123456789' AS DECIMAL(30, 0))",
      "false")

    testSqlApi(
      "f4 < f0",
      "true")

    // TODO add all tests if FLINK-4070 is fixed
    testSqlApi(
      "12 < f1",
      "true")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Row = {
    val testData = new Row(6)
    testData.setField(0, Decimal.castFrom("123456789.123456789123456789", 30, 18))
    testData.setField(1, Decimal.castFrom("123456789123456789123456789", 30, 0))
    testData.setField(2, 42)
    testData.setField(3, 4.2)
    testData.setField(4, Decimal.castFrom("123456789", 10, 0))
    testData.setField(5, Decimal.castFrom("0.000", 10, 3))
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */ DecimalTypeInfo.of(30, 18),
      /* 1 */ DecimalTypeInfo.of(30, 0),
      /* 2 */ Types.INT,
      /* 3 */ Types.DOUBLE,
      /* 4 */ DecimalTypeInfo.of(10, 0),
      /* 5 */ DecimalTypeInfo.of(10, 3))
  }
}
