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

import java.math.{BigDecimal => JBigDecimal}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.table.typeutils.BigDecimalTypeInfo
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row

import org.junit.{Ignore, Rule, Test}
import org.junit.rules.ExpectedException

import scala.annotation.meta.getter

class BuiltinScalarFunctionTest extends ExpressionTestBase {

  @(Rule @getter)
  override val thrown: ExpectedException = ExpectedException.none()

  @Test
  def testInvalidStringToMap(): Unit = {
    // test non-exist key access
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Invalid number of arguments to function 'STR_TO_MAP'")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2:v2', ';')",
      "EXCEPTION"
    )
  }

  @Test
  def testStringToMap(): Unit = {
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')",
      "{k1=v1, k2=v2}")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2: v2', ';', ':')",
      "{k1=v1, k2= v2}")

    // test empty
    testSqlApi(
      "STR_TO_MAP('')",
      "{}")

    // test key access
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')['k1']",
      "v1")
    testSqlApi(
      "STR_TO_MAP('k1:v1;k2:v2', ';', ':')['k2']",
      "v2")

    // test non-exist key access
    testSqlApi(
      "STR_TO_MAP('k1=v1,k2=v2')['k3']",
      "null")

    testSqlApi(
      "STR_TO_MAP(f19)",
      "{test1=1, test2=2, test3=3}")

    testSqlApi(
      "STR_TO_MAP(f20)",
      "null")

    testSqlNullable("STR_TO_MAP(f20)")
  }

  @Ignore // TODO
  @Test
  def testInvalidIf(): Unit = {
    // test IF(BOOL, STRING, BOOLEAN), will do implicit type coercion.
    testSqlApi(
      "IF(f7 > 5, f0, f1)",
      "true")

    // test IF(BOOL, INT, BIGINT), will do implicit type coercion.
    testSqlApi(
      "IF(f7 > 5, f14, f4)",
      "44")
    // test input with null
    testSqlApi(
      "IF(f7 < 5, cast(null as int), f4)",
      "null")
  }

  @Test
  def testIf(): Unit = {
    // f0 is a STRING, cast(f0 as double) should never be ran
    testSqlApi(
      "IF(1 = 1, f6, cast(f0 as double))",
      "4.6")

    // test STRING, STRING
    testSqlApi(
      "IF(f7 > 5, f0, f8)",
      " This is a test String. ")

    // test BYTE, BYTE
    testSqlApi(
      "IF(f7 < 5, f2, f9)",
      "42")

    // test INT, INT
    testSqlApi(
      "IF(f7 < 5, f14, f7)",
      "-3")

    // test SHORT, SHORT
    testSqlApi(
      "IF(f7 < 5, f3, f10)",
      "43")

    // test Long, Long
    testSqlApi(
      "IF(f7 < 5, f4, f11)",
      "44")

    // test Double, Double
    testSqlApi(
      "IF(f7 < 5, f6, f13)",
      "4.6")

    // test BOOL, BOOL
    testSqlApi(
      "IF(f7 < 5, f1, f21)",
      "true")

    // test DECIMAL, DECIMAL
    testSqlApi(
      "IF(f7 < 5, f15, f22)",
      "-1231.1231231321321321111")

    // test BINARY, BINARY
    // the answer BINARY will cast to STRING in ExpressionTestBase.scala
    testSqlApi(
      "IF(f7 < 5, f26, f27)",
      "hello world") // hello world

    // test null, int
    // TODO
//    testSqlApi(
//      "IF(f7 < 5, null, f4)",
//      "null")

  }

  @Test
  def testIfWithTime(): Unit = {
    // test DATE, DATE
    testSqlApi(
      "IF(f7 < 5, f16, f23)",
      "1996-11-10")

    // test TIME, TIME
    testSqlApi(
      "IF(f7 < 5, f17, f24)",
      "06:55:44")

    // test TIMESTAMP, TIMESTAMP
    testSqlApi(
      "IF(f7 < 5, f18, f25)",
      "1996-11-10 06:55:44.333")
  }

  @Test
  def testToTimestampWithNumeric(): Unit = {
    // Test integral and fractional numeric to timestamp.
    testSqlApi(
      "to_timestamp(f2)",
      "1970-01-01 00:00:00.042")
    testSqlApi(
      "to_timestamp(f3)",
      "1970-01-01 00:00:00.043")
    testSqlApi(
      "to_timestamp(f4)",
      "1970-01-01 00:00:00.044")
    testSqlApi(
      "to_timestamp(f5)",
      "1970-01-01 00:00:00.004")
    testSqlApi(
      "to_timestamp(f6)",
      "1970-01-01 00:00:00.004")
    testSqlApi(
      "to_timestamp(f7)",
      "1970-01-01 00:00:00.003")
    // Test decimal to timestamp.
    testSqlApi(
      "to_timestamp(f15)",
      "1969-12-31 23:59:58.769")
    // test with null input
    testSqlApi(
      "to_timestamp(cast(null as varchar))",
      "null")
  }

  @Test
  def testFromUnixTimeWithNumeric(): Unit = {
    // Test integral and fractional numeric from_unixtime.
    testSqlApi(
      "from_unixtime(f2)",
      "1970-01-01 00:00:42")
    testSqlApi(
      "from_unixtime(f3)",
      "1970-01-01 00:00:43")
    testSqlApi(
      "from_unixtime(f4)",
      "1970-01-01 00:00:44")
    testSqlApi(
      "from_unixtime(f5)",
      "1970-01-01 00:00:04")
    testSqlApi(
      "from_unixtime(f6)",
      "1970-01-01 00:00:04")
    testSqlApi(
      "from_unixtime(f7)",
      "1970-01-01 00:00:03")
    // Test decimal to from_unixtime.
    testSqlApi(
      "from_unixtime(f15)",
      "1969-12-31 23:39:29")
    // test with null input
    testSqlApi(
      "from_unixtime(cast(null as int))",
      "null")
  }

  override def testData: Row = {
    val testData = new Row(28)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")
    testData.setField(9, -42.toByte)
    testData.setField(10, -43.toShort)
    testData.setField(11, -44.toLong)
    testData.setField(12, -4.5.toFloat)
    testData.setField(13, -4.6)
    testData.setField(14, -3)
    testData.setField(15, new JBigDecimal("-1231.1231231321321321111"))
    testData.setField(16, UTCDate("1996-11-10"))
    testData.setField(17, UTCTime("06:55:44"))
    testData.setField(18, UTCTimestamp("1996-11-10 06:55:44.333"))
    testData.setField(19, "test1=1,test2=2,test3=3")
    testData.setField(20, null)
    testData.setField(21, false)
    testData.setField(22, new JBigDecimal("1345.1231231321321321111"))
    testData.setField(23, UTCDate("1997-11-11"))
    testData.setField(24, UTCTime("09:44:55"))
    testData.setField(25, UTCTimestamp("1997-11-11 09:44:55.333"))
    testData.setField(26, "hello world".getBytes)
    testData.setField(27, "This is a testing string.".getBytes)
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      Types.STRING,
      Types.BOOLEAN,
      Types.BYTE,
      Types.SHORT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.INT,
      Types.STRING,
      Types.BYTE,
      Types.SHORT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.INT,
      BigDecimalTypeInfo.of(38, 19),
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.STRING,
      Types.STRING,
      Types.BOOLEAN,
      BigDecimalTypeInfo.of(38, 19),
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.PRIMITIVE_ARRAY(Types.BYTE),
      Types.PRIMITIVE_ARRAY(Types.BYTE))
  }
}
