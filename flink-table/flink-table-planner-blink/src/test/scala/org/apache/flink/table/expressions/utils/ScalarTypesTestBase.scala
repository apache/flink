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

package org.apache.flink.table.expressions.utils

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.typeutils.{DecimalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row

abstract class ScalarTypesTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(46)
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
    testData.setField(15, Decimal.castFrom("-1231.1231231321321321111", 38, 19))
    testData.setField(16, UTCDate("1996-11-10"))
    testData.setField(17, UTCTime("06:55:44"))
    testData.setField(18, UTCTimestamp("1996-11-10 06:55:44.333"))
    testData.setField(19, 1467012213000L) // +16979 07:23:33.000
    testData.setField(20, 25) // +2-01
    testData.setField(21, null)
    testData.setField(22, Decimal.castFrom("2", 38, 19))
    testData.setField(23, "%This is a test String.")
    testData.setField(24, "*_This is a test String.")
    testData.setField(25, 0.42.toByte)
    testData.setField(26, 0.toShort)
    testData.setField(27, 0.toLong)
    testData.setField(28, 0.45.toFloat)
    testData.setField(29, 0.46)
    testData.setField(30, 1)
    testData.setField(31, Decimal.castFrom("-0.1231231321321321111", 38, 0))
    testData.setField(32, -1)
    testData.setField(33, null)
    testData.setField(34, Decimal.castFrom("1514356320000", 38, 19))
    testData.setField(35, "a")
    testData.setField(36, "b")
    testData.setField(37, Array[Byte](1, 2, 3, 4))
    testData.setField(38, "AQIDBA==")
    testData.setField(39, "1世3")
    testData.setField(40, null)
    testData.setField(41, null)
    testData.setField(42, 256.toLong)
    testData.setField(43, -1.toLong)
    testData.setField(44, 256)
    testData.setField(45, UTCTimestamp("1996-11-10 06:55:44.333").toString)
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
      DecimalTypeInfo.of(38, 19),
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      TimeIntervalTypeInfo.INTERVAL_MILLIS,
      TimeIntervalTypeInfo.INTERVAL_MONTHS,
      Types.BOOLEAN,
      DecimalTypeInfo.of(38, 19),
      Types.STRING,
      Types.STRING,
      Types.BYTE,
      Types.SHORT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.INT,
      DecimalTypeInfo.of(38, 19),
      Types.INT,
      Types.STRING,
      DecimalTypeInfo.of(19, 0),
      Types.STRING,
      Types.STRING,
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
      Types.STRING,
      Types.STRING,
      Types.STRING,
      DecimalTypeInfo.of(38, 19),
      Types.LONG,
      Types.LONG,
      Types.INT,
      Types.STRING)
  }
}
