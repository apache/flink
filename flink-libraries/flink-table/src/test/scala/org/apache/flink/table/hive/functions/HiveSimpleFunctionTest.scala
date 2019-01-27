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

package org.apache.flink.table.hive.functions

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{ExpressionTestBase, SimplePojo}
import org.apache.flink.types.Row
import org.junit.Test

/**
 * Test case for the Hive's Simple UDF.
 */
class HiveSimpleFunctionTest extends ExpressionTestBase {

  @Test
  def testUDFToBoolean(): Unit = {
    val HiveUDFToBoolean = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFToBoolean"))
    testAllApis(
      HiveUDFToBoolean("true"),
      "HiveUDFToBoolean('true')",
      "HiveUDFToBoolean('true')",
      "true"
    )
  }

  @Test
  def testUDFPi(): Unit = {
    val HiveUDFPI = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFPI"))
    testAllApis(
      HiveUDFPI(),
      "HiveUDFPI()",
      "HiveUDFPI()",
      "3.141592653589793"
    )
  }

  @Test
  def testUDFJson(): Unit = {
    val HiveUDFJson = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFJson"))
    testAllApis(
      HiveUDFJson("{\"store\": \"zhuoluo\", \"owner\": \"amy\"}", "$.owner"),
      "HiveUDFJson('{\"store\": \"zhuoluo\", \"owner\": \"amy\"}', '$.owner')",
      "HiveUDFJson('{\"store\": \"zhuoluo\", \"owner\": \"amy\"}', '$.owner')",
      "amy"
    )
  }

  @Test
  def testUnbase64(): Unit = {
    val HiveUDFUnbase64 = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFUnbase64"))
    testAllApis(
      HiveUDFUnbase64("y6Ss+zCYObpCbgfWfyNWTw=="),
      "HiveUDFUnbase64('y6Ss+zCYObpCbgfWfyNWTw==')",
      "HiveUDFUnbase64('y6Ss+zCYObpCbgfWfyNWTw==')",
      "cb a4 ac fb 30 98 39 ba 42 6e 07 d6 7f 23 56 4f"
    )
  }

  @Test
  def testUDFHex(): Unit = {
    val HiveUDFHex = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFHex"))
    testAllApis(
      HiveUDFHex(17),
      "HiveUDFHex(17)",
      "HiveUDFHex(17)",
      "11"
    )
    testAllApis(
      HiveUDFHex("Facebook"),
      "HiveUDFHex('Facebook')",
      "HiveUDFHex('Facebook')",
      "46616365626F6F6B"
    )
  }

  @Test
  def testUDFMinute(): Unit = {
    val HiveUDFMinute = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFMinute"))
    testAllApis(
      HiveUDFMinute("2009-07-30 12:58:59"),
      "HiveUDFMinute('2009-07-30 12:58:59')",
      "HiveUDFMinute('2009-07-30 12:58:59')",
      "58"
    )
  }

  @Test
  def testUDFAcos(): Unit = {
    val HiveUDFAcos = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAcos"))
    testAllApis(
      HiveUDFAcos(1.0),
      "HiveUDFAcos(1.0)",
      "HiveUDFAcos(1.0E0)",
      "0.0"
    )
  }

  @Test
  def testUDFAscii(): Unit = {
    val HiveUDFAscii = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAscii"))
    testAllApis(
      HiveUDFAscii("0"),
      "HiveUDFAscii('0')",
      "HiveUDFAscii('0')",
      "48"
    )
  }

  @Test
  def testUDFAsin(): Unit = {
    val HiveUDFAsin = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAsin"))
    testAllApis(
      HiveUDFAsin("0"),
      "HiveUDFAsin('0')",
     "HiveUDFAsin('0')",
      "0.0"
    )
  }

  @Test
  def testUDFBin(): Unit = {
    val HiveUDFBin = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFBin"))
    testAllApis(
      HiveUDFBin(13),
      "HiveUDFBin(13)",
      "HiveUDFBin(13)",
      "1101"
    )
  }

  @Test
  def testUDFConv(): Unit = {
    val HiveUDFConv = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFConv"))
    testAllApis(
      HiveUDFConv("100", 2, 10),
      "HiveUDFConv('100', 2, 10)",
      "HiveUDFConv('100', 2, 10)",
      "4"
    )
    testAllApis(
      HiveUDFConv(-10, 16, -10),
      "HiveUDFConv(-10, 16, -10)",
      "HiveUDFConv(-10, 16, -10)",
      "-16"
    )
  }

  @Test
  def testUDFCos(): Unit = {
    val HiveUDFCos = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFCos"))
    testAllApis(
      HiveUDFCos(0.0),
      "HiveUDFCos(0.0)",
      "HiveUDFCos(0.0E0)",
      "1.0"
    )
  }

  @Test
  def testUDFDayOfMonth(): Unit = {
    val HiveUDFDayOfMonth = new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFDayOfMonth"))
    testAllApis(
      HiveUDFDayOfMonth("2009-07-30"),
      "HiveUDFDayOfMonth('2009-07-30')",
      "HiveUDFDayOfMonth('2009-07-30')",
      "30"
    )
  }

  override def rowTestData: Row = {
    val testData = new Row(9)
    testData.setField(0, 42)
    testData.setField(1, "Test")
    testData.setField(2, null)
    testData.setField(3, SimplePojo("Bob", 36))
    testData.setField(4, Date.valueOf("1990-10-14"))
    testData.setField(5, Time.valueOf("12:10:10"))
    testData.setField(6, Timestamp.valueOf("1990-10-14 12:10:10"))
    testData.setField(7, 12)
    testData.setField(8, 1000L)
    testData
  }

  override def rowType: RowTypeInfo = {
    new RowTypeInfo(
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      TypeInformation.of(classOf[SimplePojo]),
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS
    )
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "HiveUDFChr" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFChr")),
    "HiveUDFToBoolean" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFToBoolean")),
    "HiveUDFPI" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFPI")),
    "HiveUDFSha1" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFSha1")),
    "HiveUDFJson" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFJson")),
    "HiveUDFUnbase64" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFUnbase64")),
    "HiveUDFHex" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFHex")),
    "HiveUDFMinute" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFMinute")),
    "HiveUDFAcos" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAcos")),
    "HiveUDFAscii" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAscii")),
    "HiveUDFAsin" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFAsin")),
    "HiveUDFBin" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFBin")),
    "HiveUDFConv" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFConv")),
    "HiveUDFCos" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFCos")),
    "HiveUDFDayOfMonth" -> new HiveSimpleUDF(
      new HiveFunctionWrapper("org.apache.hadoop.hive.ql.udf.UDFDayOfMonth"))
  )
}
