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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.hive.functions.utils.{ExpressionTestBase, SimplePojo}
import org.apache.flink.types.Row
import org.junit.Test

class HiveScalarFunctionTest extends ExpressionTestBase {

  @Test
  def testHiveSimpleFunctions(): Unit = {
    val HiveUDFAscii = new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFAscii")
    testAllApis(
      HiveUDFAscii("0"),
      "HiveUDFAscii('0')",
      "HiveUDFAscii('0')",
      "48"
    )

    val HiveUDFConv = new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFConv")
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

    val HiveUDFDayOfMonth = new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFDayOfMonth")
    testAllApis(
      HiveUDFDayOfMonth("2009-07-30"),
      "HiveUDFDayOfMonth('2009-07-30')",
      "HiveUDFDayOfMonth('2009-07-30')",
      "30"
    )
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Any = {
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

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      TypeInformation.of(classOf[SimplePojo]),
      Types.DATE,
      Types.TIME,
      Types.TIMESTAMP,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS
    ).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "HiveUDFAscii" -> new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFAscii"),
    "HiveUDFConv" -> new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFConv"),
    "HiveUDFDayOfMonth" -> new HiveSimpleUDF("org.apache.hadoop.hive.ql.udf.UDFDayOfMonth")
  )
}
