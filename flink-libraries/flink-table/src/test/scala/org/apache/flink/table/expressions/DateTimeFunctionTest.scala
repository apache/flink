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

import java.sql.Timestamp
import java.util.Date

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.flink.types.Row
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.Test

class DateTimeFunctionTest extends ExpressionTestBase {
  private val INSTANT = DateTime.parse("1990-01-02T03:04:05.678Z")
  private val LOCAL_ZONE = DateTimeZone.getDefault
  private val LOCAL_TIME = INSTANT.toDateTime(LOCAL_ZONE)

  @Test
  def testStrToDate(): Unit = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(LOCAL_ZONE)
    testSqlApi("STR_TO_DATE('12 22:29:44', '%d %H:%i:%s')", "2000-01-12 22:29:44.0")
    testSqlApi("STR_TO_DATE('22:29', '%H:%i')", "22:29:00")
    testSqlApi("STR_TO_DATE('22:29:44', '%H:%i:%s')", "22:29:44")
    testSqlApi("STR_TO_DATE('20170203', '%Y%m%d')", "2017-02-03")
    testSqlApi("STR_TO_DATE('01,5,2013', '%d,%m,%Y')", "2013-05-01")
    testSqlApi("STR_TO_DATE('20110303 am03:29:44', '%Y%m%d %p%h:%i:%s')", "2011-03-03 03:29:44.0")
    testSqlApi("STR_TO_DATE('20110303 03:29:44', '%Y%m%d %H:%i:%s')", "2011-03-03 03:29:44.0")
    testSqlApi("STR_TO_DATE('20110303 14:29:44', '%Y%m%d %H:%i:%s')", "2011-03-03 14:29:44.0")
    testSqlApi("STR_TO_DATE('20110303 22:29:44', '%Y%m%d %H:%i:%s')", "2011-03-03 22:29:44.0")
    testSqlApi(
      "STR_TO_DATE('02/03/2017', f1)",
      fmt.parseLocalDateTime("2017-02-03").toString("yyyy-MM-dd HH:mm:ss.S")
    )
  }

  @Test
  def testDateFormat(): Unit = {
    val expected = LOCAL_TIME.toString("MM/dd/yyyy HH:mm:ss.SSSSSS")
    testAllApis(
      dateFormat('f0, "%m/%d/%Y %H:%i:%s.%f"),
      "dateFormat(f0, \"%m/%d/%Y %H:%i:%s.%f\")",
      "DATE_FORMAT(f0, '%m/%d/%Y %H:%i:%s.%f')",
      expected)
  }

  @Test
  def testDateFormatNonConstantFormatter(): Unit = {
    val expected = LOCAL_TIME.toString("MM/dd/yyyy")
    testAllApis(
      dateFormat('f0, 'f1),
      "dateFormat(f0, f1)",
      "DATE_FORMAT(f0, f1)",
      expected)
  }

  override def testData: Any = {
    val testData = new Row(2)
    // SQL expect a timestamp in the local timezone
    testData.setField(0, new Timestamp(LOCAL_ZONE.convertLocalToUTC(INSTANT.getMillis, true)))
    testData.setField(1, "%m/%d/%Y")
    testData
  }

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo(Types.SQL_TIMESTAMP, Types.STRING).asInstanceOf[TypeInformation[Any]]
}
