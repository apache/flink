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

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.Test

class DateTimeFunctionTest extends ExpressionTestBase {

  @Test
  def testDateFormat(): Unit = {
    testAllApis(
      DateFormat('f0, "%Y"),
      "dateFormat(f0, \"%Y\")",
      "DATE_FORMAT(f0, '%Y')",
      "1990")
  }

  override def testData: Any = {
    val testData = new Row(1)
    testData.setField(0, Timestamp.valueOf("1990-10-14 12:10:10"))
    testData
  }

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo(Types.SQL_TIMESTAMP).asInstanceOf[TypeInformation[Any]]
}
