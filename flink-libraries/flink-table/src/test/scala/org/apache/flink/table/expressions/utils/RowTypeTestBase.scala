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

import java.sql.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

class RowTypeTestBase extends ExpressionTestBase {

  override def testData: Any = {
    val row = new Row(3)
    row.setField(0, 2)
    row.setField(1, "foo")
    row.setField(2, true)
    val nestedRow = new Row(2)
    nestedRow.setField(0, 3)
    nestedRow.setField(1, row)
    val specialTypeRow = new Row(3)
    specialTypeRow.setField(0, Date.valueOf("1984-03-12"))
    specialTypeRow.setField(1, BigDecimal("0.00000000").bigDecimal)
    specialTypeRow.setField(2, Array(1, 2, 3))
    val testData = new Row(7)
    testData.setField(0, null)
    testData.setField(1, 1)
    testData.setField(2, row)
    testData.setField(3, nestedRow)
    testData.setField(4, specialTypeRow)
    testData.setField(5, Row.of("foo", null))
    testData.setField(6, Row.of(null, null))
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.STRING,
      Types.INT,
      Types.ROW(Types.INT, Types.STRING, Types.BOOLEAN),
      Types.ROW(Types.INT, Types.ROW(Types.INT, Types.STRING, Types.BOOLEAN)),
      Types.ROW(Types.SQL_DATE, Types.DECIMAL, ObjectArrayTypeInfo.getInfoFor(Types.INT)),
      Types.ROW(Types.STRING, Types.BOOLEAN),
      Types.ROW(Types.STRING, Types.STRING)
    ).asInstanceOf[TypeInformation[Any]]
  }
}
