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

package org.apache.flink.table.planner.expressions.utils

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.data.DecimalDataUtils
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo
import org.apache.flink.types.Row

abstract class ScalarOperatorsTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(18)
    testData.setField(0, 1: Byte)
    testData.setField(1, 1: Short)
    testData.setField(2, 1)
    testData.setField(3, 1L)
    testData.setField(4, 1.0f)
    testData.setField(5, 1.0d)
    testData.setField(6, true)
    testData.setField(7, 0.0d)
    testData.setField(8, 5)
    testData.setField(9, 10)
    testData.setField(10, "String")
    testData.setField(11, false)
    testData.setField(12, null)
    testData.setField(13, Row.of("foo", null))
    testData.setField(14, null)
    testData.setField(15, localDate("1996-11-10"))
    testData.setField(16, DecimalDataUtils.castFrom("0.00000000", 19, 8))
    testData.setField(17, DecimalDataUtils.castFrom("10.0", 19, 1))
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  Types.BYTE,
      /* 1 */  Types.SHORT,
      /* 2 */  Types.INT,
      /* 3 */  Types.LONG,
      /* 4 */  Types.FLOAT,
      /* 5 */  Types.DOUBLE,
      /* 6 */  Types.BOOLEAN,
      /* 7 */  Types.DOUBLE,
      /* 8 */  Types.INT,
      /* 9 */  Types.INT,
      /* 10 */ Types.STRING,
      /* 11 */ Types.BOOLEAN,
      /* 12 */ Types.BOOLEAN,
      /* 13 */ Types.ROW(Types.STRING, Types.STRING),
      /* 14 */ Types.STRING,
      /* 15 */ Types.LOCAL_DATE,
      /* 16 */ DecimalDataTypeInfo.of(19, 8),
      /* 17 */ DecimalDataTypeInfo.of(19, 1)
    )
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "shouldNotExecuteFunc" -> ShouldNotExecuteFunc
  )
}
