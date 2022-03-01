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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.DecimalDataUtils
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row

abstract class ScalarOperatorsTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val testData = new Row(23)
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
    testData.setField(16,
        DecimalDataUtils.castFrom("0.00000000", 19, 8).toBigDecimal)
    testData.setField(17,
        DecimalDataUtils.castFrom("10.0", 19, 1).toBigDecimal)
    testData.setField(18, "hello world".getBytes())
    testData.setField(19, "hello flink".getBytes())
    testData.setField(20, "who".getBytes())
    testData.setField(21, localTime("12:34:56"))
    testData.setField(22, localDateTime("1996-11-10 12:34:56"))
    testData
  }

  override def testDataType: AbstractDataType[_] = {
    DataTypes.ROW(
        DataTypes.FIELD("f0", DataTypes.TINYINT()),
        DataTypes.FIELD("f1", DataTypes.SMALLINT()),
        DataTypes.FIELD("f2", DataTypes.INT()),
        DataTypes.FIELD("f3", DataTypes.BIGINT()),
        DataTypes.FIELD("f4", DataTypes.FLOAT()),
        DataTypes.FIELD("f5", DataTypes.DOUBLE()),
        DataTypes.FIELD("f6", DataTypes.BOOLEAN()),
        DataTypes.FIELD("f7", DataTypes.DOUBLE()),
        DataTypes.FIELD("f8", DataTypes.INT()),
        DataTypes.FIELD("f9", DataTypes.INT()),
        DataTypes.FIELD("f10", DataTypes.STRING()),
        DataTypes.FIELD("f11", DataTypes.BOOLEAN()),
        DataTypes.FIELD("f12", DataTypes.BOOLEAN()),
        DataTypes.FIELD("f13", DataTypes.ROW(
            DataTypes.FIELD("f0", DataTypes.STRING()),
            DataTypes.FIELD("f1", DataTypes.STRING()))
        ),
        DataTypes.FIELD("f14", DataTypes.STRING()),
        DataTypes.FIELD("f15", DataTypes.DATE()),
        DataTypes.FIELD("f16", DataTypes.DECIMAL(19, 8)),
        DataTypes.FIELD("f17", DataTypes.DECIMAL(19, 1)),
        DataTypes.FIELD("f18", DataTypes.BINARY(200)),
        DataTypes.FIELD("f19", DataTypes.VARBINARY(200)),
        DataTypes.FIELD("f20", DataTypes.VARBINARY(200)),
        DataTypes.FIELD("f21", DataTypes.TIME()),
        DataTypes.FIELD("f22", DataTypes.TIMESTAMP())
    )
  }

  override def containsLegacyTypes: Boolean = false

  override def functions: Map[String, ScalarFunction] = Map(
    "shouldNotExecuteFunc" -> ShouldNotExecuteFunc
  )
}
