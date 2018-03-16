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
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

class ScalarOperatorsTestBase extends ExpressionTestBase {

  def testData: Row = {
    val testData = new Row(21)
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
    testData.setField(15, Date.valueOf("1996-11-10"))
    testData.setField(16, BigDecimal("0.00000000").bigDecimal)
    testData.setField(17, BigDecimal("10.0").bigDecimal)
    testData.setField(18, Array[Integer](1,2))
    testData.setField(19, Array[(Int, String)]((1,"a"), (2, "b")))
    testData.setField(20, BigDecimal("1514356320000").bigDecimal)
    testData
  }

  def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.BYTE,
      Types.SHORT,
      Types.INT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.BOOLEAN,
      Types.DOUBLE,
      Types.INT,
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      Types.BOOLEAN,
      Types.ROW(Types.STRING, Types.STRING),
      Types.STRING,
      Types.SQL_DATE,
      Types.DECIMAL,
      Types.DECIMAL,
      Types.OBJECT_ARRAY(Types.INT),
      Types.OBJECT_ARRAY(createTypeInformation[(Int, String)]),
      Types.DECIMAL
      ).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "shouldNotExecuteFunc" -> ShouldNotExecuteFunc
  )
}
