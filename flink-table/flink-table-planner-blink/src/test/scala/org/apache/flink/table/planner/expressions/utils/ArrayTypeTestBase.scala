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

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, Types}
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.types.Row

abstract class ArrayTypeTestBase extends ExpressionTestBase {

  case class MyCaseClass(string: String, int: Int)

  override def testData: Row = {
    val testData = new Row(12)
    testData.setField(0, null)
    testData.setField(1, 42)
    testData.setField(2, Array(1, 2, 3))
    testData.setField(3, Array(localDate("1984-03-12"), localDate("1984-02-10")))
    testData.setField(4, null)
    testData.setField(5, Array(Array(1, 2, 3), null))
    testData.setField(6, Array[Integer](1, null, null, 4))
    testData.setField(7, Array(1, 2, 3, 4))
    testData.setField(8, Array(4.0))
    testData.setField(9, Array[Integer](1))
    testData.setField(10, Array[Integer]())
    testData.setField(11, Array[Integer](1))
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  Types.INT,
      /* 1 */  Types.INT,
      /* 2 */  PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      /* 3 */  ObjectArrayTypeInfo.getInfoFor(Types.LOCAL_DATE),
      /* 4 */  ObjectArrayTypeInfo.getInfoFor(ObjectArrayTypeInfo.getInfoFor(Types.INT)),
      /* 5 */  ObjectArrayTypeInfo.getInfoFor(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO),
      /* 6 */  ObjectArrayTypeInfo.getInfoFor(Types.INT),
      /* 7 */  PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      /* 8 */  PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
      /* 9 */  ObjectArrayTypeInfo.getInfoFor(Types.INT),
      /* 10 */ ObjectArrayTypeInfo.getInfoFor(Types.INT),
      /* 11 */ BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO
    )
  }
}
