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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, Types}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, RowTypeInfo}
import org.apache.flink.types.Row

import com.google.common.collect.ImmutableMap

import java.util.{HashMap => JHashMap}

abstract class MapTypeTestBase extends ExpressionTestBase {

  override def testData: Row = {
    val map1 = new JHashMap[String, Int]()
    map1.put("a", 12)
    map1.put("b", 13)
    val map2 = new JHashMap[Int, String]()
    map2.put(12, "a")
    map2.put(13, "b")
    val map3 = new JHashMap[Long, Int]()
    map3.put(10L, 1)
    map3.put(20L, 2)
    val map4 = new JHashMap[Int, Array[Int]]()
    map4.put(1, Array(10, 100))
    map4.put(2, Array(20, 200))
    val testData = new Row(12)
    testData.setField(0, null)
    testData.setField(1, new JHashMap[String, Int]())
    testData.setField(2, map1)
    testData.setField(3, map2)
    testData.setField(4, "foo")
    testData.setField(5, 12)
    testData.setField(6, Array(1.2, 1.3))
    testData.setField(7, ImmutableMap.of(12, "a", 13, "b"))
    testData.setField(8, map3)
    testData.setField(9, map3)
    testData.setField(10, map4)
    testData.setField(11, map4)
    testData
  }

  override def typeInfo: RowTypeInfo = {
    new RowTypeInfo(
      /* 0 */  new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      /* 1 */  new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      /* 2 */  new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      /* 3 */  new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      /* 4 */  Types.STRING,
      /* 5 */  Types.INT,
      /* 6 */  Types.PRIMITIVE_ARRAY(Types.DOUBLE),
      /* 7 */  new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      /* 8 */  new MapTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      /* 9 */  new MapTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      /* 10 */ new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, Types.PRIMITIVE_ARRAY(Types.INT)),
      /* 11 */ new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, Types.PRIMITIVE_ARRAY(Types.INT))
    )
  }
}
