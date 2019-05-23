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

import java.util.{HashMap => JHashMap}

import com.google.common.collect.ImmutableMap
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

class MapTypeTestBase extends ExpressionTestBase {

  override def testData: Any = {
    val map1 = new JHashMap[String, Int]()
    map1.put("a", 12)
    map1.put("b", 13)
    val map2 = new JHashMap[Int, String]()
    map2.put(12, "a")
    map2.put(13, "b")
    val testData = new Row(8)
    testData.setField(0, null)
    testData.setField(1, new JHashMap[String, Int]())
    testData.setField(2, map1)
    testData.setField(3, map2)
    testData.setField(4, "foo")
    testData.setField(5, 12)
    testData.setField(6, Array(1.2, 1.3))
    testData.setField(7, ImmutableMap.of(12, "a", 13, "b"))
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO),
      new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      Types.STRING,
      Types.INT,
      Types.OBJECT_ARRAY(Types.DOUBLE),
      new MapTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    ).asInstanceOf[TypeInformation[Any]]
  }
}
