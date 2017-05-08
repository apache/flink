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

package org.apache.flink.table.runtime.types

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.{ComparatorTestBase, TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row

class CRowComparatorTest extends ComparatorTestBase[CRow] {

  val rowType = new RowTypeInfo(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO
  )

  val cRowType = new CRowTypeInfo(rowType)

  override protected def createComparator(asc: Boolean): TypeComparator[CRow] = {
    cRowType.createComparator(
      Array[Int](0, 2),
      Array[Boolean](asc, asc),
      0,
      new ExecutionConfig
    )
  }

  override protected def createSerializer(): TypeSerializer[CRow] =
    cRowType.createSerializer(new ExecutionConfig)

  override protected def getSortedTestData: Array[CRow] = Array[CRow](
    new CRow(Row.of(new JInt(1), "Hello", new JLong(1L)), true),
    new CRow(Row.of(new JInt(1), "Hello", new JLong(2L)), true),
    new CRow(Row.of(new JInt(2), "Hello", new JLong(2L)), false),
    new CRow(Row.of(new JInt(2), "Hello", new JLong(3L)), true),
    new CRow(Row.of(new JInt(3), "World", new JLong(0L)), false),
    new CRow(Row.of(new JInt(4), "Hello", new JLong(0L)), true),
    new CRow(Row.of(new JInt(5), "Hello", new JLong(1L)), true),
    new CRow(Row.of(new JInt(5), "Hello", new JLong(4L)), false)
  )
}
