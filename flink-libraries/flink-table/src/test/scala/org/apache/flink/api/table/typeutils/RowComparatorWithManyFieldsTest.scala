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

package org.apache.flink.api.table.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.{ComparatorTestBase, TypeComparator, TypeSerializer}
import org.apache.flink.api.table.Row
import org.apache.flink.util.Preconditions
import org.junit.Assert._

/**
  * Tests [[RowComparator]] for wide rows.
  */
class RowComparatorWithManyFieldsTest extends ComparatorTestBase[Row] {
  val numberOfFields = 10
  val fieldTypes = new Array[TypeInformation[_]](numberOfFields)
  for (i <- 0 until numberOfFields) {
    fieldTypes(i) = BasicTypeInfo.STRING_TYPE_INFO
  }
  val typeInfo = new RowTypeInfo(fieldTypes)

  val data: Array[Row] = Array(
    createRow(Array(null, "b0", "c0", "d0", "e0", "f0", "g0", "h0", "i0", "j0")),
    createRow(Array("a1", "b1", "c1", "d1", "e1", "f1", "g1", "h1", "i1", "j1")),
    createRow(Array("a2", "b2", "c2", "d2", "e2", "f2", "g2", "h2", "i2", "j2")),
    createRow(Array("a3", "b3", "c3", "d3", "e3", "f3", "g3", "h3", "i3", "j3"))
  )

  override protected def deepEquals(message: String, should: Row, is: Row): Unit = {
    val arity = should.productArity
    assertEquals(message, arity, is.productArity)
    var index = 0
    while (index < arity) {
      val copiedValue: Any = should.productElement(index)
      val element: Any = is.productElement(index)
      assertEquals(message, element, copiedValue)
      index += 1
    }
  }

  override protected def createComparator(ascending: Boolean): TypeComparator[Row] = {
    typeInfo.createComparator(
      Array(0),
      Array(ascending),
      0,
      new ExecutionConfig())
  }

  override protected def createSerializer(): TypeSerializer[Row] = {
    typeInfo.createSerializer(new ExecutionConfig())
  }

  override protected def getSortedTestData: Array[Row] = {
    data
  }

  override protected def supportsNullKeys: Boolean = true

  private def createRow(values: Array[_]): Row = {
    Preconditions.checkArgument(values.length == numberOfFields)
    val r: Row = new Row(numberOfFields)
    values.zipWithIndex.foreach { case (e, i) => r.setField(i, e) }
    r
  }
}
