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

package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.{ComparatorTestBase, TypeComparator, TypeSerializer}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.RowComparatorTest.MyPojo
import org.junit.Assert._

class RowComparatorTest extends ComparatorTestBase[Row] {

  val typeInfo = new RowTypeInfo(
    Array(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      new TupleTypeInfo[tuple.Tuple2[Int, Boolean]](
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.BOOLEAN_TYPE_INFO,
        BasicTypeInfo.SHORT_TYPE_INFO),
      TypeExtractor.createTypeInfo(classOf[MyPojo])))

  val testPojo1 = new MyPojo()
  // TODO we cannot test null here as PojoComparator has no support for null keys
  testPojo1.name = ""
  val testPojo2 = new MyPojo()
  testPojo2.name = "Test1"
  val testPojo3 = new MyPojo()
  testPojo3.name = "Test2"

  val data: Array[Row] = Array(
    createRow(null, null, null, null, null),
    createRow(0, null, null, null, null),
    createRow(0, 0.0, null, null, null),
    createRow(0, 0.0, "a", null, null),
    createRow(1, 0.0, "a", null, null),
    createRow(1, 1.0, "a", null, null),
    createRow(1, 1.0, "b", null, null),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](1, false, 2), null),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, false, 2), null),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, true, 2), null),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, true, 3), null),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, true, 3), testPojo1),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, true, 3), testPojo2),
    createRow(1, 1.0, "b", new tuple.Tuple3[Int, Boolean, Short](2, true, 3), testPojo3)
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
      Array(0, 1, 2, 3, 4, 5, 6),
      Array(ascending, ascending, ascending, ascending, ascending, ascending, ascending),
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

  def createRow(f0: Any, f1: Any, f2: Any, f3: Any, f4: Any): Row = {
    val r: Row = new Row(5)
    r.setField(0, f0)
    r.setField(1, f1)
    r.setField(2, f2)
    r.setField(3, f3)
    r.setField(4, f4)
    r
  }
}

object RowComparatorTest {
  class MyPojo() extends Serializable with Comparable[MyPojo] {
    // we cannot use null because the PojoComparator does not support null properly
    var name: String = ""

    override def compareTo(o: MyPojo): Int = {
      if (name == null && o.name == null) {
        0
      }
      else if (name == null) {
        -1
      }
      else if (o.name == null) {
        1
      }
      else {
        name.compareTo(o.name)
      }
    }

    override def equals(other: Any): Boolean = other match {
      case that: MyPojo => compareTo(that) == 0
      case _ => false
    }
  }
}
