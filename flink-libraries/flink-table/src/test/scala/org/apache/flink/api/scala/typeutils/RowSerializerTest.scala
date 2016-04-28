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
import org.apache.flink.api.common.typeutils.{SerializerTestInstance, TypeSerializer}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{TypeExtractor, TupleTypeInfo}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeutils.RowSerializerTest.MyPojo
import org.junit.Assert._
import org.junit.Test

class RowSerializerTest {

  class RowSerializerTestInstance(
      serializer: TypeSerializer[Row],
      testData: Array[Row])
    extends SerializerTestInstance[Row](serializer, classOf[Row], -1, testData: _*) {

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
  }

  @Test
  def testRowSerializer(): Unit = {
    val rowInfo: TypeInformation[Row] = new RowTypeInfo(
      Seq(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))

    val row1 = new Row(2)
    row1.setField(0, 1)
    row1.setField(1, "a")

    val row2 = new Row(2)
    row2.setField(0, 2)
    row2.setField(1, null)

    val testData: Array[Row] = Array(row1, row2)

    val rowSerializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)

    val testInstance = new RowSerializerTestInstance(rowSerializer, testData)

    testInstance.testAll()
  }

  @Test
  def testLargeRowSerializer(): Unit = {
    val rowInfo: TypeInformation[Row] = new RowTypeInfo(Seq(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))

    val row = new Row(13)
    row.setField(0, 2)
    row.setField(1, null)
    row.setField(3, null)
    row.setField(4, null)
    row.setField(5, null)
    row.setField(6, null)
    row.setField(7, null)
    row.setField(8, null)
    row.setField(9, null)
    row.setField(10, null)
    row.setField(11, null)
    row.setField(12, "Test")

    val testData: Array[Row] = Array(row)

    val rowSerializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)

    val testInstance = new RowSerializerTestInstance(rowSerializer, testData)

    testInstance.testAll()
  }

  @Test
  def testRowSerializerWithComplexTypes(): Unit = {
    val rowInfo = new RowTypeInfo(
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
    testPojo1.name = null
    val testPojo2 = new MyPojo()
    testPojo2.name = "Test1"
    val testPojo3 = new MyPojo()
    testPojo3.name = "Test2"

    val testData: Array[Row] = Array(
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

    val rowSerializer: TypeSerializer[Row] = rowInfo.createSerializer(new ExecutionConfig)

    val testInstance = new RowSerializerTestInstance(rowSerializer, testData)

    testInstance.testAll()
  }

  // ----------------------------------------------------------------------------------------------

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

object RowSerializerTest {
  class MyPojo() extends Serializable with Comparable[MyPojo] {
    var name: String = null

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
