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

package org.apache.flink.table.runtime.conversion

import java.util.{Map => JavaMap}

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types._
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.dataformat._
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.conversion.DataStructureConverters._
import org.apache.flink.table.typeutils.{BaseArraySerializer, BaseMapSerializer}
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._

class DataStructureConvertersTest {

  private val simpleTypes: Seq[DataType] = Seq(
    DataTypes.STRING,
    DataTypes.DATE,
    DataTypes.BOOLEAN,
    DataTypes.BYTE,
    DataTypes.SHORT,
    DataTypes.INT,
    DataTypes.LONG,
    DataTypes.FLOAT,
    DataTypes.DOUBLE,
    DataTypes.createDecimalType(38, 19))

  @Test
  def testNullHandlingInRows(): Unit = {
    val schema = DataTypes.createRowType(simpleTypes: _*)
    val convertToInternal = createToInternalConverter(schema)
    val convertToScala = createToExternalConverter(schema)

    val row = new Row(simpleTypes.length)
    assert(convertToScala(convertToInternal(row)) == row)
  }

  @Test
  def testNullHandlingIndividualValues(): Unit = {
    for (dataType <- simpleTypes) {
      assertTrue(createToExternalConverter(dataType)(null) == null)
    }
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val intArray = Array(1, 100, 10000)
    val intBinaryArray = BinaryArray.fromPrimitiveArray(intArray)
    val intArrayType = DataTypes.createPrimitiveArrayType(DataTypes.INT)
    assertTrue(createToExternalConverter(intArrayType)(intBinaryArray).asInstanceOf[Array[Int]]
        sameElements intArray)

    val doubleArray = Array(1.1, 111.1, 11111.1)
    val doubleBinaryArray = BinaryArray.fromPrimitiveArray(doubleArray)
    val doubleArrayType = DataTypes.createPrimitiveArrayType(DataTypes.DOUBLE)
    assertTrue(createToExternalConverter(doubleArrayType)(doubleBinaryArray)
        .asInstanceOf[Array[Double]]
        sameElements doubleArray)
  }

  @Test
  def testArrayNullHandling(): Unit = {

    def toBinaryArray(javaArray: Array[_], t: InternalType): BinaryArray = {
      val array = new BinaryArray
      val writer = new BinaryArrayWriter(
        array, javaArray.length, BinaryArray.calculateElementSize(t))
      javaArray.zipWithIndex.foreach { case (field, index) =>
        if (field == null) {
          writer.setNullAt(index, t)
        } else BaseRowUtil.write(writer, index, field, t,
          DataTypes.createInternalSerializer(t))
      }
      writer.complete()
      array
    }

    val intArray = Array[java.lang.Integer](1, null, 100, null, 10000)
    val intArrayType = DataTypes.createArrayType(DataTypes.INT)
    val internalIntArray = toBinaryArray(intArray, DataTypes.INT)
    assertTrue(createToExternalConverter(intArrayType)(internalIntArray).asInstanceOf[Array[Any]]
        sameElements intArray)

    val baseIntArray = createToInternalConverter(intArrayType)(intArray).asInstanceOf[BaseArray]
    intArray.zipWithIndex.foreach(e =>
      if (e._1 == null) {
        assertTrue(baseIntArray.isNullAt(e._2))
      } else {
        assertEquals(e._1, baseIntArray.getInt(e._2))
      }
    )

    val binaryIntArray = DataTypes.createInternalSerializer(intArrayType)
      .asInstanceOf[BaseArraySerializer].baseArrayToBinary(baseIntArray)
    assertEquals(internalIntArray, binaryIntArray)

    val doubleArray = Array[java.lang.Double](1.1, null, 111.1, null, 11111.1)
    val doubleArrayType = DataTypes.createArrayType(DataTypes.DOUBLE)
    val internalDoubleArray = toBinaryArray(doubleArray, DataTypes.DOUBLE)
    assertTrue(createToExternalConverter(
      doubleArrayType)(internalDoubleArray).asInstanceOf[Array[Any]] sameElements doubleArray)

    val baseDoubleArray = createToInternalConverter(
      doubleArrayType)(doubleArray).asInstanceOf[BaseArray]
    doubleArray.zipWithIndex.foreach(e =>
      if (e._1 == null) {
        assertTrue(baseDoubleArray.isNullAt(e._2))
      } else {
        assertEquals(e._1, baseDoubleArray.getDouble(e._2))
      }
    )

    val binaryDoubleArray = DataTypes.createInternalSerializer(doubleArrayType)
      .asInstanceOf[BaseArraySerializer].baseArrayToBinary(baseDoubleArray)
    assertEquals(internalDoubleArray, binaryDoubleArray)
  }

  @Test
  def testString(): Unit = {
    val value = "InternalTypeConvertersTest"
    val internal = createToInternalConverter(DataTypes.STRING)(value)
    assertEquals(fromString(value), internal)
    assertEquals(value, createToExternalConverter(DataTypes.STRING)(internal))
  }

  @Test
  def testCaseClass(): Unit = {
    val value = MyCaseClass(5, 10)
    val t: DataType = createTypeInformation[MyCaseClass]
    val internal = createToInternalConverter(t)(value).asInstanceOf[GenericRow]
    assertEquals(5, internal.getField(0))
    assertEquals(10, internal.getField(1))
    assertTrue(value == createToExternalConverter(t)(internal))
  }

  @Test
  def testTuple(): Unit = {
    val value = new org.apache.flink.api.java.tuple.Tuple2(5, 10)
    val t = DataTypes.createTupleType(value.getClass, DataTypes.INT, DataTypes.INT)
    val internal = createToInternalConverter(t)(value).asInstanceOf[GenericRow]
    assertEquals(5, internal.getField(0))
    assertEquals(10, internal.getField(1))
    assertTrue(value == createToExternalConverter(t)(internal))
  }

  @Test
  def testPojo(): Unit = {
    val value = new MyPojo(5, 10)
    val t: DataType = TypeExtractor.createTypeInfo(value.getClass)
    val internal = createToInternalConverter(t)(value).asInstanceOf[GenericRow]
    assertEquals(5, internal.getField(0))
    assertEquals(10, internal.getField(1))
    assertTrue(value == createToExternalConverter(t)(internal).asInstanceOf[MyPojo])
  }

  @Test
  def testRow(): Unit = {
    val t = DataTypes.createRowType(DataTypes.INT, DataTypes.INT)
    val value = new Row(2)
    value.setField(0, 5)
    value.setField(1, 6)
    val internal = createToInternalConverter(t)(value).asInstanceOf[GenericRow]
    assertEquals(5, internal.getField(0))
    assertEquals(6, internal.getField(1))
    assertTrue(value == createToExternalConverter(t)(internal).asInstanceOf[Row])
  }

  @Test
  def testArray(): Unit = {
    val value = Array(1, 5, 6)
    val t = DataTypes.createPrimitiveArrayType(DataTypes.INT)
    val internal = createToInternalConverter(t)(value)
        .asInstanceOf[BaseArray]
    assertEquals(1, internal.getInt(0))
    assertEquals(5, internal.getInt(1))
    assertEquals(6, internal.getInt(2))
    assertTrue(value sameElements
        createToExternalConverter(t)(internal).asInstanceOf[Array[Int]])
  }

  @Test
  def testMap(): Unit = {
    val t = new MapType(DataTypes.INT, DataTypes.STRING)
    val value = Map(1 -> "haha", 5 -> "ahha", 6 -> "hehe", null.asInstanceOf[Int] -> "null")
    val internal = createToInternalConverter(t)(value).asInstanceOf[BaseMap]
    val javaMap = internal.toJavaMap(DataTypes.INT, DataTypes.STRING)
    assertEquals(fromString("haha"), javaMap.get(1))
    assertEquals(fromString("ahha"), javaMap.get(5))
    assertEquals(fromString("hehe"), javaMap.get(6))
    assertEquals(fromString("null"), javaMap.get(null))

    val binaryMap = DataTypes.createInternalSerializer(t)
      .asInstanceOf[BaseMapSerializer].baseMapToBinary(internal)
    assertTrue(value.asJava == createToExternalConverter(t)(binaryMap).asInstanceOf[JavaMap[_, _]])
  }
}

case class MyCaseClass(f1: Int, f2: Int)

class MyPojo() {
  var f1: Int = 0
  var f2: Int = 0

  def this(f1: Int, f2: Int) {
    this()
    this.f1 = f1
    this.f2 = f2
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyPojo]

  override def equals(other: Any): Boolean = other match {
    case that: MyPojo =>
      (that canEqual this) &&
          f1 == that.f1 &&
          f2 == that.f2
    case _ => false
  }
}
