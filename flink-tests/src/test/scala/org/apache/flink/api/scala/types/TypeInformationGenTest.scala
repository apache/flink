/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.types

import java.io.DataInput
import java.io.DataOutput
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.typeutils._
import org.apache.flink.types.{IntValue, StringValue}
import org.apache.hadoop.io.Writable
import org.junit.Assert
import org.junit.Test

import org.apache.flink.api.scala._

class MyWritable extends Writable {
  def write(out: DataOutput) {
  }

  def readFields(in: DataInput) {
  }
}

case class CustomCaseClass(a: String, b: Int)

class CustomType(var myField1: String, var myField2: Int) {
  def this() {
    this(null, 0)
  }
}

class MyObject[A](var a: A) {
  def this() { this(null.asInstanceOf[A]) }
}

class TypeInformationGenTest {

  @Test
  def testBasicType(): Unit = {
    val ti = createTypeInformation[Boolean]

    Assert.assertTrue(ti.isBasicType)
    Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti)
    Assert.assertEquals(classOf[java.lang.Boolean], ti.getTypeClass)
  }

  @Test
  def testWritableType(): Unit = {
    val ti = createTypeInformation[MyWritable]

    Assert.assertTrue(ti.isInstanceOf[WritableTypeInfo[_]])
    Assert.assertEquals(classOf[MyWritable], ti.asInstanceOf[WritableTypeInfo[_]].getTypeClass)
  }

  @Test
  def testTupleWithBasicTypes(): Unit = {
    val ti = createTypeInformation[(Int, Long, Double, Float, Boolean, String, Char, Short, Byte)]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(9, ti.getArity)
    Assert.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[Tuple9[_,_,_,_,_,_,_,_,_]], tti.getTypeClass)
    for (i <- 0 until 0) {
      Assert.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1))
    Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti.getTypeAt(2))
    Assert.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(3))
    Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(4))
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(5))
    Assert.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti.getTypeAt(6))
    Assert.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti.getTypeAt(7))
    Assert.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti.getTypeAt(8))
  }

  @Test
  def testTupleWithTuples(): Unit = {
    val ti = createTypeInformation[(Tuple1[String], Tuple1[Int], Tuple2[Long, Long])]

    Assert.assertTrue(ti.isTupleType())
    Assert.assertEquals(3, ti.getArity)
    Assert.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[Tuple3[_, _, _]], tti.getTypeClass)
    Assert.assertTrue(tti.getTypeAt(0).isTupleType())
    Assert.assertTrue(tti.getTypeAt(1).isTupleType())
    Assert.assertTrue(tti.getTypeAt(2).isTupleType())
    Assert.assertEquals(classOf[Tuple1[_]], tti.getTypeAt(0).getTypeClass)
    Assert.assertEquals(classOf[Tuple1[_]], tti.getTypeAt(1).getTypeClass)
    Assert.assertEquals(classOf[Tuple2[_, _]], tti.getTypeAt(2).getTypeClass)
    Assert.assertEquals(1, tti.getTypeAt(0).getArity)
    Assert.assertEquals(1, tti.getTypeAt(1).getArity)
    Assert.assertEquals(2, tti.getTypeAt(2).getArity)
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO,
      tti.getTypeAt(0).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO,
      tti.getTypeAt(1).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
  }

  @Test
  def testCaseClass(): Unit = {
    val ti = createTypeInformation[CustomCaseClass]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(2, ti.getArity)
    Assert.assertEquals(
      BasicTypeInfo.STRING_TYPE_INFO,
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assert.assertEquals(
      BasicTypeInfo.INT_TYPE_INFO,
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
    Assert.assertEquals(
      classOf[CustomCaseClass],ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeClass())
  }

  @Test
  def testCustomType(): Unit = {
    val ti = createTypeInformation[CustomType]

    Assert.assertFalse(ti.isBasicType)
    Assert.assertFalse(ti.isTupleType)
    Assert.assertTrue(ti.isInstanceOf[PojoTypeInfo[_]])
    Assert.assertEquals(ti.getTypeClass, classOf[CustomType])
  }

  @Test
  def testTupleWithCustomType(): Unit = {
    val ti = createTypeInformation[(Long, CustomType)]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(2, ti.getArity)
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[Tuple2[_, _]], tti.getTypeClass)
    Assert.assertEquals(classOf[java.lang.Long], tti.getTypeAt(0).getTypeClass)
    Assert.assertTrue(tti.getTypeAt(1).isInstanceOf[PojoTypeInfo[_]])
    Assert.assertEquals(classOf[CustomType], tti.getTypeAt(1).getTypeClass)
  }

  @Test
  def testValue(): Unit = {
    val ti = createTypeInformation[StringValue]

    Assert.assertFalse(ti.isBasicType)
    Assert.assertFalse(ti.isTupleType)
    Assert.assertTrue(ti.isInstanceOf[ValueTypeInfo[_]])
    Assert.assertEquals(ti.getTypeClass, classOf[StringValue])
    Assert.assertTrue(TypeExtractor.getForClass(classOf[StringValue])
      .isInstanceOf[ValueTypeInfo[_]])
    Assert.assertEquals(TypeExtractor.getForClass(classOf[StringValue]).getTypeClass,
      ti.getTypeClass)
  }

  @Test
  def testTupleOfValues(): Unit = {
    val ti = createTypeInformation[(StringValue, IntValue)]
    Assert.assertFalse(ti.isBasicType)
    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(
      classOf[StringValue],
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0).getTypeClass)
    Assert.assertEquals(
      classOf[IntValue],
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1).getTypeClass)
  }


  @Test
  def testBasicArray(): Unit = {
    val ti = createTypeInformation[Array[String]]

    Assert.assertFalse(ti.isBasicType)
    Assert.assertFalse(ti.isTupleType)
    Assert.assertTrue(ti.isInstanceOf[BasicArrayTypeInfo[_, _]] ||
      ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    if (ti.isInstanceOf[BasicArrayTypeInfo[_, _]]) {
      Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, ti)
    }
    else {
      Assert.assertEquals(
        BasicTypeInfo.STRING_TYPE_INFO,
        ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo)
    }
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val ti = createTypeInformation[Array[Boolean]]

    Assert.assertTrue(ti.isInstanceOf[PrimitiveArrayTypeInfo[_]])
    Assert.assertEquals(ti, PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)
  }

  @Test
  def testCustomArray(): Unit = {
    val ti = createTypeInformation[Array[CustomType]]
    Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    Assert.assertEquals(
      classOf[CustomType],
      ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentType)
  }

  @Test
  def testTupleArray(): Unit = {
    val ti = createTypeInformation[Array[(String, String)]]

    Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
    Assert.assertTrue(oati.getComponentInfo.isTupleType)
    val tti = oati.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
  }

  @Test
  def testParamertizedCustomObject(): Unit = {
    val ti = createTypeInformation[MyObject[String]]

    Assert.assertTrue(ti.isInstanceOf[PojoTypeInfo[_]])
  }

  @Test
  def testTupleWithPrimitiveArray(): Unit = {
    val ti = createTypeInformation[(Array[Int], Array[Double], Array[Long],
      Array[Byte], Array[Char], Array[Float], Array[Short], Array[Boolean],
      Array[String])]

    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(0))
    Assert.assertEquals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(1))
    Assert.assertEquals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(2))
    Assert.assertEquals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(3))
    Assert.assertEquals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(4))
    Assert.assertEquals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(5))
    Assert.assertEquals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(6))
    Assert.assertEquals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(7))
    Assert.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, tti.getTypeAt(8))
  }
}

