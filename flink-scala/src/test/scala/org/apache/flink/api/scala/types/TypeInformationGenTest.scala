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
package org.apache.flink.api.scala.types

import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.TypeExtractorTest.CustomTuple
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, UnitTypeInfo}
import org.apache.flink.types.{IntValue, StringValue}

import org.junit.{Assert, Test}

case class CustomCaseClass(a: String, b: Int)

case class UmlautCaseClass(ä: String, ß: Int)

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
  def testJavaTuple(): Unit = {
    val ti = createTypeInformation[org.apache.flink.api.java.tuple.Tuple3[Int, String, Integer]]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(3, ti.getArity)
    Assert.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[org.apache.flink.api.java.tuple.Tuple3[_, _, _]], tti.getTypeClass)
    for (i <- 0 until 3) {
      Assert.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(2))
  }

  @Test
  def testCustomJavaTuple(): Unit = {
    val ti = createTypeInformation[CustomTuple]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(2, ti.getArity)
    Assert.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[CustomTuple], tti.getTypeClass)
    for (i <- 0 until 2) {
      Assert.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(1))
  }

  @Test
  def testBasicType(): Unit = {
    val ti = createTypeInformation[Boolean]

    Assert.assertTrue(ti.isBasicType)
    Assert.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti)
    Assert.assertEquals(classOf[java.lang.Boolean], ti.getTypeClass)
  }

  @Test
  def testTypeParameters(): Unit = {

    val data = Seq(1.0d, 2.0d)

    def f[T: TypeInformation](data: Seq[T]): (T, Seq[T]) = {

      val ti = createTypeInformation[(T, Seq[T])]

      Assert.assertTrue(ti.isTupleType)
      val ccti = ti.asInstanceOf[CaseClassTypeInfo[(T, Seq[T])]]
      Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, ccti.getTypeAt(0))

      (data.head, data)
    }

    f(data)

  }

  @Test
  def testGenericArrays(): Unit = {

    class MyObject(var a: Int, var b: String) {
      def this() = this(0, "")
    }

    val boolArray = Array(true, false)
    val byteArray = Array(1.toByte, 2.toByte, 3.toByte)
    val charArray= Array(1.toChar, 2.toChar, 3.toChar)
    val shortArray = Array(1.toShort, 2.toShort, 3.toShort)
    val intArray = Array(1, 2, 3)
    val longArray = Array(1L, 2L, 3L)
    val floatArray = Array(1.0f, 2.0f, 3.0f)
    val doubleArray = Array(1.0, 2.0, 3.0)
    val stringArray = Array("hey", "there")
    val objectArray = Array(new MyObject(1, "hey"), new MyObject(2, "there"))

    def getType[T: TypeInformation](arr: Array[T]): TypeInformation[Array[T]] = {
      createTypeInformation[Array[T]]
    }

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(boolArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(byteArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(charArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(shortArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(intArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(longArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(floatArray))

    Assert.assertEquals(
      PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(doubleArray))

    Assert.assertEquals(
      BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
      getType(stringArray))

    Assert.assertTrue(getType(objectArray).isInstanceOf[ObjectArrayTypeInfo[_, _]])
    Assert.assertTrue(
      getType(objectArray).asInstanceOf[ObjectArrayTypeInfo[_, _]]
        .getComponentInfo.isInstanceOf[PojoTypeInfo[_]])
  }

  @Test
  def testTupleWithBasicTypes(): Unit = {
    val ti = createTypeInformation[(Int, Long, Double, Float, Boolean, String, Char, Short, Byte)]

    Assert.assertTrue(ti.isTupleType)
    Assert.assertEquals(9, ti.getArity)
    Assert.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assert.assertEquals(classOf[Tuple9[_,_,_,_,_,_,_,_,_]], tti.getTypeClass)
    for (i <- 0 until 9) {
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
      ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo.getTypeClass)
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
  def testMultidimensionalArrays(): Unit = {
    // Tuple
    {
      val ti = createTypeInformation[Array[Array[(String, String)]]]
    
      Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assert.assertTrue(oati.getComponentInfo.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assert.assertTrue(oati2.getComponentInfo.isTupleType)
      val tti = oati2.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
      Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
      Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
    }
    
    // primitives
    {
      val ti = createTypeInformation[Array[Array[Int]]]
    
      Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assert.assertEquals(oati.getComponentInfo,
        PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)
    }
    
    // basic types
    {
      val ti = createTypeInformation[Array[Array[Integer]]]
    
      Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assert.assertEquals(oati.getComponentInfo, BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO)
    }
    
    // pojo
    {
      val ti = createTypeInformation[Array[Array[CustomType]]]
    
      Assert.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assert.assertTrue(oati.getComponentInfo.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      val tti = oati2.getComponentInfo.asInstanceOf[PojoTypeInfo[_]]
      Assert.assertEquals(classOf[CustomType], tti.getTypeClass())
    }
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

  @Test
  def testTrait(): Unit = {
    trait TestTrait {
      def foo() = 1
      def bar(x: Int): Int
    }

    val ti = createTypeInformation[TestTrait]

    Assert.assertTrue(ti.isInstanceOf[GenericTypeInfo[TestTrait]])
  }

  @Test
  def testGetFlatFields(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    Assert.assertEquals(0, tupleTypeInfo.getFlatFields("0").get(0).getPosition)
    Assert.assertEquals(1, tupleTypeInfo.getFlatFields("1").get(0).getPosition)
    Assert.assertEquals(2, tupleTypeInfo.getFlatFields("2").get(0).getPosition)
    Assert.assertEquals(3, tupleTypeInfo.getFlatFields("3").get(0).getPosition)
    Assert.assertEquals(0, tupleTypeInfo.getFlatFields("_1").get(0).getPosition)
    Assert.assertEquals(1, tupleTypeInfo.getFlatFields("_2").get(0).getPosition)
    Assert.assertEquals(2, tupleTypeInfo.getFlatFields("_3").get(0).getPosition)
    Assert.assertEquals(3, tupleTypeInfo.getFlatFields("_4").get(0).getPosition)

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    Assert.assertEquals(0, nestedTypeInfo.getFlatFields("0").get(0).getPosition)
    Assert.assertEquals(1, nestedTypeInfo.getFlatFields("1.0").get(0).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("1.1").get(0).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("1.2").get(0).getPosition)
    Assert.assertEquals(4, nestedTypeInfo.getFlatFields("2").get(0).getPosition)
    Assert.assertEquals(5, nestedTypeInfo.getFlatFields("3.0").get(0).getPosition)
    Assert.assertEquals(6, nestedTypeInfo.getFlatFields("3.1").get(0).getPosition)
    Assert.assertEquals(4, nestedTypeInfo.getFlatFields("_3").get(0).getPosition)
    Assert.assertEquals(5, nestedTypeInfo.getFlatFields("_4._1").get(0).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("1").size)
    Assert.assertEquals(1, nestedTypeInfo.getFlatFields("1").get(0).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("1").get(1).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("1").get(2).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("1.*").size)
    Assert.assertEquals(1, nestedTypeInfo.getFlatFields("1.*").get(0).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("1.*").get(1).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("1.*").get(2).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("3").size)
    Assert.assertEquals(5, nestedTypeInfo.getFlatFields("3").get(0).getPosition)
    Assert.assertEquals(6, nestedTypeInfo.getFlatFields("3").get(1).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("_2").size)
    Assert.assertEquals(1, nestedTypeInfo.getFlatFields("_2").get(0).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("_2").get(1).getPosition)
    Assert.assertEquals(3, nestedTypeInfo.getFlatFields("_2").get(2).getPosition)
    Assert.assertEquals(2, nestedTypeInfo.getFlatFields("_4").size)
    Assert.assertEquals(5, nestedTypeInfo.getFlatFields("_4").get(0).getPosition)
    Assert.assertEquals(6, nestedTypeInfo.getFlatFields("_4").get(1).getPosition)
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO,
      nestedTypeInfo.getFlatFields("0").get(0).getType)
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO,
      nestedTypeInfo.getFlatFields("1.1").get(0).getType)
    Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      nestedTypeInfo.getFlatFields("1").get(2).getType)
    Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO,
      nestedTypeInfo.getFlatFields("3").get(1).getType)

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    Assert.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("1").size)
    Assert.assertEquals(1, deepNestedTupleTypeInfo.getFlatFields("1").get(0).getPosition)
    Assert.assertEquals(2, deepNestedTupleTypeInfo.getFlatFields("1").get(1).getPosition)
    Assert.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("1").get(2).getPosition)
    Assert.assertEquals(5, deepNestedTupleTypeInfo.getFlatFields("*").size)
    Assert.assertEquals(0, deepNestedTupleTypeInfo.getFlatFields("*").get(0).getPosition)
    Assert.assertEquals(1, deepNestedTupleTypeInfo.getFlatFields("*").get(1).getPosition)
    Assert.assertEquals(2, deepNestedTupleTypeInfo.getFlatFields("*").get(2).getPosition)
    Assert.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("*").get(3).getPosition)
    Assert.assertEquals(4, deepNestedTupleTypeInfo.getFlatFields("*").get(4).getPosition)

    val caseClassTypeInfo = createTypeInformation[CustomCaseClass].
      asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    Assert.assertEquals(0, caseClassTypeInfo.getFlatFields("a").get(0).getPosition)
    Assert.assertEquals(1, caseClassTypeInfo.getFlatFields("b").get(0).getPosition)
    Assert.assertEquals(2, caseClassTypeInfo.getFlatFields("*").size)
    Assert.assertEquals(0, caseClassTypeInfo.getFlatFields("*").get(0).getPosition)
    Assert.assertEquals(1, caseClassTypeInfo.getFlatFields("*").get(1).getPosition)

    val caseClassInTupleTypeInfo = createTypeInformation[(Int, UmlautCaseClass)].
      asInstanceOf[CaseClassTypeInfo[(Int, UmlautCaseClass)]]
    Assert.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("_2.ä").get(0).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1.ß").get(0).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1").size)
    Assert.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("1.*").get(0).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1").get(1).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("_2.*").size)
    Assert.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("_2.*").get(0).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("_2").get(1).getPosition)
    Assert.assertEquals(3, caseClassInTupleTypeInfo.getFlatFields("*").size)
    Assert.assertEquals(0, caseClassInTupleTypeInfo.getFlatFields("*").get(0).getPosition)
    Assert.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("*").get(1).getPosition)
    Assert.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("*").get(2).getPosition)

  }

  @Test
  def testFieldAtStringRef(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("0"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("2"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("_2"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("_4"))

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("0"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("1.0"))
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, nestedTypeInfo.getTypeAt("1.1"))
    Assert.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, nestedTypeInfo.getTypeAt("1.2"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("2"))
    Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("3.0"))
    Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("3.1"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("_3"))
    Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("_4._1"))
    Assert.assertEquals(createTypeInformation[(Int, String, Long)], nestedTypeInfo.getTypeAt("1"))
    Assert.assertEquals(createTypeInformation[(Double, Double)], nestedTypeInfo.getTypeAt("3"))
    Assert.assertEquals(createTypeInformation[(Int, String, Long)], nestedTypeInfo.getTypeAt("_2"))
    Assert.assertEquals(createTypeInformation[(Double, Double)], nestedTypeInfo.getTypeAt("_4"))

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    Assert.assertEquals(createTypeInformation[(Int, (Int, Int))],
      deepNestedTupleTypeInfo.getTypeAt("1"))

    val umlautCaseClassTypeInfo = createTypeInformation[UmlautCaseClass].
      asInstanceOf[CaseClassTypeInfo[UmlautCaseClass]]
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, umlautCaseClassTypeInfo.getTypeAt("ä"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, umlautCaseClassTypeInfo.getTypeAt("ß"))

    val caseClassTypeInfo = createTypeInformation[CustomCaseClass].
      asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    val caseClassInTupleTypeInfo = createTypeInformation[(Int, CustomCaseClass)].
      asInstanceOf[CaseClassTypeInfo[(Int, CustomCaseClass)]]
    Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, caseClassInTupleTypeInfo.getTypeAt("_2.a"))
    Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, caseClassInTupleTypeInfo.getTypeAt("1.b"))
    Assert.assertEquals(caseClassTypeInfo, caseClassInTupleTypeInfo.getTypeAt("1"))
    Assert.assertEquals(caseClassTypeInfo, caseClassInTupleTypeInfo.getTypeAt("_2"))

  }

  /**
   * Tests the "implicit val scalaNothingTypeInfo" in
   * flink-scala/src/main/scala/org/apache/flink/api/scala/package.scala
   * This does not compile without that line.
   */
  @Test
  def testNothingTypeInfoIsAvailableImplicitly() : Unit = {
    def g() = {

      def f[O: TypeInformation](x: O): Unit = {}

      f(???) // O will be Nothing
    }
    // (Do not call g, because it throws NotImplementedError. This is a compile time test.)
  }

  @Test
  def testUnit(): Unit = {
    val ti = createTypeInformation[Unit]
    Assert.assertTrue(ti.isInstanceOf[UnitTypeInfo])

    // This checks the condition in checkCollection. If this fails with IllegalArgumentException,
    // then things like "env.fromElements((),(),())" won't work.
    import scala.collection.JavaConversions._
    CollectionInputFormat.checkCollection(Seq((),(),()), (new UnitTypeInfo).getTypeClass())
  }
}

