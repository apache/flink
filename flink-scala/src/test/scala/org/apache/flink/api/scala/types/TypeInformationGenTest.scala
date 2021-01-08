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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.TypeExtractorTest.CustomTuple
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, TraversableSerializer, UnitTypeInfo}
import org.apache.flink.types.{IntValue, StringValue}
import org.junit.{Assert, Test}

@SerialVersionUID(-1509730037212683566L)
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

    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(3, ti.getArity)
    Assertions.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(classOf[org.apache.flink.api.java.tuple.Tuple3[_, _, _]], tti.getTypeClass)
    for (i <- 0 until 3) {
      Assertions.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(2))
  }

  @Test
  def testCustomJavaTuple(): Unit = {
    val ti = createTypeInformation[CustomTuple]

    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(2, ti.getArity)
    Assertions.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(classOf[CustomTuple], tti.getTypeClass)
    for (i <- 0 until 2) {
      Assertions.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(1))
  }

  @Test
  def testBasicType(): Unit = {
    val ti = createTypeInformation[Boolean]

    Assertions.assertTrue(ti.isBasicType)
    Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, ti)
    Assertions.assertEquals(classOf[java.lang.Boolean], ti.getTypeClass)
  }

  @Test
  def testTypeParameters(): Unit = {

    val data = Seq(1.0d, 2.0d)

    def f[T: TypeInformation](data: Seq[T]): (T, Seq[T]) = {

      val ti = createTypeInformation[(T, Seq[T])]

      Assertions.assertTrue(ti.isTupleType)
      val ccti = ti.asInstanceOf[CaseClassTypeInfo[(T, Seq[T])]]
      Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, ccti.getTypeAt(0))

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

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(boolArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(byteArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(charArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(shortArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(intArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(longArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(floatArray))

    Assertions.assertEquals(
      PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
      getType(doubleArray))

    Assertions.assertEquals(
      BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
      getType(stringArray))

    Assertions.assertTrue(getType(objectArray).isInstanceOf[ObjectArrayTypeInfo[_, _]])
    Assertions.assertTrue(
      getType(objectArray).asInstanceOf[ObjectArrayTypeInfo[_, _]]
        .getComponentInfo.isInstanceOf[PojoTypeInfo[_]])
  }

  @Test
  def testTupleWithBasicTypes(): Unit = {
    val ti = createTypeInformation[(Int, Long, Double, Float, Boolean, String, Char, Short, Byte)]

    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(9, ti.getArity)
    Assertions.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(classOf[Tuple9[_,_,_,_,_,_,_,_,_]], tti.getTypeClass)
    for (i <- 0 until 9) {
      Assertions.assertTrue(tti.getTypeAt(i).isInstanceOf[BasicTypeInfo[_]])
    }

    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tti.getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, tti.getTypeAt(1))
    Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tti.getTypeAt(2))
    Assertions.assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, tti.getTypeAt(3))
    Assertions.assertEquals(BasicTypeInfo.BOOLEAN_TYPE_INFO, tti.getTypeAt(4))
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(5))
    Assertions.assertEquals(BasicTypeInfo.CHAR_TYPE_INFO, tti.getTypeAt(6))
    Assertions.assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tti.getTypeAt(7))
    Assertions.assertEquals(BasicTypeInfo.BYTE_TYPE_INFO, tti.getTypeAt(8))
  }

  @Test
  def testTupleWithTuples(): Unit = {
    val ti = createTypeInformation[(Tuple1[String], Tuple1[Int], Tuple2[Long, Long])]

    Assertions.assertTrue(ti.isTupleType())
    Assertions.assertEquals(3, ti.getArity)
    Assertions.assertTrue(ti.isInstanceOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(classOf[Tuple3[_, _, _]], tti.getTypeClass)
    Assertions.assertTrue(tti.getTypeAt(0).isTupleType())
    Assertions.assertTrue(tti.getTypeAt(1).isTupleType())
    Assertions.assertTrue(tti.getTypeAt(2).isTupleType())
    Assertions.assertEquals(classOf[Tuple1[_]], tti.getTypeAt(0).getTypeClass)
    Assertions.assertEquals(classOf[Tuple1[_]], tti.getTypeAt(1).getTypeClass)
    Assertions.assertEquals(classOf[Tuple2[_, _]], tti.getTypeAt(2).getTypeClass)
    Assertions.assertEquals(1, tti.getTypeAt(0).getArity)
    Assertions.assertEquals(1, tti.getTypeAt(1).getArity)
    Assertions.assertEquals(2, tti.getTypeAt(2).getArity)
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO,
      tti.getTypeAt(0).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO,
      tti.getTypeAt(1).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
  }

  @Test
  def testCaseClass(): Unit = {
    val ti = createTypeInformation[CustomCaseClass]

    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(2, ti.getArity)
    Assertions.assertEquals(
      BasicTypeInfo.STRING_TYPE_INFO,
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
    Assertions.assertEquals(
      BasicTypeInfo.INT_TYPE_INFO,
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
    Assertions.assertEquals(
      classOf[CustomCaseClass],ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeClass())
  }

  @Test
  def testCustomType(): Unit = {
    val ti = createTypeInformation[CustomType]

    Assertions.assertFalse(ti.isBasicType)
    Assertions.assertFalse(ti.isTupleType)
    Assertions.assertTrue(ti.isInstanceOf[PojoTypeInfo[_]])
    Assertions.assertEquals(ti.getTypeClass, classOf[CustomType])
  }

  @Test
  def testTupleWithCustomType(): Unit = {
    val ti = createTypeInformation[(Long, CustomType)]

    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(2, ti.getArity)
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(classOf[Tuple2[_, _]], tti.getTypeClass)
    Assertions.assertEquals(classOf[java.lang.Long], tti.getTypeAt(0).getTypeClass)
    Assertions.assertTrue(tti.getTypeAt(1).isInstanceOf[PojoTypeInfo[_]])
    Assertions.assertEquals(classOf[CustomType], tti.getTypeAt(1).getTypeClass)
  }

  @Test
  def testValue(): Unit = {
    val ti = createTypeInformation[StringValue]

    Assertions.assertFalse(ti.isBasicType)
    Assertions.assertFalse(ti.isTupleType)
    Assertions.assertTrue(ti.isInstanceOf[ValueTypeInfo[_]])
    Assertions.assertEquals(ti.getTypeClass, classOf[StringValue])
    Assertions.assertTrue(TypeExtractor.getForClass(classOf[StringValue])
      .isInstanceOf[ValueTypeInfo[_]])
    Assertions.assertEquals(TypeExtractor.getForClass(classOf[StringValue]).getTypeClass,
      ti.getTypeClass)
  }

  @Test
  def testTupleOfValues(): Unit = {
    val ti = createTypeInformation[(StringValue, IntValue)]
    Assertions.assertFalse(ti.isBasicType)
    Assertions.assertTrue(ti.isTupleType)
    Assertions.assertEquals(
      classOf[StringValue],
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0).getTypeClass)
    Assertions.assertEquals(
      classOf[IntValue],
      ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1).getTypeClass)
  }


  @Test
  def testBasicArray(): Unit = {
    val ti = createTypeInformation[Array[String]]

    Assertions.assertFalse(ti.isBasicType)
    Assertions.assertFalse(ti.isTupleType)
    Assertions.assertTrue(ti.isInstanceOf[BasicArrayTypeInfo[_, _]] ||
      ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    if (ti.isInstanceOf[BasicArrayTypeInfo[_, _]]) {
      Assertions.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, ti)
    }
    else {
      Assertions.assertEquals(
        BasicTypeInfo.STRING_TYPE_INFO,
        ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo)
    }
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val ti = createTypeInformation[Array[Boolean]]

    Assertions.assertTrue(ti.isInstanceOf[PrimitiveArrayTypeInfo[_]])
    Assertions.assertEquals(ti, PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)
  }

  @Test
  def testCustomArray(): Unit = {
    val ti = createTypeInformation[Array[CustomType]]
    Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    Assertions.assertEquals(
      classOf[CustomType],
      ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo.getTypeClass)
  }

  @Test
  def testTupleArray(): Unit = {
    val ti = createTypeInformation[Array[(String, String)]]

    Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
    val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
    Assertions.assertTrue(oati.getComponentInfo.isTupleType)
    val tti = oati.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
  }
  
  @Test
  def testMultidimensionalArrays(): Unit = {
    // Tuple
    {
      val ti = createTypeInformation[Array[Array[(String, String)]]]
    
      Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assertions.assertTrue(oati.getComponentInfo.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assertions.assertTrue(oati2.getComponentInfo.isTupleType)
      val tti = oati2.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
      Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(0))
      Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tti.getTypeAt(1))
    }
    
    // primitives
    {
      val ti = createTypeInformation[Array[Array[Int]]]
    
      Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assertions.assertEquals(oati.getComponentInfo,
        PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)
    }
    
    // basic types
    {
      val ti = createTypeInformation[Array[Array[Integer]]]
    
      Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assertions.assertEquals(oati.getComponentInfo, BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO)
    }
    
    // pojo
    {
      val ti = createTypeInformation[Array[Array[CustomType]]]
    
      Assertions.assertTrue(ti.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      Assertions.assertTrue(oati.getComponentInfo.isInstanceOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      val tti = oati2.getComponentInfo.asInstanceOf[PojoTypeInfo[_]]
      Assertions.assertEquals(classOf[CustomType], tti.getTypeClass())
    }
  }

  @Test
  def testParamertizedCustomObject(): Unit = {
    val ti = createTypeInformation[MyObject[String]]

    Assertions.assertTrue(ti.isInstanceOf[PojoTypeInfo[_]])
  }

  @Test
  def testTupleWithPrimitiveArray(): Unit = {
    val ti = createTypeInformation[(Array[Int], Array[Double], Array[Long],
      Array[Byte], Array[Char], Array[Float], Array[Short], Array[Boolean],
      Array[String])]

    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    Assertions.assertEquals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(0))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(1))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(2))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(3))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(4))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(5))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(6))
    Assertions.assertEquals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO, tti.getTypeAt(7))
    Assertions.assertEquals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO, tti.getTypeAt(8))
  }

  @Test
  def testTrait(): Unit = {
    trait TestTrait {
      def foo() = 1
      def bar(x: Int): Int
    }

    val ti = createTypeInformation[TestTrait]

    Assertions.assertTrue(ti.isInstanceOf[GenericTypeInfo[TestTrait]])
  }

  @Test
  def testGetFlatFields(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    Assertions.assertEquals(0, tupleTypeInfo.getFlatFields("0").get(0).getPosition)
    Assertions.assertEquals(1, tupleTypeInfo.getFlatFields("1").get(0).getPosition)
    Assertions.assertEquals(2, tupleTypeInfo.getFlatFields("2").get(0).getPosition)
    Assertions.assertEquals(3, tupleTypeInfo.getFlatFields("3").get(0).getPosition)
    Assertions.assertEquals(0, tupleTypeInfo.getFlatFields("_1").get(0).getPosition)
    Assertions.assertEquals(1, tupleTypeInfo.getFlatFields("_2").get(0).getPosition)
    Assertions.assertEquals(2, tupleTypeInfo.getFlatFields("_3").get(0).getPosition)
    Assertions.assertEquals(3, tupleTypeInfo.getFlatFields("_4").get(0).getPosition)

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    Assertions.assertEquals(0, nestedTypeInfo.getFlatFields("0").get(0).getPosition)
    Assertions.assertEquals(1, nestedTypeInfo.getFlatFields("1.0").get(0).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("1.1").get(0).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("1.2").get(0).getPosition)
    Assertions.assertEquals(4, nestedTypeInfo.getFlatFields("2").get(0).getPosition)
    Assertions.assertEquals(5, nestedTypeInfo.getFlatFields("3.0").get(0).getPosition)
    Assertions.assertEquals(6, nestedTypeInfo.getFlatFields("3.1").get(0).getPosition)
    Assertions.assertEquals(4, nestedTypeInfo.getFlatFields("_3").get(0).getPosition)
    Assertions.assertEquals(5, nestedTypeInfo.getFlatFields("_4._1").get(0).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("1").size)
    Assertions.assertEquals(1, nestedTypeInfo.getFlatFields("1").get(0).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("1").get(1).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("1").get(2).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("1.*").size)
    Assertions.assertEquals(1, nestedTypeInfo.getFlatFields("1.*").get(0).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("1.*").get(1).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("1.*").get(2).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("3").size)
    Assertions.assertEquals(5, nestedTypeInfo.getFlatFields("3").get(0).getPosition)
    Assertions.assertEquals(6, nestedTypeInfo.getFlatFields("3").get(1).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("_2").size)
    Assertions.assertEquals(1, nestedTypeInfo.getFlatFields("_2").get(0).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("_2").get(1).getPosition)
    Assertions.assertEquals(3, nestedTypeInfo.getFlatFields("_2").get(2).getPosition)
    Assertions.assertEquals(2, nestedTypeInfo.getFlatFields("_4").size)
    Assertions.assertEquals(5, nestedTypeInfo.getFlatFields("_4").get(0).getPosition)
    Assertions.assertEquals(6, nestedTypeInfo.getFlatFields("_4").get(1).getPosition)
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO,
      nestedTypeInfo.getFlatFields("0").get(0).getType)
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO,
      nestedTypeInfo.getFlatFields("1.1").get(0).getType)
    Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO,
      nestedTypeInfo.getFlatFields("1").get(2).getType)
    Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO,
      nestedTypeInfo.getFlatFields("3").get(1).getType)

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    Assertions.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("1").size)
    Assertions.assertEquals(1, deepNestedTupleTypeInfo.getFlatFields("1").get(0).getPosition)
    Assertions.assertEquals(2, deepNestedTupleTypeInfo.getFlatFields("1").get(1).getPosition)
    Assertions.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("1").get(2).getPosition)
    Assertions.assertEquals(5, deepNestedTupleTypeInfo.getFlatFields("*").size)
    Assertions.assertEquals(0, deepNestedTupleTypeInfo.getFlatFields("*").get(0).getPosition)
    Assertions.assertEquals(1, deepNestedTupleTypeInfo.getFlatFields("*").get(1).getPosition)
    Assertions.assertEquals(2, deepNestedTupleTypeInfo.getFlatFields("*").get(2).getPosition)
    Assertions.assertEquals(3, deepNestedTupleTypeInfo.getFlatFields("*").get(3).getPosition)
    Assertions.assertEquals(4, deepNestedTupleTypeInfo.getFlatFields("*").get(4).getPosition)

    val caseClassTypeInfo = createTypeInformation[CustomCaseClass].
      asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    Assertions.assertEquals(0, caseClassTypeInfo.getFlatFields("a").get(0).getPosition)
    Assertions.assertEquals(1, caseClassTypeInfo.getFlatFields("b").get(0).getPosition)
    Assertions.assertEquals(2, caseClassTypeInfo.getFlatFields("*").size)
    Assertions.assertEquals(0, caseClassTypeInfo.getFlatFields("*").get(0).getPosition)
    Assertions.assertEquals(1, caseClassTypeInfo.getFlatFields("*").get(1).getPosition)

    val caseClassInTupleTypeInfo = createTypeInformation[(Int, UmlautCaseClass)].
      asInstanceOf[CaseClassTypeInfo[(Int, UmlautCaseClass)]]
    Assertions.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("_2.ä").get(0).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1.ß").get(0).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1").size)
    Assertions.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("1.*").get(0).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("1").get(1).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("_2.*").size)
    Assertions.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("_2.*").get(0).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("_2").get(1).getPosition)
    Assertions.assertEquals(3, caseClassInTupleTypeInfo.getFlatFields("*").size)
    Assertions.assertEquals(0, caseClassInTupleTypeInfo.getFlatFields("*").get(0).getPosition)
    Assertions.assertEquals(1, caseClassInTupleTypeInfo.getFlatFields("*").get(1).getPosition)
    Assertions.assertEquals(2, caseClassInTupleTypeInfo.getFlatFields("*").get(2).getPosition)

  }

  @Test
  def testFieldAtStringRef(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("0"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("2"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("_2"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleTypeInfo.getTypeAt("_4"))

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("0"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("1.0"))
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, nestedTypeInfo.getTypeAt("1.1"))
    Assertions.assertEquals(BasicTypeInfo.LONG_TYPE_INFO, nestedTypeInfo.getTypeAt("1.2"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("2"))
    Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("3.0"))
    Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("3.1"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, nestedTypeInfo.getTypeAt("_3"))
    Assertions.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, nestedTypeInfo.getTypeAt("_4._1"))
    Assertions.assertEquals(createTypeInformation[(Int, String, Long)], nestedTypeInfo.getTypeAt("1"))
    Assertions.assertEquals(createTypeInformation[(Double, Double)], nestedTypeInfo.getTypeAt("3"))
    Assertions.assertEquals(createTypeInformation[(Int, String, Long)], nestedTypeInfo.getTypeAt("_2"))
    Assertions.assertEquals(createTypeInformation[(Double, Double)], nestedTypeInfo.getTypeAt("_4"))

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)].
      asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    Assertions.assertEquals(createTypeInformation[(Int, (Int, Int))],
      deepNestedTupleTypeInfo.getTypeAt("1"))

    val umlautCaseClassTypeInfo = createTypeInformation[UmlautCaseClass].
      asInstanceOf[CaseClassTypeInfo[UmlautCaseClass]]
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, umlautCaseClassTypeInfo.getTypeAt("ä"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, umlautCaseClassTypeInfo.getTypeAt("ß"))

    val caseClassTypeInfo = createTypeInformation[CustomCaseClass].
      asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    val caseClassInTupleTypeInfo = createTypeInformation[(Int, CustomCaseClass)].
      asInstanceOf[CaseClassTypeInfo[(Int, CustomCaseClass)]]
    Assertions.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, caseClassInTupleTypeInfo.getTypeAt("_2.a"))
    Assertions.assertEquals(BasicTypeInfo.INT_TYPE_INFO, caseClassInTupleTypeInfo.getTypeAt("1.b"))
    Assertions.assertEquals(caseClassTypeInfo, caseClassInTupleTypeInfo.getTypeAt("1"))
    Assertions.assertEquals(caseClassTypeInfo, caseClassInTupleTypeInfo.getTypeAt("_2"))

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
    Assertions.assertTrue(ti.isInstanceOf[UnitTypeInfo])

    // This checks the condition in checkCollection. If this fails with IllegalArgumentException,
    // then things like "env.fromElements((),(),())" won't work.
    import scala.collection.JavaConversions._
    CollectionInputFormat.checkCollection(Seq((),(),()), (new UnitTypeInfo).getTypeClass())
  }

  @Test
  def testNestedTraversableWithTypeParametersReplacesTypeParametersInCanBuildFrom(): Unit = {

    def createTraversableTypeInfo[T: TypeInformation] = createTypeInformation[Seq[Seq[T]]]

    val traversableTypeInfo = createTraversableTypeInfo[Int]
    val outerTraversableSerializer = traversableTypeInfo.createSerializer(new ExecutionConfig)
      .asInstanceOf[TraversableSerializer[Seq[Seq[Int]], Seq[Int]]]

    // make sure that we still create the correct inner element serializer, despite the generics
    val innerTraversableSerializer = outerTraversableSerializer.elementSerializer
      .asInstanceOf[TraversableSerializer[Seq[Int], Int]]
    Assertions.assertEquals(
      classOf[IntSerializer],
      innerTraversableSerializer.elementSerializer.getClass)

    // if the code in here had Ts it would not compile. This would already fail above when
    // creating the serializer. This is just to verify.
    Assertions.assertEquals(
      "implicitly[scala.collection.generic.CanBuildFrom[" +
        "Seq[Seq[Object]], Seq[Object], Seq[Seq[Object]]]" +
        "]",
      outerTraversableSerializer.cbfCode)
  }

  @Test
  def testNestedTraversableWithSpecificTypesDoesNotReplaceTypeParametersInCanBuildFrom(): Unit = {

    val traversableTypeInfo = createTypeInformation[Seq[Seq[Int]]]
    val outerTraversableSerializer = traversableTypeInfo.createSerializer(new ExecutionConfig)
      .asInstanceOf[TraversableSerializer[Seq[Seq[Int]], Seq[Int]]]

    val innerTraversableSerializer = outerTraversableSerializer.elementSerializer
      .asInstanceOf[TraversableSerializer[Seq[Int], Int]]
    Assertions.assertEquals(
      classOf[IntSerializer],
      innerTraversableSerializer.elementSerializer.getClass)

    Assertions.assertEquals(
      "implicitly[scala.collection.generic.CanBuildFrom[" +
        "Seq[Seq[Int]], Seq[Int], Seq[Seq[Int]]]" +
        "]",
      outerTraversableSerializer.cbfCode)
  }
}

