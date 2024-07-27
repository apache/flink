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
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.java.typeutils.TypeExtractorTest.CustomTuple
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, TraversableSerializer, UnitTypeInfo}
import org.apache.flink.types.{IntValue, StringValue}

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

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

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(3)
    assertThat(ti).isInstanceOf(classOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeClass).isEqualTo(classOf[org.apache.flink.api.java.tuple.Tuple3[_, _, _]])
    for (i <- 0 until 3) {
      assertThat(tti.getTypeAt(i)).isInstanceOf(classOf[BasicTypeInfo[_]])
    }

    assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(tti.getTypeAt(2)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
  }

  @Test
  def testCustomJavaTuple(): Unit = {
    val ti = createTypeInformation[CustomTuple]

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(2)
    assertThat(ti).isInstanceOf(classOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeClass).isEqualTo(classOf[CustomTuple])
    for (i <- 0 until 2) {
      assertThat(tti.getTypeAt(i)).isInstanceOf(classOf[BasicTypeInfo[_]])
    }

    assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
  }

  @Test
  def testBasicType(): Unit = {
    val ti = createTypeInformation[Boolean]

    assertThat(ti.isBasicType).isTrue
    assertThat(ti).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO)
    assertThat(ti.getTypeClass).isEqualTo(classOf[java.lang.Boolean])
  }

  @Test
  def testTypeParameters(): Unit = {

    val data = Seq(1.0d, 2.0d)

    def f[T: TypeInformation](data: Seq[T]): (T, Seq[T]) = {

      val ti = createTypeInformation[(T, Seq[T])]

      assertThat(ti.isTupleType).isTrue
      val ccti = ti.asInstanceOf[CaseClassTypeInfo[(T, Seq[T])]]
      assertThat(ccti.getTypeAt(0)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)

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
    val charArray = Array(1.toChar, 2.toChar, 3.toChar)
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

    assertThat(getType(boolArray)).isEqualTo(
      PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO
    )

    assertThat(getType(byteArray)).isEqualTo(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(charArray)).isEqualTo(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(shortArray)).isEqualTo(
      PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(intArray)).isEqualTo(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(longArray)).isEqualTo(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(floatArray)).isEqualTo(
      PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(doubleArray)).isEqualTo(
      PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)

    assertThat(getType(stringArray)).isEqualTo(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)

    assertThat(getType(objectArray)).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
    assertThat(
      getType(objectArray)
        .asInstanceOf[ObjectArrayTypeInfo[_, _]]
        .getComponentInfo
        .isInstanceOf[PojoTypeInfo[_]]).isTrue
  }

  @Test
  def testTupleWithBasicTypes(): Unit = {
    val ti = createTypeInformation[(Int, Long, Double, Float, Boolean, String, Char, Short, Byte)]

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(9)
    assertThat(ti).isInstanceOf(classOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeClass).isEqualTo(classOf[(_, _, _, _, _, _, _, _, _)])
    for (i <- 0 until 9) {
      assertThat(tti.getTypeAt(i)).isInstanceOf(classOf[BasicTypeInfo[_]])
    }

    assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO)
    assertThat(tti.getTypeAt(2)).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)
    assertThat(tti.getTypeAt(3)).isEqualTo(BasicTypeInfo.FLOAT_TYPE_INFO)
    assertThat(tti.getTypeAt(4)).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO)
    assertThat(tti.getTypeAt(5)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(tti.getTypeAt(6)).isEqualTo(BasicTypeInfo.CHAR_TYPE_INFO)
    assertThat(tti.getTypeAt(7)).isEqualTo(BasicTypeInfo.SHORT_TYPE_INFO)
    assertThat(tti.getTypeAt(8)).isEqualTo(BasicTypeInfo.BYTE_TYPE_INFO)
  }

  @Test
  def testTupleWithTuples(): Unit = {
    val ti = createTypeInformation[(Tuple1[String], Tuple1[Int], Tuple2[Long, Long])]

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(3)
    assertThat(ti).isInstanceOf(classOf[TupleTypeInfoBase[_]])
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeClass).isEqualTo(classOf[(_, _, _)])
    assertThat(tti.getTypeAt(0).isTupleType).isTrue
    assertThat(tti.getTypeAt(1).isTupleType).isTrue
    assertThat(tti.getTypeAt(2).isTupleType).isTrue
    assertThat(tti.getTypeAt(0).getTypeClass).isEqualTo(classOf[Tuple1[_]])
    assertThat(tti.getTypeAt(1).getTypeClass).isEqualTo(classOf[Tuple1[_]])
    assertThat(tti.getTypeAt(2).getTypeClass).isEqualTo(classOf[(_, _)])
    assertThat(tti.getTypeAt(0).getArity).isEqualTo(1)
    assertThat(tti.getTypeAt(1).getArity).isEqualTo(1)
    assertThat(tti.getTypeAt(2).getArity).isEqualTo(2)
    assertThat(tti.getTypeAt(0).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
      .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(tti.getTypeAt(1).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
      .isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
      .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO)
    assertThat(tti.getTypeAt(2).asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
      .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO)
  }

  @Test
  def testCaseClass(): Unit = {
    val ti = createTypeInformation[CustomCaseClass]

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(2)
    assertThat(ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0))
      .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1))
      .isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeClass)
      .isEqualTo(classOf[CustomCaseClass])
  }

  @Test
  def testCustomType(): Unit = {
    val ti = createTypeInformation[CustomType]

    assertThat(ti.isBasicType).isFalse
    assertThat(ti.isTupleType).isFalse
    assertThat(ti).isInstanceOf(classOf[PojoTypeInfo[_]])
    assertThat(ti.getTypeClass).isEqualTo(classOf[CustomType])
  }

  @Test
  def testTupleWithCustomType(): Unit = {
    val ti = createTypeInformation[(Long, CustomType)]

    assertThat(ti.isTupleType).isTrue
    assertThat(ti.getArity).isEqualTo(2)
    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeClass).isEqualTo(classOf[(_, _)])
    assertThat(tti.getTypeAt(0).getTypeClass).isEqualTo(classOf[java.lang.Long])
    assertThat(tti.getTypeAt(1)).isInstanceOf(classOf[PojoTypeInfo[_]])
    assertThat(tti.getTypeAt(1).getTypeClass).isEqualTo(classOf[CustomType])
  }

  @Test
  def testValue(): Unit = {
    val ti = createTypeInformation[StringValue]

    assertThat(ti.isBasicType).isFalse
    assertThat(ti.isTupleType).isFalse
    assertThat(ti).isInstanceOf(classOf[ValueTypeInfo[_]])
    assertThat(ti.getTypeClass).isEqualTo(classOf[StringValue])
    assertThat(
      TypeExtractor
        .getForClass(classOf[StringValue]))
      .isInstanceOf(classOf[ValueTypeInfo[_]])
    assertThat(TypeExtractor.getForClass(classOf[StringValue]).getTypeClass)
      .isEqualTo(ti.getTypeClass)
  }

  @Test
  def testTupleOfValues(): Unit = {
    val ti = createTypeInformation[(StringValue, IntValue)]
    assertThat(ti.isBasicType).isFalse
    assertThat(ti.isTupleType).isTrue
    assertThat(ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(0).getTypeClass)
      .isEqualTo(classOf[StringValue])
    assertThat(ti.asInstanceOf[TupleTypeInfoBase[_]].getTypeAt(1).getTypeClass)
      .isEqualTo(classOf[IntValue])
  }

  @Test
  def testBasicArray(): Unit = {
    val ti = createTypeInformation[Array[String]]

    assertThat(ti.isBasicType).isFalse
    assertThat(ti.isTupleType).isFalse
    assertThat(ti).isInstanceOfAny(
      classOf[BasicArrayTypeInfo[_, _]],
      classOf[ObjectArrayTypeInfo[_, _]])
    if (ti.isInstanceOf[BasicArrayTypeInfo[_, _]]) {
      assertThat(ti).isEqualTo(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)
    } else {
      assertThat(ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo)
        .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    }
  }

  @Test
  def testPrimitiveArray(): Unit = {
    val ti = createTypeInformation[Array[Boolean]]

    assertThat(ti).isInstanceOf(classOf[PrimitiveArrayTypeInfo[_]])
    assertThat(ti).isEqualTo(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)
  }

  @Test
  def testCustomArray(): Unit = {
    val ti = createTypeInformation[Array[CustomType]]
    assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
    assertThat(ti.asInstanceOf[ObjectArrayTypeInfo[_, _]].getComponentInfo.getTypeClass)
      .isEqualTo(classOf[CustomType])
  }

  @Test
  def testTupleArray(): Unit = {
    val ti = createTypeInformation[Array[(String, String)]]

    assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
    val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
    assertThat(oati.getComponentInfo.isTupleType).isTrue
    val tti = oati.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
  }

  @Test
  def testMultidimensionalArrays(): Unit = {
    // Tuple
    {
      val ti = createTypeInformation[Array[Array[(String, String)]]]

      assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      assertThat(oati.getComponentInfo).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      assertThat(oati2.getComponentInfo.isTupleType).isTrue
      val tti = oati2.getComponentInfo.asInstanceOf[TupleTypeInfoBase[_]]
      assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
      assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    }

    // primitives
    {
      val ti = createTypeInformation[Array[Array[Int]]]

      assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      assertThat(oati.getComponentInfo).isEqualTo(
        PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)
    }

    // basic types
    {
      val ti = createTypeInformation[Array[Array[Integer]]]

      assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      assertThat(oati.getComponentInfo).isEqualTo(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO)
    }

    // pojo
    {
      val ti = createTypeInformation[Array[Array[CustomType]]]

      assertThat(ti).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati = ti.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      assertThat(oati.getComponentInfo).isInstanceOf(classOf[ObjectArrayTypeInfo[_, _]])
      val oati2 = oati.getComponentInfo.asInstanceOf[ObjectArrayTypeInfo[_, _]]
      val tti = oati2.getComponentInfo.asInstanceOf[PojoTypeInfo[_]]
      assertThat(tti.getTypeClass).isEqualTo(classOf[CustomType])
    }
  }

  @Test
  def testParamertizedCustomObject(): Unit = {
    val ti = createTypeInformation[MyObject[String]]

    assertThat(ti).isInstanceOf(classOf[PojoTypeInfo[_]])
  }

  @Test
  def testTupleWithPrimitiveArray(): Unit = {
    val ti = createTypeInformation[(
        Array[Int],
        Array[Double],
        Array[Long],
        Array[Byte],
        Array[Char],
        Array[Float],
        Array[Short],
        Array[Boolean],
        Array[String])]

    val tti = ti.asInstanceOf[TupleTypeInfoBase[_]]
    assertThat(tti.getTypeAt(0)).isEqualTo(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(1)).isEqualTo(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(2)).isEqualTo(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(3)).isEqualTo(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(4)).isEqualTo(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(5)).isEqualTo(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(6)).isEqualTo(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(7)).isEqualTo(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)
    assertThat(tti.getTypeAt(8)).isEqualTo(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)
  }

  @Test
  def testTrait(): Unit = {
    trait TestTrait {
      def foo() = 1
      def bar(x: Int): Int
    }

    val ti = createTypeInformation[TestTrait]

    assertThat(ti).isInstanceOf(classOf[GenericTypeInfo[TestTrait]])
  }

  @Test
  def testGetFlatFields(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)]
      .asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    assertThat(tupleTypeInfo.getFlatFields("0").get(0).getPosition).isEqualTo(0)
    assertThat(tupleTypeInfo.getFlatFields("1").get(0).getPosition).isEqualTo(1)
    assertThat(tupleTypeInfo.getFlatFields("2").get(0).getPosition).isEqualTo(2)
    assertThat(tupleTypeInfo.getFlatFields("3").get(0).getPosition).isEqualTo(3)
    assertThat(tupleTypeInfo.getFlatFields("_1").get(0).getPosition).isEqualTo(0)
    assertThat(tupleTypeInfo.getFlatFields("_2").get(0).getPosition).isEqualTo(1)
    assertThat(tupleTypeInfo.getFlatFields("_3").get(0).getPosition).isEqualTo(2)
    assertThat(tupleTypeInfo.getFlatFields("_4").get(0).getPosition).isEqualTo(3)

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))]
      .asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    assertThat(nestedTypeInfo.getFlatFields("0").get(0).getPosition).isEqualTo(0)
    assertThat(nestedTypeInfo.getFlatFields("1.0").get(0).getPosition).isEqualTo(1)
    assertThat(nestedTypeInfo.getFlatFields("1.1").get(0).getPosition).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("1.2").get(0).getPosition).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("2").get(0).getPosition).isEqualTo(4)
    assertThat(nestedTypeInfo.getFlatFields("3.0").get(0).getPosition).isEqualTo(5)
    assertThat(nestedTypeInfo.getFlatFields("3.1").get(0).getPosition).isEqualTo(6)
    assertThat(nestedTypeInfo.getFlatFields("_3").get(0).getPosition).isEqualTo(4)
    assertThat(nestedTypeInfo.getFlatFields("_4._1").get(0).getPosition).isEqualTo(5)
    assertThat(nestedTypeInfo.getFlatFields("1").size()).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("1").get(0).getPosition).isEqualTo(1)
    assertThat(nestedTypeInfo.getFlatFields("1").get(1).getPosition).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("1").get(2).getPosition).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("1.*").size()).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("1.*").get(0).getPosition).isEqualTo(1)
    assertThat(nestedTypeInfo.getFlatFields("1.*").get(1).getPosition).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("1.*").get(2).getPosition).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("3").size()).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("3").get(0).getPosition).isEqualTo(5)
    assertThat(nestedTypeInfo.getFlatFields("3").get(1).getPosition).isEqualTo(6)
    assertThat(nestedTypeInfo.getFlatFields("_2").size()).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("_2").get(0).getPosition).isEqualTo(1)
    assertThat(nestedTypeInfo.getFlatFields("_2").get(1).getPosition).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("_2").get(2).getPosition).isEqualTo(3)
    assertThat(nestedTypeInfo.getFlatFields("_4").size()).isEqualTo(2)
    assertThat(nestedTypeInfo.getFlatFields("_4").get(0).getPosition).isEqualTo(5)
    assertThat(nestedTypeInfo.getFlatFields("_4").get(1).getPosition).isEqualTo(6)
    assertThat(nestedTypeInfo.getFlatFields("0").get(0).getType)
      .isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(nestedTypeInfo.getFlatFields("1.1").get(0).getType)
      .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(nestedTypeInfo.getFlatFields("1").get(2).getType)
      .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO)
    assertThat(nestedTypeInfo.getFlatFields("3").get(1).getType)
      .isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)]
      .asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    assertThat(deepNestedTupleTypeInfo.getFlatFields("1").size()).isEqualTo(3)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(0).getPosition).isEqualTo(1)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(1).getPosition).isEqualTo(2)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("1").get(2).getPosition).isEqualTo(3)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").size()).isEqualTo(5)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(0).getPosition).isEqualTo(0)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(1).getPosition).isEqualTo(1)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(2).getPosition).isEqualTo(2)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(3).getPosition).isEqualTo(3)
    assertThat(deepNestedTupleTypeInfo.getFlatFields("*").get(4).getPosition).isEqualTo(4)

    val caseClassTypeInfo =
      createTypeInformation[CustomCaseClass].asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    assertThat(caseClassTypeInfo.getFlatFields("a").get(0).getPosition).isEqualTo(0)
    assertThat(caseClassTypeInfo.getFlatFields("b").get(0).getPosition).isEqualTo(1)
    assertThat(caseClassTypeInfo.getFlatFields("*").size()).isEqualTo(2)
    assertThat(caseClassTypeInfo.getFlatFields("*").get(0).getPosition).isEqualTo(0)
    assertThat(caseClassTypeInfo.getFlatFields("*").get(1).getPosition).isEqualTo(1)

    val caseClassInTupleTypeInfo = createTypeInformation[(Int, UmlautCaseClass)]
      .asInstanceOf[CaseClassTypeInfo[(Int, UmlautCaseClass)]]
    assertThat(caseClassInTupleTypeInfo.getFlatFields("_2.ä").get(0).getPosition).isEqualTo(1)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("1.ß").get(0).getPosition).isEqualTo(2)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("1").size()).isEqualTo(2)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("1.*").get(0).getPosition).isEqualTo(1)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("1").get(1).getPosition).isEqualTo(2)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("_2.*").size()).isEqualTo(2)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("_2.*").get(0).getPosition).isEqualTo(1)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("_2").get(1).getPosition).isEqualTo(2)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("*").size()).isEqualTo(3)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("*").get(0).getPosition).isEqualTo(0)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("*").get(1).getPosition).isEqualTo(1)
    assertThat(caseClassInTupleTypeInfo.getFlatFields("*").get(2).getPosition).isEqualTo(2)

  }

  @Test
  def testFieldAtStringRef(): Unit = {

    val tupleTypeInfo = createTypeInformation[(Int, Int, Int, Int)]
      .asInstanceOf[CaseClassTypeInfo[(Int, Int, Int, Int)]]
    assertThat(tupleTypeInfo.getTypeAt("0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tupleTypeInfo.getTypeAt("2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tupleTypeInfo.getTypeAt("_2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(tupleTypeInfo.getTypeAt("_4")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)

    val nestedTypeInfo = createTypeInformation[(Int, (Int, String, Long), Int, (Double, Double))]
      .asInstanceOf[CaseClassTypeInfo[(Int, (Int, String, Long), Int, (Double, Double))]]
    assertThat(nestedTypeInfo.getTypeAt("0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("1.0")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("1.1")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("1.2")).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("2")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("3.0")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("3.1")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("_3")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("_4._1")).isEqualTo(BasicTypeInfo.DOUBLE_TYPE_INFO)
    assertThat(nestedTypeInfo.getTypeAt("1")).isEqualTo(createTypeInformation[(Int, String, Long)])
    assertThat(nestedTypeInfo.getTypeAt("3")).isEqualTo(createTypeInformation[(Double, Double)])
    assertThat(nestedTypeInfo.getTypeAt("_2")).isEqualTo(createTypeInformation[(Int, String, Long)])
    assertThat(nestedTypeInfo.getTypeAt("_4")).isEqualTo(createTypeInformation[(Double, Double)])

    val deepNestedTupleTypeInfo = createTypeInformation[(Int, (Int, (Int, Int)), Int)]
      .asInstanceOf[CaseClassTypeInfo[(Int, (Int, (Int, Int)), Int)]]
    assertThat(deepNestedTupleTypeInfo.getTypeAt("1"))
      .isEqualTo(createTypeInformation[(Int, (Int, Int))])

    val umlautCaseClassTypeInfo =
      createTypeInformation[UmlautCaseClass].asInstanceOf[CaseClassTypeInfo[UmlautCaseClass]]
    assertThat(umlautCaseClassTypeInfo.getTypeAt("ä")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(umlautCaseClassTypeInfo.getTypeAt("ß")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)

    val caseClassTypeInfo =
      createTypeInformation[CustomCaseClass].asInstanceOf[CaseClassTypeInfo[CustomCaseClass]]
    val caseClassInTupleTypeInfo = createTypeInformation[(Int, CustomCaseClass)]
      .asInstanceOf[CaseClassTypeInfo[(Int, CustomCaseClass)]]
    assertThat(caseClassInTupleTypeInfo.getTypeAt("_2.a")).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO)
    assertThat(caseClassInTupleTypeInfo.getTypeAt("1.b")).isEqualTo(BasicTypeInfo.INT_TYPE_INFO)
    assertThat(caseClassTypeInfo).isEqualTo(caseClassInTupleTypeInfo.getTypeAt("1"))
    assertThat(caseClassTypeInfo).isEqualTo(caseClassInTupleTypeInfo.getTypeAt("_2"))

  }

  /**
   * Tests the "implicit val scalaNothingTypeInfo" in
   * flink-scala/src/main/scala/org/apache/flink/api/scala/package.scala This does not compile
   * without that line.
   */
  @Test
  def testNothingTypeInfoIsAvailableImplicitly(): Unit = {
    def g() = {

      def f[O: TypeInformation](x: O): Unit = {}

      f(???) // O will be Nothing
    }
    // (Do not call g, because it throws NotImplementedError. This is a compile time test.)
  }

  @Test
  def testUnit(): Unit = {
    val ti = createTypeInformation[Unit]
    assertThat(ti).isInstanceOf(classOf[UnitTypeInfo])

    // This checks the condition in checkCollection. If this fails with IllegalArgumentException,
    // then things like "env.fromElements((),(),())" won't work.
    import scala.collection.JavaConversions._
    CollectionInputFormat.checkCollection(Seq((), (), ()), (new UnitTypeInfo).getTypeClass())
  }

  @Test
  def testNestedTraversableWithTypeParametersReplacesTypeParametersInCanBuildFrom(): Unit = {

    def createTraversableTypeInfo[T: TypeInformation] = createTypeInformation[Seq[Seq[T]]]

    val traversableTypeInfo = createTraversableTypeInfo[Int]
    val outerTraversableSerializer = traversableTypeInfo
      .createSerializer(new SerializerConfigImpl)
      .asInstanceOf[TraversableSerializer[Seq[Seq[Int]], Seq[Int]]]

    // make sure that we still create the correct inner element serializer, despite the generics
    val innerTraversableSerializer = outerTraversableSerializer.elementSerializer
      .asInstanceOf[TraversableSerializer[Seq[Int], Int]]
    assertThat(innerTraversableSerializer.elementSerializer.getClass)
      .isEqualTo(classOf[IntSerializer])

    // if the code in here had Ts it would not compile. This would already fail above when
    // creating the serializer. This is just to verify.
    assertThat(outerTraversableSerializer.cbfCode)
      .isEqualTo(
        "implicitly[scala.collection.generic.CanBuildFrom[" +
          "Seq[Seq[Object]], Seq[Object], Seq[Seq[Object]]]" +
          "]"
      )
  }

  @Test
  def testNestedTraversableWithSpecificTypesDoesNotReplaceTypeParametersInCanBuildFrom(): Unit = {

    val traversableTypeInfo = createTypeInformation[Seq[Seq[Int]]]
    val outerTraversableSerializer = traversableTypeInfo
      .createSerializer(new SerializerConfigImpl)
      .asInstanceOf[TraversableSerializer[Seq[Seq[Int]], Seq[Int]]]

    val innerTraversableSerializer = outerTraversableSerializer.elementSerializer
      .asInstanceOf[TraversableSerializer[Seq[Int], Int]]
    assertThat(innerTraversableSerializer.elementSerializer.getClass)
      .isEqualTo(classOf[IntSerializer])

    assertThat(outerTraversableSerializer.cbfCode)
      .isEqualTo(
        "implicitly[scala.collection.generic.CanBuildFrom[" +
          "Seq[Seq[Int]], Seq[Int], Seq[Seq[Int]]]" +
          "]")
  }
}
