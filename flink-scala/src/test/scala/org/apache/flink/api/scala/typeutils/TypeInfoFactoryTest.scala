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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInfoFactory, TypeInformation}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.{EitherTypeInfo => JavaEitherTypeInfo}
import org.apache.flink.api.java.typeutils.TypeInfoFactoryTest._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.TypeInfoFactoryTest._

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.lang.reflect.Type
import java.util

class TypeInfoFactoryTest {

  @Test
  def testSimpleType(): Unit = {
    val ti = createTypeInformation[ScalaIntLike]
    assertThat(ti).isEqualTo(INT_TYPE_INFO)
  }

  @Test
  def testMyTuple(): Unit = {
    val ti = createTypeInformation[MyTuple[Double, String]]
    assertThat(ti).isInstanceOf(classOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertThat(mtti.getField0).isEqualTo(DOUBLE_TYPE_INFO)
    assertThat(mtti.getField1).isEqualTo(STRING_TYPE_INFO)
  }

  @Test
  def testMyTupleHierarchy(): Unit = {
    val ti = createTypeInformation[MyTuple2]
    assertThat(ti).isInstanceOf(classOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertThat(mtti.getField0).isEqualTo(STRING_TYPE_INFO)
    assertThat(mtti.getField1).isEqualTo(BOOLEAN_TYPE_INFO)

    val ti2 = createTypeInformation[MyScalaTupleClass]
    assertThat(ti2).isInstanceOf(classOf[MyTupleTypeInfo[_, _]])
    val mtti2 = ti2.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertThat(mtti2.getField0).isEqualTo(STRING_TYPE_INFO)
    assertThat(mtti2.getField1).isEqualTo(BOOLEAN_TYPE_INFO)
  }

  @Test
  def testMyTupleHierarchyWithCaseClass(): Unit = {
    val ti = createTypeInformation[MyScalaTupleCaseClass]
    assertThat(ti.isInstanceOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertThat(mtti.getField0).isEqualTo(DOUBLE_TYPE_INFO)
    assertThat(mtti.getField1).isEqualTo(BOOLEAN_TYPE_INFO)
  }

  @Test
  def testMyEitherGenericType(): Unit = {
    val ti = createTypeInformation[MyScalaEither[String, (Double, Int)]]
    assertThat(ti).isInstanceOf(classOf[JavaEitherTypeInfo[_, _]])
    val eti = ti.asInstanceOf[JavaEitherTypeInfo[_, _]]
    assertThat(eti.getLeftType).isEqualTo(STRING_TYPE_INFO)
    assertThat(eti.getRightType).isInstanceOf(classOf[CaseClassTypeInfo[_]])
    val cti = eti.getRightType.asInstanceOf[CaseClassTypeInfo[_]]
    assertThat(cti.getTypeAt(0)).isEqualTo(DOUBLE_TYPE_INFO)
    assertThat(cti.getTypeAt(1)).isEqualTo(INT_TYPE_INFO)
  }

  @Test
  def testScalaFactory(): Unit = {
    val ti = createTypeInformation[MyScalaOption[Double]]
    assertThat(ti).isInstanceOf(classOf[MyScalaOptionTypeInfo])
    val moti = ti.asInstanceOf[MyScalaOptionTypeInfo]
    assertThat(moti.elementType).isEqualTo(DOUBLE_TYPE_INFO)
  }
}

// --------------------------------------------------------------------------------------------
//  Utilities
// --------------------------------------------------------------------------------------------

object TypeInfoFactoryTest {

  @TypeInfo(classOf[IntLikeTypeInfoFactory])
  case class ScalaIntLike(myint: Int)

  class MyScalaTupleClass extends MyTuple2

  case class MyScalaTupleCaseClass(additional: Boolean) extends MyTuple3[Double]

  @TypeInfo(classOf[MyEitherTypeInfoFactory[_, _]])
  class MyScalaEither[A, B] {
    // do nothing here
  }

  @TypeInfo(classOf[MyScalaOptionTypeInfoFactory])
  class MyScalaOption[Z] {
    // do nothing here
  }

  class MyScalaOptionTypeInfoFactory extends TypeInfoFactory[MyOption[_]] {

    override def createTypeInfo(
        t: Type,
        genericParameters: util.Map[String, TypeInformation[_]]): TypeInformation[MyOption[_]] = {
      new MyScalaOptionTypeInfo(genericParameters.get("Z"))
    }
  }

  class MyScalaOptionTypeInfo(val elementType: TypeInformation[_])
    extends TypeInformation[MyOption[_]] {

    override def isBasicType: Boolean = ???

    override def isTupleType: Boolean = ???

    override def getArity: Int = ???

    override def getTotalFields: Int = ???

    override def getTypeClass: Class[MyOption[_]] = ???

    override def isKeyType: Boolean = ???

    override def createSerializer(config: SerializerConfig): TypeSerializer[MyOption[_]] = ???

    override def createSerializer(config: ExecutionConfig): TypeSerializer[MyOption[_]] = ???

    override def canEqual(obj: scala.Any): Boolean = ???

    override def hashCode(): Int = ???

    override def toString: String = ???

    override def equals(obj: scala.Any): Boolean = ???
  }
}
