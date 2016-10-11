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

import java.lang.reflect.Type
import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{TypeInfo, TypeInfoFactory, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.TypeInfoFactoryTest._
import org.apache.flink.api.java.typeutils.{EitherTypeInfo => JavaEitherTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.TypeInfoFactoryTest._
import org.apache.flink.util.TestLogger
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

class TypeInfoFactoryTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testSimpleType(): Unit = {
    val ti = createTypeInformation[ScalaIntLike]
    assertEquals(INT_TYPE_INFO, ti)
  }

  @Test
  def testMyTuple(): Unit = {
    val ti = createTypeInformation[MyTuple[Double, String]]
    assertTrue(ti.isInstanceOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertEquals(DOUBLE_TYPE_INFO, mtti.getField0)
    assertEquals(STRING_TYPE_INFO, mtti.getField1)
  }

  @Test
  def testMyTupleHierarchy() {
    val ti = createTypeInformation[MyTuple2]
    assertTrue(ti.isInstanceOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertEquals(STRING_TYPE_INFO, mtti.getField0)
    assertEquals(BOOLEAN_TYPE_INFO, mtti.getField1)

    val ti2 = createTypeInformation[MyScalaTupleClass]
    assertTrue(ti2.isInstanceOf[MyTupleTypeInfo[_, _]])
    val mtti2 = ti2.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertEquals(STRING_TYPE_INFO, mtti.getField0)
    assertEquals(BOOLEAN_TYPE_INFO, mtti.getField1)
  }

  @Test
  def testMyTupleHierarchyWithCaseClass(): Unit = {
    val ti = createTypeInformation[MyScalaTupleCaseClass]
    assertTrue(ti.isInstanceOf[MyTupleTypeInfo[_, _]])
    val mtti = ti.asInstanceOf[MyTupleTypeInfo[_, _]]
    assertEquals(DOUBLE_TYPE_INFO, mtti.getField0)
    assertEquals(BOOLEAN_TYPE_INFO, mtti.getField1)
  }

  @Test
  def testMyEitherGenericType(): Unit = {
    val ti = createTypeInformation[MyScalaEither[String, (Double, Int)]]
    assertTrue(ti.isInstanceOf[JavaEitherTypeInfo[_, _]])
    val eti = ti.asInstanceOf[JavaEitherTypeInfo[_, _]]
    assertEquals(STRING_TYPE_INFO, eti.getLeftType)
    assertTrue(eti.getRightType.isInstanceOf[CaseClassTypeInfo[_]])
    val cti = eti.getRightType.asInstanceOf[CaseClassTypeInfo[_]]
    assertEquals(DOUBLE_TYPE_INFO, cti.getTypeAt(0))
    assertEquals(INT_TYPE_INFO, cti.getTypeAt(1))
  }

  @Test
  def testScalaFactory(): Unit = {
    val ti = createTypeInformation[MyScalaOption[Double]]
    assertTrue(ti.isInstanceOf[MyScalaOptionTypeInfo])
    val moti = ti.asInstanceOf[MyScalaOptionTypeInfo]
    assertEquals(DOUBLE_TYPE_INFO, moti.elementType)
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
        genericParameters: util.Map[String, TypeInformation[_]])
      : TypeInformation[MyOption[_]] = {
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

    override def createSerializer(config: ExecutionConfig): TypeSerializer[MyOption[_]] = ???

    override def canEqual(obj: scala.Any): Boolean = ???

    override def hashCode(): Int = ???

    override def toString: String = ???

    override def equals(obj: scala.Any): Boolean = ???
  }
}
