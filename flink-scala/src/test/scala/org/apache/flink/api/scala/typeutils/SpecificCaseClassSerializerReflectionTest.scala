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

import org.apache.flink.api.scala.typeutils.SpecificCaseClassSerializerReflectionTest.{Generic, HigherKind, SimpleCaseClass}
import org.apache.flink.util.TestLogger

import org.junit.Assert.assertEquals
import org.junit.Test

import java.lang.invoke.MethodHandle

/**
  * Test obtaining the primary constructor of a case class
  * via reflection.
  */
class SpecificCaseClassSerializerReflectionTest extends TestLogger {

  @Test
  def usageExample(): Unit = {
    val constructor: MethodHandle = SpecificCaseClassSerializer
      .lookupConstructor(classOf[SimpleCaseClass])

    val actual = constructor.invoke(Array("hi", 1.asInstanceOf[Any]))

    assertEquals(SimpleCaseClass("hi", 1), actual)
  }

  @Test
  def genericCaseClass(): Unit = {
    val constructor: MethodHandle = SpecificCaseClassSerializer
      .lookupConstructor(classOf[Generic[_]])

    val actual = constructor.invoke(Array(1.asInstanceOf[AnyRef]))

    assertEquals(Generic[Int](1), actual)
  }

  @Test
  def caseClassWithParameterizedList(): Unit = {
    val constructor: MethodHandle = SpecificCaseClassSerializer
      .lookupConstructor(classOf[HigherKind])

    val actual = constructor.invoke(Array(List(1, 2, 3), "hey"))

    assertEquals(HigherKind(List(1, 2, 3), "hey"), actual)
  }

  @Test
  def tupleType(): Unit = {
    val constructor: MethodHandle = SpecificCaseClassSerializer
      .lookupConstructor(classOf[(String, String, Int)])

    val actual = constructor.invoke(Array("a", "b", 7))

    assertEquals(("a", "b", 7), actual)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def unsupportedInstanceClass(): Unit = {

    val outerInstance = new OuterClass

    SpecificCaseClassSerializer
      .lookupConstructor(classOf[outerInstance.InnerCaseClass])
  }
}

object SpecificCaseClassSerializerReflectionTest {

  case class SimpleCaseClass(name: String, var age: Int) {

    def this(name: String) = this(name, 0)

  }

  case class HigherKind(name: List[Int], id: String)

  case class Generic[T](item: T)

}

class OuterClass {

  case class InnerCaseClass(name: String, age: Int)

}
