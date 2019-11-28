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

import org.apache.flink.api.scala.typeutils.ScalaCaseClassSerializerReflectionTest._
import org.apache.flink.util.TestLogger
import org.junit.Assert.assertEquals
import org.junit.Test


/**
  * Test obtaining the primary constructor of a case class
  * via reflection.
  */
class ScalaCaseClassSerializerReflectionTest extends TestLogger {

  @Test
  def usageExample(): Unit = {
    val constructor = ScalaCaseClassSerializer
      .lookupConstructor(classOf[SimpleCaseClass])

    val actual = constructor(Array("hi", 1.asInstanceOf[AnyRef]))

    assertEquals(SimpleCaseClass("hi", 1), actual)
  }

  @Test
  def genericCaseClass(): Unit = {
    val constructor = ScalaCaseClassSerializer
      .lookupConstructor(classOf[Generic[_]])

    val actual = constructor(Array(1.asInstanceOf[AnyRef]))

    assertEquals(Generic[Int](1), actual)
  }

  @Test
  def caseClassWithParameterizedList(): Unit = {
    val constructor = ScalaCaseClassSerializer
      .lookupConstructor(classOf[HigherKind])

    val actual = constructor(Array(List(1, 2, 3), "hey"))

    assertEquals(HigherKind(List(1, 2, 3), "hey"), actual)
  }

  @Test
  def tupleType(): Unit = {
    val constructor = ScalaCaseClassSerializer
      .lookupConstructor(classOf[(String, String, Int)])

    val actual = constructor(Array("a", "b", 7.asInstanceOf[AnyRef]))

    assertEquals(("a", "b", 7), actual)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def unsupportedInstanceClass(): Unit = {

    val outerInstance = new OuterClass

    ScalaCaseClassSerializer
      .lookupConstructor(classOf[outerInstance.InnerCaseClass])
  }

  @Test
  def valueClass(): Unit = {
    val constructor = ScalaCaseClassSerializer
      .lookupConstructor(classOf[Measurement])

    val arguments = Array(
      1.asInstanceOf[AnyRef],
      new DegreeCelsius(0.5f).asInstanceOf[AnyRef]
    )
    
    val actual = constructor(arguments)

    assertEquals(Measurement(1, new DegreeCelsius(0.5f)), actual)
  }
}

object ScalaCaseClassSerializerReflectionTest {

  case class SimpleCaseClass(name: String, var age: Int) {

    def this(name: String) = this(name, 0)

  }

  case class HigherKind(name: List[Int], id: String)

  case class Generic[T](item: T)

  class DegreeCelsius(val value: Float) extends AnyVal {
    override def toString: String = s"$value °C"
  }

  case class Measurement(i: Int, temperature: DegreeCelsius)

}

class OuterClass {

  case class InnerCaseClass(name: String, age: Int)

}
