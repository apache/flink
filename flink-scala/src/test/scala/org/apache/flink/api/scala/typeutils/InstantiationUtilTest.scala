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

import java.io.ByteArrayOutputStream

import org.apache.flink.util.{InstantiationUtil, TestLogger}
import org.hamcrest.Matchers
import org.junit.{Assert, Test}

/**
  * Serialization/Deserialization tests of Scala types using the
  * [[org.apache.flink.util.InstantiationUtil]].
  */
class InstantiationUtilTest extends TestLogger {

  @Test
  def testNestedScalaTypeSerDe(): Unit = {
    val instance = new Foo.Bar.Foobar(42)

    val copy = serializeDeserializeInstance(instance)

    Assert.assertThat(copy, Matchers.equalTo(instance))
  }

  @Test
  def testAnonymousScalaTypeSerDe(): Unit = {
    val instance = new Foo.FooTrait {
      val value: Int = 41

      override def hashCode(): Int = 37 * value

      override def equals(obj: scala.Any): Boolean = {
        obj match {
          case x: Foo.FooTrait => value == x.value()
          case _ => false
        }
      }
    }

    val copy = serializeDeserializeInstance(instance)

    Assert.assertThat(copy, Matchers.equalTo(instance))
  }

  private def serializeDeserializeInstance[T](instance: T): T = {
    val baos = new ByteArrayOutputStream()

    InstantiationUtil.serializeObject(baos, instance)

    InstantiationUtil.deserializeObject(
      baos.toByteArray,
      getClass.getClassLoader,
      true)
  }
}

object Foo extends Serializable {
  trait FooTrait extends Serializable {
    def value(): Int
  }

  object Bar extends Serializable {
    class Foobar(val x: Int) extends Serializable {
      override def hashCode(): Int = 37 * x

      override def equals(obj: scala.Any): Boolean = {
        obj match {
          case other: Foobar =>
            other.x == x
          case _ =>
            false
        }
      }
    }

  }
}
