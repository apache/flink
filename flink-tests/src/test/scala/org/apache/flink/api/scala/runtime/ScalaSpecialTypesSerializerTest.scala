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
package org.apache.flink.api.scala.runtime

import org.junit.Assert._

import org.apache.flink.api.common.typeutils.{TypeSerializer, SerializerTestInstance}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.junit.{Assert, Test}

import org.apache.flink.api.scala._

class ScalaSpecialTypesSerializerTest {

  @Test
  def testOption(): Unit = {
    val testData = Array(Some("Hello"), Some("Ciao"), None)
    runTests(testData)
  }

  @Test
  def testSome(): Unit = {
    val testData = Array(Some("Hello"), Some("Ciao"))
    runTests(testData)
  }

  @Test
  def testNone(): Unit = {
    val testData = Array(None, None)
    runTests(testData)
  }

  @Test
  def testEither(): Unit = {
    val testData = Array(Left("Hell"), Right(3))
    runTests(testData)
  }

  @Test
  def testLeft(): Unit = {
    val testData = Array(Left("Hell"), Left("CIao"))
    runTests(testData)
  }

  @Test
  def testRight(): Unit = {
    val testData = Array(Right("Hell"), Right("CIao"))
    runTests(testData)
  }


  private final def runTests[T : TypeInformation](instances: Array[T]) {
    try {
      val typeInfo = implicitly[TypeInformation[T]]
      val serializer = typeInfo.createSerializer
      val typeClass = typeInfo.getTypeClass
      val test =
        new ScalaSpecialTypesSerializerTestInstance[T](serializer, typeClass, -1, instances)
      test.testAll()
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }
}

class ScalaSpecialTypesSerializerTestInstance[T](
                                                serializer: TypeSerializer[T],
                                                typeClass: Class[T],
                                                length: Int,
                                                testData: Array[T])
  extends SerializerTestInstance[T](serializer, typeClass, length, testData: _*) {

  @Test
  override def testInstantiate(): Unit = {
    try {
      val serializer: TypeSerializer[T] = getSerializer
      val instance: T = serializer.createInstance
      assertNotNull("The created instance must not be null.", instance)
      val tpe: Class[T] = getTypeClass
      assertNotNull("The test is corrupt: type class is null.", tpe)
      // We cannot check this because Collection Instances are not always of the type
      // that the user writes, they might have generated names.
      // assertEquals("Type of the instantiated object is wrong.", tpe, instance.getClass)
    }
    catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
      }
    }
  }

  override protected def deepEquals(message: String, should: T, is: T) {
    should match {
      case trav: TraversableOnce[_] =>
        val isTrav = is.asInstanceOf[TraversableOnce[_]]
        assertEquals(message, trav.size, isTrav.size)
        val it = trav.toIterator
        val isIt = isTrav.toIterator
        while (it.hasNext) {
          val should = it.next()
          val is = isIt.next()
          assertEquals(message, should, is)
        }

      case _ =>
        super.deepEquals(message, should, is)
    }
  }
}

