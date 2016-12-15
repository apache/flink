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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{SerializerTestInstance, TypeSerializer}
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.{Assert, Test}

import scala.collection.{SortedMap, SortedSet}
import scala.util.{Failure, Success}

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

  @Test
  def testEnumValue(): Unit = {
    val testData = Array(WeekDay.Mon, WeekDay.Fri, WeekDay.Tue, WeekDay.Sun, WeekDay.Wed)
    runTests(testData)
  }

  @Test
  def testTry(): Unit = {
    val testData = Array(Success("Hell"), Failure(new RuntimeException("test")))
    runTests(testData)
  }

  @Test
  def testSuccess(): Unit = {
    val testData = Array(Success("Hell"), Success("Yeah"))
    runTests(testData)
  }

  @Test
  def testFailure(): Unit = {
    val testData = Array(
      Failure(new RuntimeException("test")),
      Failure(new RuntimeException("one, two")))
    runTests(testData)
  }

  @Test
  def testStringArray(): Unit = {
    val testData = Array(Array("Foo", "Bar"), Array("Hello"))
    runTests(testData)
  }

  @Test
  def testIntArray(): Unit = {
    val testData = Array(Array(1,3,3,7), Array(4,7))
    runTests(testData)
  }

  @Test
  def testArrayWithCaseClass(): Unit = {
    val testData = Array(Array((1, "String"), (2, "Foo")), Array((4, "String"), (3, "Foo")))
    runTests(testData)
  }

  @Test
  def testSortedMap(): Unit = {
    val testData = Array(SortedMap("Hello" -> 1, "World" -> 2), SortedMap("Foo" -> 42))
    runTests(testData)
  }

  @Test
  def testSortedSet(): Unit = {
    val testData = Array(SortedSet(1,2,3), SortedSet(2,3))
    runTests(testData)
  }

  private final def runTests[T : TypeInformation](instances: Array[T]) {
    try {
      val typeInfo = implicitly[TypeInformation[T]]
      val serializer = typeInfo.createSerializer(new ExecutionConfig)
      val typeClass = typeInfo.getTypeClass
      val test = new ScalaSpecialTypesSerializerTestInstance[T](
        serializer,
        typeClass,
        serializer.getLength,
        instances)
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
      if (!serializer.isInstanceOf[KryoSerializer[_]]) {
        // kryo serializer does return null, so only test for non-kryo-serializers
        val instance: T = serializer.createInstance
        assertNotNull("The created instance must not be null.", instance)
      }
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

      case Failure(t) =>
        is.asInstanceOf[Failure[_]].exception.equals(t)

      case _ =>
        super.deepEquals(message, should, is)
    }
  }
}

object WeekDay extends Enumeration {
  type WeekDay = Value
  val Mon, Tue, Wed, Thu, Fri, Sat, Sun = Value
}

