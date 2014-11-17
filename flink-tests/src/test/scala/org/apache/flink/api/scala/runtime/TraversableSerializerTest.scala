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

import org.apache.flink.api.common.functions.InvalidTypesException
import org.junit.Assert._

import org.apache.flink.api.common.typeutils.{TypeSerializer, SerializerTestInstance}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.junit.{Ignore, Assert, Test}

import org.apache.flink.api.scala._

import scala.collection.immutable.{BitSet, SortedSet, LinearSeq}
import scala.collection.{SortedMap, mutable}

class TraversableSerializerTest {

  @Test
  def testSeq(): Unit = {
    val testData = Array(Seq(1,2,3), Seq(2,3))
    runTests(testData)
  }

  @Test
  def testIndexedSeq(): Unit = {
    val testData = Array(IndexedSeq(1,2,3), IndexedSeq(2,3))
    runTests(testData)
  }

  @Test
  def testLinearSeq(): Unit = {
    val testData = Array(LinearSeq(1,2,3), LinearSeq(2,3))
    runTests(testData)
  }

  @Test
  def testMap(): Unit = {
    val testData = Array(Map("Hello" -> 1, "World" -> 2), Map("Foo" -> 42))
    runTests(testData)
  }

  @Test(expected = classOf[InvalidTypesException])
  def testSortedMap(): Unit = {
    // SortedSet is not supported right now.
    val testData = Array(SortedMap("Hello" -> 1, "World" -> 2), SortedMap("Foo" -> 42))
    runTests(testData)
  }

  @Test
  def testSet(): Unit = {
    val testData = Array(Set(1,2,3,3), Set(2,3))
    runTests(testData)
  }

  @Test(expected = classOf[InvalidTypesException])
  def testSortedSet(): Unit = {
    // SortedSet is not supported right now.
    val testData = Array(SortedSet(1,2,3), SortedSet(2,3))
    runTests(testData)
  }

  @Test
  def testBitSet(): Unit = {
    val testData = Array(BitSet(1,2,3,4), BitSet(2,3,2))
    runTests(testData)
  }

  @Test
  def testMutableList(): Unit = {
    val testData = Array(mutable.MutableList(1,2,3), mutable.MutableList(2,3,2))
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
//
  @Test
  def testWithCaseClass(): Unit = {
    val testData = Array(Seq((1, "String"), (2, "Foo")), Seq((4, "String"), (3, "Foo")))
    runTests(testData)
  }

  @Test
  def testWithPojo(): Unit = {
    val testData = Array(Seq(new Pojo("hey", 1)), Seq(new Pojo("Ciao", 2), new Pojo("Foo", 3)))
    runTests(testData)
  }

  @Test
  @Ignore
  def testWithMixedPrimitives(): Unit = {
    // Does not work yet because the GenericTypeInfo used for the elements will
    // have a typeClass of Object, and therefore not deserializer the elements correctly.
    // It does work when used in a Job, though. Because the Objects get cast to
    // the correct type in the user function.
    val testData = Array(Seq(1,1L,1d,true,"Hello"), Seq(2,2L,2d,false,"Ciao"))
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

class Pojo(val name: String, val count: Int) {
  def this() = this("", -1)

  override def equals(other: Any): Boolean = {
    other match {
      case oP: Pojo => name == oP.name && count == oP.count
      case _ => false
    }
  }
}

class ScalaCollectionSerializerTestInstance[T](
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

