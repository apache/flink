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
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.TraversableSerializer
import org.junit.Assert._
import org.junit.{Assert, Ignore, Test}

import scala.collection.immutable.{BitSet, LinearSeq}
import scala.collection.mutable
import scala.ref.WeakReference
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader

class TraversableSerializerTest {

  // Note: SortedMap and SortedSet are serialized with Kryo

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

  @Test
  def testSet(): Unit = {
    val testData = Array(Set(1,2,3,3), Set(2,3))
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
  def testWithMixedPrimitives(): Unit = {
    // Does not work yet because the GenericTypeInfo used for the elements will
    // have a typeClass of Object, and therefore not deserialize the elements correctly.
    // It does work when used in a Job, though. Because the Objects get cast to
    // the correct type in the user function.
    val testData = Array(Seq(1, 1L, 1d, true, "Hello"), Seq(2, 2L, 2d, false, "Ciao"))
    runTests(testData)
  }

  @Test
  def sameClassLoaderAndCodeShouldProvideEqualKeys(): Unit = {
    val classLoaderA = new URLClassLoader(Seq.empty[java.net.URL], null)

    val keyA = TraversableSerializer.Key(classLoaderA, "code")
    val keyB = TraversableSerializer.Key(classLoaderA, "code")

    assertEquals(keyA, keyB)
  }

  @Test
  def differentClassLoadersProvideNonEqualKeys(): Unit = {
    val classLoaderA = new URLClassLoader(Seq.empty[java.net.URL], null)
    val classLoaderB = new URLClassLoader(Seq.empty[java.net.URL], null)
    
    val keyA = TraversableSerializer.Key(classLoaderA, "code")
    val keyB = TraversableSerializer.Key(classLoaderB, "code")
    
    assertNotEquals(keyA, keyB)
  }

  @Test
  def expiredReferenceShouldProduceNonEqualKeys(): Unit = {
    val classLoaderA = new URLClassLoader(Seq.empty[java.net.URL], null)

    val keyA = TraversableSerializer.Key(classLoaderA, "code")
    val keyB = keyA.copy(classLoaderRef = WeakReference(null)) 

    assertNotEquals(keyA, keyB)
  }

  @Test
  def bootStrapClassLoaderShouldProduceTheSameKeys(): Unit = {
    val keyA = TraversableSerializer.Key(null, "a")
    val keyB = TraversableSerializer.Key(null, "a")

    assertEquals(keyA, keyB)
  }

  @Test
  def differentCanBuildFromCodeShouldProduceDifferentKeys(): Unit = {
    val classLoaderA = new URLClassLoader(Seq.empty[java.net.URL], null)

    val keyA = TraversableSerializer.Key(classLoaderA, "a")
    val keyB = TraversableSerializer.Key(classLoaderA, "b")

    assertNotEquals(keyA, keyB)
  }
  
  private final def runTests[T : TypeInformation](instances: Array[T]) {
    try {
      val typeInfo = implicitly[TypeInformation[T]]
      val serializer = typeInfo.createSerializer(new ExecutionConfig)
      val typeClass = typeInfo.getTypeClass
      val test = new TraversableSerializerTestInstance[T](serializer, typeClass, -1, instances)
      test.testAll()
    } catch {
      case e: Exception =>
        System.err.println(e.getMessage)
        e.printStackTrace()
        Assert.fail(e.getMessage)
    }
  }
}

class Pojo(var name: String, var count: Int) {
  def this() = this("", -1)

  override def equals(other: Any): Boolean = {
    other match {
      case oP: Pojo => name == oP.name && count == oP.count
      case _ => false
    }
  }
}

@Ignore("Prevents this class from being considered a test class by JUnit.")
class TraversableSerializerTestInstance[T](
    serializer: TypeSerializer[T],
    typeClass: Class[T],
    length: Int,
    testData: Array[T])
  extends ScalaSpecialTypesSerializerTestInstance[T](serializer, typeClass, length, testData) {

  @Test
  override def testAll(): Unit = {
    super.testAll()
    testTraversableDeepCopy()
  }

  @Test
  def testTraversableDeepCopy(): Unit = {
    val serializer = getSerializer
    val elementSerializer = serializer.asInstanceOf[TraversableSerializer[_, _]].elementSerializer
    val data = getTestData

    // check for deep copy if type is immutable and not serialized with Kryo
    // elements of traversable should not have reference equality
    if (!elementSerializer.isImmutableType && !elementSerializer.isInstanceOf[KryoSerializer[_]]) {
      data.foreach { datum =>
        val original = datum.asInstanceOf[Traversable[_]].toIterable
        val copy = serializer.copy(datum).asInstanceOf[Traversable[_]].toIterable
        copy.zip(original).foreach { case (c: AnyRef, o: AnyRef) =>
          assertTrue("Copy of mutable element has reference equality.", c ne o)
        case _ => // ok
        }
      }
    }
  }

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
      case e: Exception =>
        System.err.println(e.getMessage)
        e.printStackTrace()
        fail("Exception in test: " + e.getMessage)
    }
  }

}

