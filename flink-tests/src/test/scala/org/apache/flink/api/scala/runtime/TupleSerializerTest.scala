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

import java.util
import java.util.Random

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.AbstractGenericTypeSerializerTest._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassSerializer
import org.apache.flink.util.StringUtils
import org.joda.time.LocalDate
import org.junit.Assert._
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class TupleSerializerTest {

  @Test
  def testProperDeepCopy(): Unit = {
    val tpe = createTypeInformation[((String, Int), (Int, String))]

    val originalSerializer =
      tpe.createSerializer(new ExecutionConfig)
        .asInstanceOf[CaseClassSerializer[((String, Int), (Int, String))]]
    val duplicateSerializer = originalSerializer.duplicate()

    duplicateSerializer.getFieldSerializers

    // the list of child serializers must be duplicated
    assertTrue(duplicateSerializer.getFieldSerializers ne originalSerializer.getFieldSerializers)

    // each of the child serializers (which are themselves CaseClassSerializers) must be duplicated
    assertTrue(
      duplicateSerializer.getFieldSerializers()(0) ne originalSerializer.getFieldSerializers()(0))

    assertTrue(
      duplicateSerializer.getFieldSerializers()(1) ne originalSerializer.getFieldSerializers()(1))
  }

  @Test
  def testTuple1Int(): Unit = {
    val testTuples = Array(Tuple1(42), Tuple1(1), Tuple1(0), Tuple1(-1), Tuple1(Int.MaxValue),
      Tuple1(Int.MinValue))
    runTests(testTuples, 4)
  }

  @Test
  def testTuple1String(): Unit = {
    val rnd: Random = new Random(68761564135413L)
    val testTuples = Array(
      Tuple1(StringUtils.getRandomString(rnd, 10, 100)),
      Tuple1("abc"),
      Tuple1(""),
      Tuple1(StringUtils.getRandomString(rnd, 30, 170)),
      Tuple1(StringUtils.getRandomString(rnd, 15, 50)),
      Tuple1(""))
    runTests(testTuples, -1)
  }

  @Test
  def testTuple1StringArray(): Unit = {
    val rnd: Random = new Random(289347567856686223L)

    val arr1 = Array(
      "abc",
      "",
      StringUtils.getRandomString(rnd, 10, 100),
      StringUtils.getRandomString(rnd, 15, 50),
      StringUtils.getRandomString(rnd, 30, 170),
      StringUtils.getRandomString(rnd, 14, 15),
      "")
    val arr2 = Array(
      "foo",
      "",
      StringUtils.getRandomString(rnd, 10, 100),
      StringUtils.getRandomString(rnd, 1000, 5000),
      StringUtils.getRandomString(rnd, 30000, 35000),
      StringUtils.getRandomString(rnd, 100 * 1024, 105 * 1024),
      "bar")
    val testTuples = Array(Tuple1(arr1), Tuple1(arr2))
    runTests(testTuples, -1)
  }

  @Test
  def testTuple2StringDouble(): Unit = {
    val rnd: Random = new Random(807346528946L)

    val testTuples = Array(
      (StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble),
      (StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble),
      (StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble),
      ("", rnd.nextDouble),
      (StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble),
      (StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble))
    runTests(testTuples, -1)
  }

  @Test
  def testTuple2StringJodaTime(): Unit = {
    val rnd: Random = new Random(807346528946L)

    val testTuples = Array(
      (StringUtils.getRandomString(rnd, 10, 100), new LocalDate(rnd.nextInt)),
      (StringUtils.getRandomString(rnd, 10, 100), new LocalDate(rnd.nextInt)),
      (StringUtils.getRandomString(rnd, 10, 100), new LocalDate(rnd.nextInt)),
      ("", rnd.nextDouble),
      (StringUtils.getRandomString(rnd, 10, 100), new LocalDate(rnd.nextInt)),
      (StringUtils.getRandomString(rnd, 10, 100), new LocalDate(rnd.nextInt)))
      
    runTests(testTuples, -1)
  }

  @Test
  def testTuple2StringStringArray(): Unit = {
    val rnd: Random = new Random(289347567856686223L)

    val arr1 = Array(
      "abc",
      "",
      StringUtils.getRandomString(rnd, 10, 100),
      StringUtils.getRandomString(rnd, 15, 50),
      StringUtils.getRandomString(rnd, 30, 170),
      StringUtils.getRandomString(rnd, 14, 15), "")
    val arr2 = Array(
      "foo",
      "",
      StringUtils.getRandomString(rnd, 10, 100),
      StringUtils.getRandomString(rnd, 1000, 5000),
      StringUtils.getRandomString(rnd, 30000, 35000),
      StringUtils.getRandomString(rnd, 100 * 1024, 105 * 1024),
      "bar")
    val testTuples = Array(
      (StringUtils.getRandomString(rnd, 30, 170), arr1),
      (StringUtils.getRandomString(rnd, 30, 170), arr2),
      (StringUtils.getRandomString(rnd, 30, 170), arr1),
      (StringUtils.getRandomString(rnd, 30, 170), arr2),
      (StringUtils.getRandomString(rnd, 30, 170), arr2))
    runTests(testTuples, -1)
  }

  @Test
  def testTuple5CustomObjects(): Unit = {
    val rnd: Random = new Random(807346528946L)

    val a = new SimpleTypes
    val b =  new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)
    val c = new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)
    val d = new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)
    val e = new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)
    val f = new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)
    val g = new SimpleTypes(rnd.nextInt, rnd.nextLong, rnd.nextInt.asInstanceOf[Byte],
        StringUtils.getRandomString(rnd, 10, 100), rnd.nextInt.asInstanceOf[Short], rnd.nextDouble)

    val o1 = new ComplexNestedObject1(5626435)
    val o2 = new ComplexNestedObject1(76923)
    val o3 = new ComplexNestedObject1(-1100)
    val o4 = new ComplexNestedObject1(0)
    val o5 = new ComplexNestedObject1(44)

    val co1 = new ComplexNestedObject2(rnd)
    val co2 = new ComplexNestedObject2
    val co3 = new ComplexNestedObject2(rnd)
    val co4 = new ComplexNestedObject2(rnd)

    val b1 = new Book(976243875L, "The Serialization Odysse", 42)
    val b2 = new Book(0L, "Debugging byte streams", 1337)
    val b3 = new Book(-1L, "Low level interfaces", 0xC0FFEE)
    val b4 = new Book(Long.MaxValue, "The joy of bits and bytes", 0xDEADBEEF)
    val b5 = new Book(Long.MaxValue, "Winnign a prize for creative test strings", 0xBADF00)
    val b6 = new Book(-2L, "Distributed Systems", 0xABCDEF0123456789L)

    // We need to use actual java Lists here, to make them serializable by the GenericSerializer
    val list = new util.LinkedList[String]()
    list.addAll(List("A", "B", "C", "D", "E").asJava)
    val ba1 = new BookAuthor(976243875L, list, "Arno Nym")

    val list2 = new util.LinkedList[String]()
    val ba2 = new BookAuthor(987654321L, list2, "The Saurus")

    val testTuples = Array(
      (a, b1, o1, ba1, co1),
      (b, b2, o2, ba2, co2),
      (c, b3, o3, ba1, co3),
      (d, b2, o4, ba1, co4),
      (e, b4, o5, ba2, co4),
      (f, b5, o1, ba2, co4),
      (g, b6, o4, ba1, co2))
    runTests(testTuples, -1)
  }

  private final def runTests[T <: Product : TypeInformation](instances: Array[T], length: Int) {
    try {
      // Register the custom Kryo Serializer
      val conf = new ExecutionConfig()
      conf.registerTypeWithKryoSerializer(classOf[LocalDate], classOf[LocalDateSerializer])
      
      val tupleTypeInfo = implicitly[TypeInformation[T]].asInstanceOf[TupleTypeInfoBase[T]]
      val serializer = tupleTypeInfo.createSerializer(conf)
      val tupleClass = tupleTypeInfo.getTypeClass
      val test = new TupleSerializerTestInstance[T](serializer, tupleClass, length, instances)
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

