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
import org.apache.flink.api.common.typeutils.SerializerTestInstance
import org.apache.flink.api.java.typeutils.GenericTypeInfo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

import org.joda.time.LocalDate

import org.junit.Test

import scala.reflect._

class KryoGenericTypeSerializerTest {

  @Test
  def testTraitSerialization(): Unit = {
    trait SimpleTrait {
      def contains(x: String): Boolean
    }
    class SimpleClass1 extends SimpleTrait {
      def contains(x: String) = true

      override def equals(other: Any): Boolean = other match {
        case other: SimpleClass1 => true
        case _ => false
      }
    }
    class SimpleClass2 extends SimpleTrait {
      def contains(x: String) = true

      override def equals(other: Any): Boolean = other match {
        case other: SimpleClass2 => true
        case _ => false
      }
    }

    val testData = Array(new SimpleClass1, new SimpleClass1, new SimpleClass2)
    runTests(testData)
  }

  @Test
  def testAbstractSerialization(): Unit = {
    abstract class SimpleAbstractBase {
      def contains(x: String): Boolean
    }
    class SimpleClass1 extends SimpleAbstractBase {
      def contains(x: String) = true

      override def equals(other: Any): Boolean = other match {
        case other: SimpleClass1 => true
        case _ => false
      }
    }
    class SimpleClass2 extends SimpleAbstractBase {
      def contains(x: String) = true

      override def equals(other: Any): Boolean = other match {
        case other: SimpleClass2 => true
        case _ => false
      }
    }

    val testData = Array(new SimpleClass1, new SimpleClass1, new SimpleClass2)
    runTests(testData)
  }

  @Test
  def testThrowableSerialization(): Unit = {
    val a = List(new RuntimeException("Hello"), new RuntimeException("there"))

    runTests(a)
  }

  @Test
  def jodaSerialization(): Unit = {
    val a = List(new LocalDate(1), new LocalDate(2))
    
    runTests(a)
  }

  @Test
  def testScalaListSerialization(): Unit = {
    val a = List(42,1,49,1337)

    runTests(a)
  }

  @Test
  def testScalaMutablelistSerialization(): Unit = {
    val a = scala.collection.mutable.ListBuffer(42,1,49,1337)

    runTests(a)
  }

  @Test
  def testScalaMapSerialization(): Unit = {
    val a = Map(("1" -> 1), ("2" -> 2), ("42" -> 42), ("1337" -> 1337))

    runTests(Seq(a))
  }

  @Test
  def testMutableMapSerialization(): Unit ={
    val a = scala.collection.mutable.Map((1 -> "1"), (2 -> "2"), (3 -> "3"))

    runTests(Seq(a))
  }

  @Test
  def testScalaListComplexTypeSerialization(): Unit = {
    val a = ComplexType("1234", 42, List(1,2,3,4))
    val b = ComplexType("4321", 24, List(4,3,2,1))
    val c = ComplexType("1337", 1, List(1))
    val list = List(a, b, c)

    runTests(list)
  }

  @Test
  def testHeterogenousScalaList(): Unit = {
    val a = new DerivedType("foo", "bar")
    val b = new BaseType("foobar")
    val c = new DerivedType2("bar", "foo")
    val list = List(a,b,c)

    runTests(list)
  }


  case class ComplexType(id: String, number: Int, values: List[Int]){
    override def equals(obj: Any): Boolean ={
      if(obj != null && obj.isInstanceOf[ComplexType]){
        val complexType = obj.asInstanceOf[ComplexType]
        id.equals(complexType.id) && number.equals(complexType.number) && values.equals(
          complexType.values)
      }else{
        false
      }
    }
  }

  class BaseType(val name: String){
    override def equals(obj: Any): Boolean = {
      if(obj != null && obj.isInstanceOf[BaseType]){
        obj.asInstanceOf[BaseType].name.equals(name)
      }else{
        false
      }
    }
  }

  class DerivedType(name: String, val sub: String) extends BaseType(name){
    override def equals(obj: Any): Boolean = {
      if(obj != null && obj.isInstanceOf[DerivedType]){
        super.equals(obj) && obj.asInstanceOf[DerivedType].sub.equals(sub)
      }else{
        false
      }
    }
  }

  class DerivedType2(name: String, val sub: String) extends BaseType(name){
    override def equals(obj: Any): Boolean = {
      if(obj != null && obj.isInstanceOf[DerivedType2]){
        super.equals(obj) && obj.asInstanceOf[DerivedType2].sub.equals(sub)
      }else{
        false
      }
    }
  }

  def runTests[T : ClassTag](objects: Seq[T]): Unit ={
    val clsTag = classTag[T]


    // Register the custom Kryo Serializer
    val conf = new ExecutionConfig
    conf.registerTypeWithKryoSerializer(classOf[LocalDate], classOf[LocalDateSerializer])
    val typeInfo = new GenericTypeInfo[T](clsTag.runtimeClass.asInstanceOf[Class[T]])
    val serializer = typeInfo.createSerializer(conf)
    val typeClass = typeInfo.getTypeClass

    val instance = new SerializerTestInstance[T](serializer, typeClass, -1, objects: _*)

    instance.testAll()
  }
}

class LocalDateSerializer extends Serializer[LocalDate] with java.io.Serializable {

  override def write(kryo: Kryo, output: Output, obj: LocalDate) {
    output.writeInt(obj.getYear())
    output.writeInt(obj.getMonthOfYear())
    output.writeInt(obj.getDayOfMonth())
  }

  override def read(kryo: Kryo, input: Input, typeClass: Class[LocalDate]) : LocalDate = {
    new LocalDate(input.readInt(), input.readInt(), input.readInt())
  }
}
