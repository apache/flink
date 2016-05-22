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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.FieldAccessorTest
import org.apache.flink.util.TestLogger
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike
import org.apache.flink.api.scala._

class CaseClassTypeInfoTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testCaseClassTypeInfoEquality(): Unit = {
    val tpeInfo1 = new CaseClassTypeInfo[Tuple2[Int, String]](
      classOf[Tuple2[Int, String]],
      Array(),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      Array("_1", "_2")) {
      override def createSerializer(config: ExecutionConfig): TypeSerializer[(Int, String)] = ???
    }

    val tpeInfo2 = new CaseClassTypeInfo[Tuple2[Int, String]](
      classOf[Tuple2[Int, String]],
      Array(),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      Array("_1", "_2")) {
      override def createSerializer(config: ExecutionConfig): TypeSerializer[(Int, String)] = ???
    }

    assert(tpeInfo1.equals(tpeInfo2))
    assert(tpeInfo1.hashCode() == tpeInfo2.hashCode())
  }

  @Test
  def testCaseClassTypeInfoInequality(): Unit = {
    val tpeInfo1 = new CaseClassTypeInfo[Tuple2[Int, String]](
      classOf[Tuple2[Int, String]],
      Array(),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
      Array("_1", "_2")) {
      override def createSerializer(config: ExecutionConfig): TypeSerializer[(Int, String)] = ???
    }

    val tpeInfo2 = new CaseClassTypeInfo[Tuple2[Int, Boolean]](
      classOf[Tuple2[Int, Boolean]],
      Array(),
      Array(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO),
      Array("_1", "_2")) {
      override def createSerializer(config: ExecutionConfig): TypeSerializer[(Int, Boolean)] = ???
    }

    assert(!tpeInfo1.equals(tpeInfo2))
  }

  @Test
  def testFieldAccessorFlatCaseClass(): Unit = {
    case class IntBoolean(foo: Int, bar: Boolean)
    val tpeInfo = createTypeInformation[IntBoolean]

    {
      // by field name
      val accessor1 = tpeInfo.getFieldAccessor[Int]("foo", null)
      val accessor2 = tpeInfo.getFieldAccessor[Boolean]("bar", null)

      val x1 = IntBoolean(5, false)
      assert(accessor1.get(x1) == 5)
      assert(accessor2.get(x1) == false)
      assert(x1.foo == 5)
      assert(x1.bar == false)

      val x2: IntBoolean = accessor1.set(x1, 6)
      assert(accessor1.get(x2) == 6)
      assert(x2.foo == 6)

      val x3 = accessor2.set(x2, true)
      assert(x3.bar == true)
      assert(accessor2.get(x3) == true)
      assert(x3.foo == 6)
    }

    {
      // by field pos
      val accessor1 = tpeInfo.getFieldAccessor[Int](0, null)
      val accessor2 = tpeInfo.getFieldAccessor[Boolean](1, null)

      val x1 = IntBoolean(5, false)
      assert(accessor1.get(x1) == 5)
      assert(accessor2.get(x1) == false)
      assert(x1.foo == 5)
      assert(x1.bar == false)

      val x2: IntBoolean = accessor1.set(x1, 6)
      assert(accessor1.get(x2) == 6)
      assert(x2.foo == 6)

      val x3 = accessor2.set(x2, true)
      assert(x3.bar == true)
      assert(accessor2.get(x3) == true)
      assert(x3.foo == 6)
    }
  }

  @Test
  def testFieldAccessorTuple(): Unit = {
    val tpeInfo = createTypeInformation[(Int, Long)]
    var x = (5, 6L)
    val f0 = tpeInfo.getFieldAccessor[Int](0, null)
    assert(f0.get(x) == 5)
    x = f0.set(x, 8)
    assert(f0.get(x) == 8)
    assert(x._1 == 8)
  }

  @Test
  def testFieldAccessorCaseClassInCaseClass(): Unit = {
    case class Inner(a: Short, b: String)
    case class Outer(a: Int, i: Inner, b: Boolean)
    val tpeInfo = createTypeInformation[Outer]

    var x = Outer(1, Inner(2, "alma"), true)

    val fib = tpeInfo.getFieldAccessor[String]("i.b", null)
    assert(fib.get(x) == "alma")
    assert(x.i.b == "alma")
    x = fib.set(x, "korte")
    assert(fib.get(x) == "korte")
    assert(x.i.b == "korte")

    val fi = tpeInfo.getFieldAccessor[Inner]("i", null)
    assert(fi.get(x) == Inner(2, "korte"))
    x = fi.set(x, Inner(3, "aaa"))
    assert(x.i == Inner(3, "aaa"))
  }

  @Test
  def testFieldAccessorPojoInCaseClass(): Unit = {
    case class Outer(a: Int, i: FieldAccessorTest.Inner, b: Boolean)
    var x = Outer(1, new FieldAccessorTest.Inner(3L, true), false)
    val tpeInfo = createTypeInformation[Outer]
    val cfg = new ExecutionConfig

    val fib = tpeInfo.getFieldAccessor[Boolean]("i.b", cfg)
    assert(fib.get(x) == true)
    assert(x.i.b == true)
    x = fib.set(x, false)
    assert(fib.get(x) == false)
    assert(x.i.b == false)

    val fi = tpeInfo.getFieldAccessor[FieldAccessorTest.Inner]("i", cfg)
    assert(fi.get(x).x == 3L)
    assert(x.i.x == 3L)
    x = fi.set(x, new FieldAccessorTest.Inner(4L, true))
    assert(fi.get(x).x == 4L)
    assert(x.i.x == 4L)

    val fin = tpeInfo.getFieldAccessor[FieldAccessorTest.Inner](1, cfg)
    assert(fin.get(x).x == 4L)
    assert(x.i.x == 4L)
    x = fin.set(x, new FieldAccessorTest.Inner(5L, true))
    assert(fin.get(x).x == 5L)
    assert(x.i.x == 5L)
  }
}
