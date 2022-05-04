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
package org.apache.flink.streaming.api.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.util.typeutils.{FieldAccessorFactory, FieldAccessorTest}
import org.apache.flink.util.TestLogger

import org.junit.Test
import org.scalatestplus.junit.JUnitSuiteLike

case class Outer(a: Int, i: Inner, b: Boolean)
case class Inner(x: Long, b: Boolean)
case class IntBoolean(foo: Int, bar: Boolean)
case class InnerCaseClass(a: Short, b: String)
case class OuterCaseClassWithInner(a: Int, i: InnerCaseClass, b: Boolean)

class CaseClassFieldAccessorTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testFieldAccessorFlatCaseClass(): Unit = {
    val tpeInfo = createTypeInformation[IntBoolean]

    {
      // by field name
      val accessor1 = FieldAccessorFactory.getAccessor[IntBoolean, Int](tpeInfo, "foo", null)
      val accessor2 = FieldAccessorFactory.getAccessor[IntBoolean, Boolean](tpeInfo, "bar", null)

      assert(accessor1.getFieldType.getTypeClass.getSimpleName == "Integer")
      assert(accessor2.getFieldType.getTypeClass.getSimpleName == "Boolean")

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
      val accessor1 = FieldAccessorFactory.getAccessor[IntBoolean, Int](tpeInfo, 0, null)
      val accessor2 = FieldAccessorFactory.getAccessor[IntBoolean, Boolean](tpeInfo, 1, null)

      assert(accessor1.getFieldType.getTypeClass.getSimpleName == "Integer")
      assert(accessor2.getFieldType.getTypeClass.getSimpleName == "Boolean")

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
  def testFieldAccessorPojoInCaseClass(): Unit = {
    var x = Outer(1, Inner(3L, true), false)
    val tpeInfo = createTypeInformation[Outer]
    val cfg = new ExecutionConfig

    val fib = FieldAccessorFactory.getAccessor[Outer, Boolean](tpeInfo, "i.b", cfg)
    assert(fib.getFieldType.getTypeClass.getSimpleName == "Boolean")
    assert(fib.get(x) == true)
    assert(x.i.b == true)
    x = fib.set(x, false)
    assert(fib.get(x) == false)
    assert(x.i.b == false)

    val fi = FieldAccessorFactory.getAccessor[Outer, Inner](tpeInfo, "i", cfg)
    assert(fi.getFieldType.getTypeClass.getSimpleName == "Inner")
    assert(fi.get(x).x == 3L)
    assert(x.i.x == 3L)
    x = fi.set(x, Inner(4L, true))
    assert(fi.get(x).x == 4L)
    assert(x.i.x == 4L)

    val fin = FieldAccessorFactory.getAccessor[Outer, Inner](tpeInfo, 1, cfg)
    assert(fin.getFieldType.getTypeClass.getSimpleName == "Inner")
    assert(fin.get(x).x == 4L)
    assert(x.i.x == 4L)
    x = fin.set(x, Inner(5L, true))
    assert(fin.get(x).x == 5L)
    assert(x.i.x == 5L)
  }

  @Test
  def testFieldAccessorTuple(): Unit = {
    val tpeInfo = createTypeInformation[(Int, Long)]
    var x = (5, 6L)
    val f0 = FieldAccessorFactory.getAccessor[(Int, Long), Int](tpeInfo, 0, null)
    assert(f0.getFieldType.getTypeClass.getSimpleName == "Integer")
    assert(f0.get(x) == 5)
    x = f0.set(x, 8)
    assert(f0.get(x) == 8)
    assert(x._1 == 8)
  }

  @Test
  def testFieldAccessorCaseClassInCaseClass(): Unit = {
    val tpeInfo = createTypeInformation[OuterCaseClassWithInner]

    var x = OuterCaseClassWithInner(1, InnerCaseClass(2, "alma"), true)

    val fib = FieldAccessorFactory
      .getAccessor[OuterCaseClassWithInner, String](tpeInfo, "i.b", null)
    assert(fib.getFieldType.getTypeClass.getSimpleName == "String")
    assert(fib.get(x) == "alma")
    assert(x.i.b == "alma")
    x = fib.set(x, "korte")
    assert(fib.get(x) == "korte")
    assert(x.i.b == "korte")

    val fi = FieldAccessorFactory
      .getAccessor[OuterCaseClassWithInner, InnerCaseClass](tpeInfo, "i", null)
    assert(fi.getFieldType.getTypeClass == classOf[InnerCaseClass])
    assert(fi.get(x) == InnerCaseClass(2, "korte"))
    x = fi.set(x, InnerCaseClass(3, "aaa"))
    assert(x.i == InnerCaseClass(3, "aaa"))
  }
}
