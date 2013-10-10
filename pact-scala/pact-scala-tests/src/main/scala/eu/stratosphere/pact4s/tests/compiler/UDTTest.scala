/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.tests.compiler

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.analysis._
import eu.stratosphere.pact4s.common.operators._

import eu.stratosphere.pact.common.`type`._
import eu.stratosphere.pact.common.`type`.base._

import scala.collection.JavaConversions._

class MutableTest {
  case class MyOtherString(var value: String)
  abstract sealed class MyValue
  case class MyString(var value: String, var foo: MyOtherString) extends MyValue
  case class MyInt(var value: Int) extends MyValue
  
  val udt1 = implicitly[UDT[MyOtherString]]
  val udt2 = implicitly[UDT[MyInt]]
  val udt3 = implicitly[UDT[MyValue]]
}

class GenericTest[T] {

  val testFun: UDT[T] => UDT[(Int, T)] = { implicit udtEv: UDT[T] => implicitly[UDT[(Int, T)]] }

  def testDef[T: UDT] = implicitly[UDT[(Int, T)]]

  val udt = {
    implicit val t1: UDT[T] = null
    implicitly[UDT[(Int, T)]]
  }
}

class SimpleTest {

  val primUdt = implicitly[UDT[Long]]
  val boxdUdt = implicitly[UDT[java.lang.Integer]]

  abstract sealed class Simple[S, T] { val x: T }
  case class A[S, T](x: T, y: A[S, T], z: Array[Simple[S, T]]) extends Simple[S, T]
  case class B[S, T](x: T, y: Seq[Seq[B[S, T]]]) extends Simple[S, T]
  case class C[S, T](x: T, y: Seq[Long]) extends Simple[S, T]

  val simpUdt = implicitly[UDT[Simple[String, Long]]]
}

class BlockTest {

  val udt = {
    val x: Int = 0
    implicitly[UDT[Int]]
  }

  def testDef(x: Int) = {
    val y = x
    implicitly[UDT[Option[Long]]]
  }

  println {
    val x: Int = 0
    implicitly[UDT[Long]]
  }

  val udtTestInst1 = {
    val x = implicitly[analysis.UDT[(Int, Int, (String, Array[Int]))]]
    val y = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s.toInt, s)) }))
    implicitly[analysis.UDT[(Int, Int, (Int, String))]]
    val z = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s, s)) }))
    implicitly[analysis.UDT[(Int, Int, (Int, Int))]]
    implicitly[analysis.UDT[(Int, Int, (String, String))]]
  }

  val udtTestInst2 = new DataSource("", DelimetedDataSourceFormat({ s: String => (s, s, (s.toInt, s)) }))
}

abstract class Test extends TestGeneratedImplicits {

  abstract sealed class Foo
  case class Bar(x: Int, y: Long, z: Seq[Baz], zz: Baz) extends Foo
  case class Baz(x: Int, y: Long, z: Array[(Int, Long)]) extends Foo

  case class Rec(x: Int, y: Long, f: Foo, g: Seq[Foo]) extends Foo

  case class Thing[T](x: Long, y: Long, z: T)

  abstract sealed class Simple
  case class B(b: Long, ba: A, s: Simple) extends Simple
  case class A(a: Long, ab: B, s: Simple) extends Simple

  case class Test1(x1: Long, y1: Test1, z1: Test2)
  case class Test2(x2: Long, y2: Test1, z2: Test2)

  abstract sealed class Base[+T] {
    val v: T; val w: Base[T]; val x: Long = 0; val y: Option[Long]
  }
  case class Sub1[T](v: T, w: Base[T], override val x: Long, y: Option[Long]) extends Base[T]
  case class Sub2[T](v: T, w: Sub1[T], y: Some[Long], z: Long) extends Base[T]
  case class Sub3[T](v: T, w: Base[T], override val x: Long, y: Option[Long], z1: Base[T], z2: Seq[Base[T]], z3: List[Seq[Base[T]]], z4: Array[List[Seq[Base[T]]]]) extends Base[T]

  val simpUdt1 = implicitly[UDT[Option[Simple]]]
  val simpUdt2 = implicitly[UDT[Option[Simple]]]

  val baseUdt1 = implicitly[UDT[Base[String]]]
  val baseUdt2 = implicitly[UDT[Base[String]]]

  val testUdt1 = implicitly[UDT[Test1]]
  val testUdt2 = implicitly[UDT[Test1]]

  val boxedLongUdt1 = implicitly[UDT[java.lang.Long]]
  val boxedLongUdt2 = implicitly[UDT[java.lang.Long]]
  val boxedLongArrayUdt1 = implicitly[UDT[Array[java.lang.Long]]]
  val boxedLongArrayUdt2 = implicitly[UDT[Array[java.lang.Long]]]
  val boxedLongListUdt1 = implicitly[UDT[Seq[java.lang.Long]]]
  val boxedLongListUdt2 = implicitly[UDT[Seq[java.lang.Long]]]

  val fooUdt1 = implicitly[UDT[Foo]]
  val fooUdt2 = implicitly[UDT[Foo]]
  val barUdt1 = implicitly[UDT[Bar]]
  val barUdt2 = implicitly[UDT[Bar]]
  val bazUdt1 = implicitly[UDT[Baz]]
  val bazUdt2 = implicitly[UDT[Baz]]
  val recUdt1 = implicitly[UDT[Rec]]
  val recUdt2 = implicitly[UDT[Rec]]
  val optBarUdt1 = implicitly[UDT[Option[Bar]]]
  val optBarUdt2 = implicitly[UDT[Option[Bar]]]
  val optFooUdt1 = implicitly[UDT[Option[Foo]]]
  val optFooUdt2 = implicitly[UDT[Option[Foo]]]

}

trait TestGeneratedImplicits { this: Test =>

  val baseUdt3 = implicitly[UDT[Base[Long]]]
  val baseUdt4 = implicitly[UDT[Base[Long]]]

  val testUdt = implicitly[UDT[Simple]]
  val fooUdt3 = implicitly[UDT[Foo]]
  val fooUdt4 = implicitly[UDT[Foo]]
  val barUdt3 = implicitly[UDT[Bar]]
  val barUdt4 = implicitly[UDT[Bar]]
  val bazUdt3 = implicitly[UDT[Baz]]
  val bazUdt4 = implicitly[UDT[Baz]]
  val recUdt3 = implicitly[UDT[Rec]]
  val recUdt4 = implicitly[UDT[Rec]]
  val optBarUdt3 = implicitly[UDT[Option[Bar]]]
  val optBarUdt4 = implicitly[UDT[Option[Bar]]]
  val optFooUdt3 = implicitly[UDT[Option[Foo]]]
  val optFooUdt4 = implicitly[UDT[Option[Foo]]]

}

class TestParent {
  abstract sealed class Foo
  case class B(b: Long, ba: A) extends Foo
  case class A(a: Long, ab: B) extends Foo
  val udtParent = implicitly[UDT[Foo]]
}

class TestChild extends TestParent {
  val udtChild = implicitly[UDT[Foo]]
}

class Outer {
  case class Test[S, T](inner: S, outer: T)
  case class X(ov: Long, ox: X)

  class Inner {
    type Y = Outer.this.X
    case class Test[S, T](inner: S, outer: T)
    case class X(iv: Long, ix: X, iy: Y)

    private val plainUdt = implicitly[UDT[Test[X, Outer.this.X]]]
    private val aliasUdt = implicitly[UDT[Test[X, Y]]]

  }

  class Sub extends Inner {
    type Z = super.X

    private val innerUdt = implicitly[UDT[Test[X, Y]]]
    private val outerUdt = implicitly[UDT[Outer.this.Test[Y, Z]]]
  }
}
