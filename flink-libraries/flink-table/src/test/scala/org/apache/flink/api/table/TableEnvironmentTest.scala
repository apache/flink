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

package org.apache.flink.api.table

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TypeExtractor, TupleTypeInfo}
import org.apache.flink.api.table.expressions.{Alias, UnresolvedFieldReference}
import org.apache.flink.api.table.sinks.TableSink
import org.junit.Test
import org.junit.Assert.assertEquals

class TableEnvironmentTest {

  val tEnv = new MockTableEnvironment

  val tupleType = new TupleTypeInfo(
    INT_TYPE_INFO,
    STRING_TYPE_INFO,
    DOUBLE_TYPE_INFO)

  val caseClassType = implicitly[TypeInformation[CClass]]

  val pojoType = TypeExtractor.createTypeInfo(classOf[PojoClass])

  val atomicType = INT_TYPE_INFO

  @Test
  def testGetFieldInfoTuple(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(tupleType)

    fieldInfo._1.zip(Array("f0", "f1", "f2")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClass(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(caseClassType)

    fieldInfo._1.zip(Array("cf1", "cf2", "cf3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojo(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(pojoType)

    fieldInfo._1.zip(Array("pf1", "pf2", "pf3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomic(): Unit = {
    tEnv.getFieldInfo(atomicType)
  }

  @Test
  def testGetFieldInfoTupleNames(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        new UnresolvedFieldReference("name1"),
        new UnresolvedFieldReference("name2"),
        new UnresolvedFieldReference("name3")
    ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassNames(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        new UnresolvedFieldReference("name1"),
        new UnresolvedFieldReference("name2"),
        new UnresolvedFieldReference("name3")
    ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoNames1(): Unit = {
    tEnv.getFieldInfo(
      pojoType,
      Array(
        new UnresolvedFieldReference("name1"),
        new UnresolvedFieldReference("name2"),
        new UnresolvedFieldReference("name3")
      ))
  }

  @Test
  def testGetFieldInfoPojoNames2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        new UnresolvedFieldReference("pf3"),
        new UnresolvedFieldReference("pf1"),
        new UnresolvedFieldReference("pf2")
      ))

    fieldInfo._1.zip(Array("pf3", "pf1", "pf2")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoAtomicName1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      atomicType,
      Array(new UnresolvedFieldReference("name"))
    )

    fieldInfo._1.zip(Array("name")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicName2(): Unit = {
    tEnv.getFieldInfo(
      atomicType,
      Array(
        new UnresolvedFieldReference("name1"),
        new UnresolvedFieldReference("name2")
      ))
  }

  @Test
  def testGetFieldInfoTupleAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        new Alias(UnresolvedFieldReference("f0"), "name1"),
        new Alias(UnresolvedFieldReference("f1"), "name2"),
        new Alias(UnresolvedFieldReference("f2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        new Alias(UnresolvedFieldReference("f2"), "name1"),
        new Alias(UnresolvedFieldReference("f0"), "name2"),
        new Alias(UnresolvedFieldReference("f1"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoTupleAlias3(): Unit = {
    tEnv.getFieldInfo(
      tupleType,
      Array(
        new Alias(UnresolvedFieldReference("xxx"), "name1"),
        new Alias(UnresolvedFieldReference("yyy"), "name2"),
        new Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test
  def testGetFieldInfoCClassAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        new Alias(new UnresolvedFieldReference("cf1"), "name1"),
        new Alias(new UnresolvedFieldReference("cf2"), "name2"),
        new Alias(new UnresolvedFieldReference("cf3"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        new Alias(new UnresolvedFieldReference("cf3"), "name1"),
        new Alias(new UnresolvedFieldReference("cf1"), "name2"),
        new Alias(new UnresolvedFieldReference("cf2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoCClassAlias3(): Unit = {
    tEnv.getFieldInfo(
      caseClassType,
      Array(
        new Alias(new UnresolvedFieldReference("xxx"), "name1"),
        new Alias(new UnresolvedFieldReference("yyy"), "name2"),
        new Alias(new UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test
  def testGetFieldInfoPojoAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        new Alias(new UnresolvedFieldReference("pf1"), "name1"),
        new Alias(new UnresolvedFieldReference("pf2"), "name2"),
        new Alias(new UnresolvedFieldReference("pf3"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojoAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        new Alias(new UnresolvedFieldReference("pf3"), "name1"),
        new Alias(new UnresolvedFieldReference("pf1"), "name2"),
        new Alias(new UnresolvedFieldReference("pf2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoAlias3(): Unit = {
    tEnv.getFieldInfo(
      pojoType,
      Array(
        new Alias(new UnresolvedFieldReference("xxx"), "name1"),
        new Alias(new UnresolvedFieldReference("yyy"), "name2"),
        new Alias(new UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicAlias(): Unit = {
    tEnv.getFieldInfo(
      atomicType,
      Array(
        new Alias(new UnresolvedFieldReference("name1"), "name2")
      ))
  }

}

class MockTableEnvironment extends TableEnvironment(new TableConfig) {

  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = ???

  override protected def checkValidTableName(name: String): Unit = ???

  override def sql(query: String): Table = ???
}

case class CClass(cf1: Int, cf2: String, cf3: Double)

class PojoClass(var pf1: Int, var pf2: String, var pf3: Double) {
  def this() = this(0, "", 0.0)
}
