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

package org.apache.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.{Alias, UnresolvedFieldReference}
import org.apache.flink.table.utils.{MockTableEnvironment, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test
import org.junit.Assert.assertEquals

class TableEnvironmentTest extends TableTestBase {

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

  @Test
  def testGetFieldInfoAtomic(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(atomicType)

    fieldInfo._1.zip(Array("f0")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleNames(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
    ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassNames(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
    ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoNames1(): Unit = {
    tEnv.getFieldInfo(
      pojoType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
      ))
  }

  @Test
  def testGetFieldInfoPojoNames2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        UnresolvedFieldReference("pf3"),
        UnresolvedFieldReference("pf1"),
        UnresolvedFieldReference("pf2")
      ))

    fieldInfo._1.zip(Array("pf3", "pf1", "pf2")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoAtomicName1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      atomicType,
      Array(UnresolvedFieldReference("name"))
    )

    fieldInfo._1.zip(Array("name")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicName2(): Unit = {
    tEnv.getFieldInfo(
      atomicType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2")
      ))
  }

  @Test
  def testGetFieldInfoTupleAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("f0"), "name1"),
        Alias(UnresolvedFieldReference("f1"), "name2"),
        Alias(UnresolvedFieldReference("f2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("f2"), "name1"),
        Alias(UnresolvedFieldReference("f0"), "name2"),
        Alias(UnresolvedFieldReference("f1"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoTupleAlias3(): Unit = {
    tEnv.getFieldInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test
  def testGetFieldInfoCClassAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("cf1"), "name1"),
        Alias(UnresolvedFieldReference("cf2"), "name2"),
        Alias(UnresolvedFieldReference("cf3"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("cf3"), "name1"),
        Alias(UnresolvedFieldReference("cf1"), "name2"),
        Alias(UnresolvedFieldReference("cf2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoCClassAlias3(): Unit = {
    tEnv.getFieldInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test
  def testGetFieldInfoPojoAlias1(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("pf1"), "name1"),
        Alias(UnresolvedFieldReference("pf2"), "name2"),
        Alias(UnresolvedFieldReference("pf3"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojoAlias2(): Unit = {
    val fieldInfo = tEnv.getFieldInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("pf3"), "name1"),
        Alias(UnresolvedFieldReference("pf1"), "name2"),
        Alias(UnresolvedFieldReference("pf2"), "name3")
      ))

    fieldInfo._1.zip(Array("name1", "name2", "name3")).foreach(x => assertEquals(x._2, x._1))
    fieldInfo._2.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoAlias3(): Unit = {
    tEnv.getFieldInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias( UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicAlias(): Unit = {
    tEnv.getFieldInfo(
      atomicType,
      Array(
        Alias(UnresolvedFieldReference("name1"), "name2")
      ))
  }

  @Test
  def testSqlWithoutRegisteringForBatchTables(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tEnv.sql(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tEnv.sql(s"SELECT d, e, f FROM $table, $table2 WHERE c = d")

    val join = unaryNode(
      "DataSetJoin",
      binaryNode(
        "DataSetCalc",
        batchTableNode(0),
        batchTableNode(1),
        term("select", "c")),
      term("where", "=(c, d)"),
      term("join", "c, d, e, f"),
      term("joinType", "InnerJoin"))

    val expected2 = unaryNode(
      "DataSetCalc",
      join,
      term("select", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }

  @Test
  def testSqlWithoutRegisteringForStreamTables(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tEnv.sql(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tEnv.sql(s"SELECT d, e, f FROM $table2 UNION SELECT a, b, c FROM $table")

    val expected2 = binaryNode(
      "DataStreamUnion",
      streamTableNode(1),
      streamTableNode(0),
      term("union", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }

}

case class CClass(cf1: Int, cf2: String, cf3: Double)

class PojoClass(var pf1: Int, var pf2: String, var pf3: Double) {
  def this() = this(0, "", 0.0)
}
