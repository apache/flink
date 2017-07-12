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
package org.apache.flink.table.api.scala.batch.validation

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala.batch.{CClass, PojoClass}
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.expressions.{Alias, UnresolvedFieldReference}
import org.apache.flink.table.runtime.types.CRowTypeInfo
import org.apache.flink.types.Row
import org.junit.Assert.assertTrue
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TableEnvironmentValidationTest(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env, config)

  val tupleType = new TupleTypeInfo(
    INT_TYPE_INFO,
    STRING_TYPE_INFO,
    DOUBLE_TYPE_INFO)

  val rowType = new RowTypeInfo(INT_TYPE_INFO, STRING_TYPE_INFO,DOUBLE_TYPE_INFO)

  val cRowType = new CRowTypeInfo(rowType)

  val caseClassType: TypeInformation[CClass] = implicitly[TypeInformation[CClass]]

  val pojoType: TypeInformation[PojoClass] = TypeExtractor.createTypeInfo(classOf[PojoClass])

  val atomicType = INT_TYPE_INFO

  val genericRowType = new GenericTypeInfo[Row](classOf[Row])


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

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicName2(): Unit = {
    tEnv.getFieldInfo(
      atomicType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2")
      ))
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

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoAlias3(): Unit = {
    tEnv.getFieldInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
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

  @Test(expected = classOf[TableException])
  def testGetFieldInfoGenericRowAlias(): Unit = {
    tEnv.getFieldInfo(
      genericRowType,
      Array(UnresolvedFieldReference("first")))
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingDataSet(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds1)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    // Must fail. Name is already in use.
    tEnv.registerDataSet("MyTable", ds2)
  }

  @Test(expected = classOf[TableException])
  def testScanUnregisteredTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    // Must fail. No table registered under that name.
    tEnv.scan("someTable")
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", t1)
    val t2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv)
    // Must fail. Name is already in use.
    tEnv.registerDataSet("MyTable", t2)
  }

  @Test(expected = classOf[TableException])
  def testRegisterTableFromOtherEnv(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv1)
    // Must fail. Table is bound to different TableEnvironment.
    tEnv2.registerTable("MyTable", t1)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithToManyFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b, 'c, 'd)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithAmbiguousFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Field names not unique.
      .toTable(tEnv, 'a, 'b, 'b)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a + 1, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a as 'foo, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testGenericRow() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet)
  }

  @Test(expected = classOf[TableException])
  def testGenericRowWithAlias() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet, "nullField")
  }
}
