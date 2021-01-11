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

package org.apache.flink.table.api.validation

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironmentTest.{CClass, PojoClass}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{TableException, ValidationException, _}
import org.apache.flink.table.runtime.types.CRowTypeInfo
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit._

class TableEnvironmentValidationTest extends TableTestBase {

  private val env = ExecutionEnvironment.getExecutionEnvironment
  private val tEnv = BatchTableEnvironment.create(env)

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

  @Test
  def testInvalidAliasInRefByPosMode(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // all references must happen position-based
    util.addTable('a, 'b, 'f2 as 'c)(tupleType)
        }
    }

  @Test
  def testInvalidAliasOnAtomicType(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // alias not allowed
    util.addTable('g as 'c)(atomicType)
        }
    }

  @Test
  def testGetFieldInfoPojoNames1(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // duplicate name
    util.addTable('name1, 'name1, 'name3)(pojoType)
        }
    }

  @Test
  def testGetFieldInfoAtomicName2(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // must be only one name
    util.addTable('name1, 'name2)(atomicType)
        }
    }

  @Test
  def testGetFieldInfoTupleAlias3(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // fields do not exist
    util.addTable('xxx as 'name1, 'yyy as 'name2, 'zzz as 'name3)(tupleType)
        }
    }

  @Test
  def testGetFieldInfoCClassAlias3(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // fields do not exist
    util.addTable('xxx as 'name1, 'yyy as 'name2, 'zzz as 'name3)(caseClassType)
        }
    }

  @Test
  def testGetFieldInfoPojoAlias3(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // fields do not exist
    util.addTable('xxx as 'name1, 'yyy as 'name2, 'zzz as 'name3)(pojoType)
        }
    }

  @Test
  def testGetFieldInfoGenericRowAlias(): Unit = {
        assertThrows[ValidationException] {
                val util = batchTestUtil()
    // unsupported generic row type
    util.addTable('first)(genericRowType)
        }
    }

  @Test
  def testRegisterExistingDataSet(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    tEnv.createTemporaryView("MyTable", ds1)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    // Must fail. Name is already in use.
    tEnv.createTemporaryView("MyTable", ds2)
        }
    }

  @Test
  def testScanUnregisteredTable(): Unit = {
        assertThrows[TableException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    // Must fail. No table registered under that name.
    tEnv.scan("someTable")
        }
    }

  @Test
  def testRegisterExistingTable(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", t1)
    val t2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv)
    // Must fail. Name is already in use.
    tEnv.createTemporaryView("MyTable", t2)
        }
    }

  @Test
  def testRegisterTableFromOtherEnv(): Unit = {
        assertThrows[TableException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = BatchTableEnvironment.create(env)
    val tEnv2 = BatchTableEnvironment.create(env)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv1)
    // Must fail. Table is bound to different TableEnvironment.
    tEnv2.registerTable("MyTable", t1)
        }
    }

  @Test
  def testToTableWithTooManyFields(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b, 'c, 'd)
        }
    }

  @Test
  def testToTableWithAmbiguousFields(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Field names not unique.
      .toTable(tEnv, 'a, 'b, 'b)
        }
    }

  @Test
  def testToTableWithNonFieldReference1(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a + 1, 'b, 'c)
        }
    }

  @Test
  def testToTableWithNonFieldReference2(): Unit = {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a as 'foo, 'b, 'c)
        }
    }

  @Test
  def testGenericRow() {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet)
        }
    }

  @Test
  def testGenericRowWithAlias() {
        assertThrows[ValidationException] {
                val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet, "nullField")
        }
    }
}
