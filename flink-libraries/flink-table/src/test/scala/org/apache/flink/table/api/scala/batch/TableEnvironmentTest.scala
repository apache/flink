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

package org.apache.flink.table.api.scala.batch

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.{TableConfig, TableEnvironment, TableException}
import org.apache.flink.table.utils.BatchTableTestUtil
import org.apache.flink.table.utils.TableTestUtil._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableEnvironmentTest(configMode: TableConfigMode) {

  def config: TableConfig = {
    val conf = new TableConfig
    configMode match {
      case TableProgramsTestBase.NO_NULL =>
        conf.setNullCheck(false)
      case _ => // keep default
    }
    conf
  }

  val utils = BatchTableTestUtil()

  @Test
  def testSimpleRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet(tableName, ds)
    val t = tEnv.scan(tableName).select('_1, '_2, '_3)

    val expected = values(
      "DataSetScan",
      term("table", "[MyTable]")
    )

    utils.verifyTable(t, expected, tEnv)
  }

  @Test
  def testRegisterWithFields(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet(tableName, ds, 'a, 'b, 'c)
    val t = tEnv.scan(tableName).select('a, 'b)

    val expected = unaryNode(
      "DataSetCalc",
      values("DataSetScan", term("table", "[MyTable]")),
      term("select", "a", "b")
    )

    utils.verifyTable(t, expected, tEnv)
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

  @Test
  def testTableRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable(tableName, t)

    val regT = tEnv.scan(tableName).select('a, 'b).filter('a > 8)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b"),
      term("where", ">(a, 8)")
    )

    utils.verifyTable(regT, expected, tEnv)
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

  @Test
  def testToTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val expected = batchTableNode(0)

    utils.verifyTable(t, expected, tEnv)
  }

  @Test
  def testToTableFromCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t =  env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c, 'd)
      .select('a, 'b, 'c, 'd)

    val expected = batchTableNode(0)


    utils.verifyTable(t, expected, tEnv)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithToFewFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b)
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
}

object TableEnvironmentTest {

  @Parameterized.Parameters(name = "Table config = {0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(TableProgramsTestBase.DEFAULT)
    ).asJava
  }
}

case class SomeCaseClass(name: String, age: Int, salary: Double, department: String) {
  def this() { this("", 0, 0.0, "") }
}
