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

package org.apache.flink.table.runtime.batch.table

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.runtime.utils.{TableProgramsCollectionTestBase, TableProgramsTestBase}
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableEnvironmentITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testSimpleRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.createTemporaryView(tableName, ds)
    val t = tEnv.scan(tableName).select('_1, '_2, '_3)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRegisterWithFieldsByPosition(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.createTemporaryView(tableName, ds, 'a, 'b, 'c) // new alias
    val t = tEnv.scan(tableName).select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" +
      "7,4\n" + "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" +
      "15,5\n" + "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRegisterWithFieldsByName(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.createTemporaryView(tableName, ds, '_3, '_1, '_2) // new order
    val t = tEnv.scan(tableName).select('_1, '_2)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" +
      "7,4\n" + "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" +
      "15,5\n" + "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable(tableName, t)

    val regT = tEnv.scan(tableName).select('a, 'b).filter('a > 8)

    val expected = "9,4\n" + "10,4\n" +
      "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" +
      "15,5\n" + "16,6\n" + "17,6\n" + "18,6\n" +
      "19,6\n" + "20,6\n" + "21,6\n"

    val results = regT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testToTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testToTableFromCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t =  env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c, 'd)
      .select('a, 'b, 'c, 'd)

    val expected: String =
      "Peter,28,4000.0,Sales\n" +
      "Anna,56,10000.0,Engineering\n" +
      "Lucy,42,6000.0,HR\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testToTableFromAndToCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t =  env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c, 'd)
      .select('a, 'b, 'c, 'd)

    val expected: String =
      "SomeCaseClass(Peter,28,4000.0,Sales)\n" +
      "SomeCaseClass(Anna,56,10000.0,Engineering)\n" +
      "SomeCaseClass(Lucy,42,6000.0,HR)\n"
    val results = t.toDataSet[SomeCaseClass].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testInsertIntoMemoryTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    MemoryTableSourceSinkUtil.clear()

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    tEnv.scan("sourceTable")
      .select('a, 'b, 'c)
      .insertInto("targetTable")
    tEnv.execute("job name")

    val expected = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, MemoryTableSourceSinkUtil.tableDataStrings.sorted)
  }
}

object TableEnvironmentITCase {

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
