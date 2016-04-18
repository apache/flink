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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{TableException, Row}
import org.apache.flink.api.table.plan.TranslationContext
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class RegisterDataSetITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSimpleRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet(tableName, ds)
    val t = tEnv.scan(tableName).select('_1, '_2, '_3)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRegisterWithFields(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet(tableName, ds, 'a, 'b, 'c)
    val t = tEnv.scan(tableName).select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" +
      "7,4\n" + "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" +
      "15,5\n" + "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingDataSet(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds1)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds2)
  }

  @Test(expected = classOf[TableException])
  def testScanUnregisteredTable(): Unit = {
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    tEnv.scan("someTable")
  }

  @Test
  def testTableRegister(): Unit = {

    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
    tEnv.registerTable(tableName, t)

    val regT = tEnv.scan(tableName).select('a, 'b).filter('a > 8)

    val expected = "9,4\n" + "10,4\n" +
      "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" +
      "15,5\n" + "16,6\n" + "17,6\n" + "18,6\n" +
      "19,6\n" + "20,6\n" + "21,6\n"

    val results = regT.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable
    tEnv.registerTable("MyTable", t1)
    val t2 = CollectionDataSets.get5TupleDataSet(env).toTable
    tEnv.registerDataSet("MyTable", t2)
  }
}
