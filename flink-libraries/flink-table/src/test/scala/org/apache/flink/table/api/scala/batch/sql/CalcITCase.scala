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

package org.apache.flink.table.api.scala.batch.sql


import java.sql.{Date, Time, Timestamp}
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.batch.sql.FilterITCase.MyHashCode
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CalcITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSelectStarFromTable(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectStarFromDataSet(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, b, c FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectWithNaming(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT _1 as a, _2 as b FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, foo FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    tEnv.sql(sqlQuery)
  }

  @Test
  def testAllRejectingFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE false"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE true"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
      "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
      "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
      "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
      "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE c LIKE '%world%'"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnInteger(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE MOD(a,2)=0"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
      "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
      "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
      "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE a < 2 OR a > 20"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterWithAnd(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE MOD(a,2)<>0 AND MOD(b,2)=0"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "3,2,Hello world\n" + "7,4,Comment#1\n" +
      "9,4,Comment#3\n" + "17,6,Comment#11\n" +
      "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
      "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable"

    val ds = env.fromElements((
      Date.valueOf("1984-07-12"),
      Time.valueOf("14:34:24"),
      Timestamp.valueOf("1984-07-12 14:34:24")))
    tEnv.registerDataSet("MyTable", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1984-07-12,14:34:24,1984-07-12 14:34:24.0," +
      "1984-07-12,14:34:24,1984-07-12 14:34:24.0"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunction(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerFunction("hashCode",
      org.apache.flink.table.api.scala.batch.table.OldHashCode)
    tEnv.registerFunction("hashCode", MyHashCode)

    val ds = env.fromElements("a", "b", "c")
    tEnv.registerDataSet("MyTable", ds, 'text)

    val result = tEnv.sql("SELECT hashCode(text) FROM MyTable")

    val expected = "97\n98\n99"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

object FilterITCase {
  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }
}

object CalcITCase {

  @Parameterized.Parameters(name = "Execution mode = {0}, Table config = {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(TestExecutionMode.COLLECTION, TableProgramsTestBase.DEFAULT),
      Array(TestExecutionMode.COLLECTION, TableProgramsTestBase.NO_NULL)).asJava
  }
}
