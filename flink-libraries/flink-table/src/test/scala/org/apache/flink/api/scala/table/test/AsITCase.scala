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
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.test.TableProgramsTestBase
import org.apache.flink.api.table.test.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class AsITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)

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
  def testAsFromCaseClass(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t =  env.fromCollection(data).as('a, 'b, 'c, 'd)

    val expected: String =
      "Peter,28,4000.0,Sales\n" +
      "Anna,56,10000.0,Engineering\n" +
      "Lucy,42,6000.0,HR\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAsFromAndToCaseClass(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t =  env.fromCollection(data).as('a, 'b, 'c, 'd)

    val expected: String =
      "SomeCaseClass(Peter,28,4000.0,Sales)\n" +
      "SomeCaseClass(Anna,56,10000.0,Engineering)\n" +
      "SomeCaseClass(Lucy,42,6000.0,HR)\n"
    val results = t.toDataSet[SomeCaseClass](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithToFewFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b)

    val expected = "no"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithToManyFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c, 'd)

    val expected = "no"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithAmbiguousFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'b)

    val expected = "no"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithNonFieldReference1(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // as can only have field references
    val t = CollectionDataSets.get3TupleDataSet(env).as('a + 1, 'b, 'b)

    val expected = "no"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithNonFieldReference2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // as can only have field references
    val t = CollectionDataSets.get3TupleDataSet(env).as('a as 'foo, 'b, 'b)

    val expected = "no"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}

case class SomeCaseClass(name: String, age: Int, salary: Double, department: String) {

  def this() { this("", 0, 0.0, "") }
}
