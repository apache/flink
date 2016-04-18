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
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SelectITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable.select('_1, '_2, '_3)

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
  def testSimpleSelectAllWithAs(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c).select('a, 'b, 'c)

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
  def testSimpleSelectWithNaming(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSelectInvalidFieldFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      // must fail. Field 'foo does not exist
      .select('a, 'foo)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSelectAmbiguousRenaming(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'foo
      .select('a + 1 as 'foo, 'b + 2 as 'foo).toDataSet[Row].print()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSelectAmbiguousRenaming2(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'a
      .select('a, 'b as 'a).toDataSet[Row].print()
  }

}
