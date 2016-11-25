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
package org.apache.flink.api.scala.batch.table


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{Row, TableEnvironment, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class InITCase(
                mode: TestExecutionMode,
                configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testInWithManyLiterals(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val simpleIn = ds1.select('a, 'b, 'c).where('c.in("Hi",
      "Hello",
      "Hello world",
      "Hello world, how are you?",
      "I am fine.",
      "Luke Skywalker",
      "Comment#1",
      "Comment#2",
      "Comment#3",
      "Comment#4",
      "Comment#5",
      "Comment#6",
      "Comment#7",
      "Comment#8",
      "Comment#9",
      "Comment#10",
      "Comment#11",
      "Comment#12",
      "Comment#13",
      "Comment#14"))
    val resultsSimple = simpleIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n" +
      "2,2,Hello\n" +
      "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" +
      "5,3,I am fine.\n" +
      "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" +
      "8,4,Comment#2\n" +
      "9,4,Comment#3\n" +
      "10,4,Comment#4\n" +
      "11,5,Comment#5\n" +
      "12,5,Comment#6\n" +
      "13,5,Comment#7\n" +
      "14,5,Comment#8\n" +
      "15,5,Comment#9\n" +
      "16,6,Comment#10\n" +
      "17,6,Comment#11\n" +
      "18,6,Comment#12\n" +
      "19,6,Comment#13\n" +
      "20,6,Comment#14\n"
    TestBaseUtils.compareResultAsText(resultsSimple.asJava, expected)

  }

  @Test
  def testInWithNumericLiterals(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val simpleIn = ds1.select('a, 'b, 'c).where('a.in(1, 3, 7))
    val resultsSimple = simpleIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n" + "3,2,Hello world\n" + "7,4,Comment#1\n"
    TestBaseUtils.compareResultAsText(resultsSimple.asJava, expected)
  }

  @Test
  def testInWithDifferentNumericTypeOperands(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val simpleIn = ds1
      .select('a, 'b, 'c)
      .where('a.in(BigDecimal(1.0), BigDecimal(2.00), BigDecimal(3.01)))
    val resultsSimple = simpleIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n" + "2,2,Hello\n"
    TestBaseUtils.compareResultAsText(resultsSimple.asJava, expected)
  }

  @Test
  def testInWithStringLiterals(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val simpleIn = ds1.select('a, 'b, 'c).where('c.in("Hi", "Hello world", "Comment#1"))
    val resultsSimple = simpleIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n" + "3,2,Hello world\n" + "7,4,Comment#1\n"
    TestBaseUtils.compareResultAsText(resultsSimple.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInWithNullOperand(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    ds1
      .select('a, 'b, 'c)
      .where('c.in("Hi", "Hello world", "Comment#1", null))
      .toDataSet[Row]
      .collect()
  }

  @Test(expected = classOf[ValidationException])
  def testInWithDifferentTypeRightOperands(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    ds1
      .select('a, 'b, 'c)
      .where('c.in("Hi", "Hello world", "Comment#1", 1))
      .toDataSet[Row]
      .collect()
  }

  @Test(expected = classOf[ValidationException])
  def testInWithDifferentTypeOperands(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    ds1.select('a, 'b, 'c)
      .where('a.in("Hi", "Hello world", "Comment#1")).toDataSet[Row].collect()
  }

}
