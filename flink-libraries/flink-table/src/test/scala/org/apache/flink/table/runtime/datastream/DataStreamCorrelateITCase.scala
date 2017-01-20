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
package org.apache.flink.table.runtime.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.expressions.utils.RichFunc2
import org.apache.flink.table.utils.{RichTableFunc1, TableFunc0, UserDefinedFunctionTestUtils}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DataStreamCorrelateITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .join(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .leftOuterJoin(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toDataStream[Row]

    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null", "Jack#22,Jack,22",
      "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> " "))
    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc1('c) as 's)
      .select('a, 's)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,Hello", "3,world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithUserDefinedScalarFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc1 = new RichTableFunc1
    val richFunc2 = new RichFunc2
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    tEnv.registerFunction("RichFunc2", richFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(
      env,
      Map("word_separator" -> "#", "string.value" -> "test"))
    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc1(richFunc2('c)) as 's)
      .select('a, 's)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hi",
      "1,test",
      "2,Hello",
      "2,test",
      "3,Hello world",
      "3,test")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  private def testData(
      env: StreamExecutionEnvironment)
    : DataStream[(Int, Long, String)] = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }

}
