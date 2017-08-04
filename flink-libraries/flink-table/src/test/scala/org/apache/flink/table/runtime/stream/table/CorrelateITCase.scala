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
package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{Func18, RichFunc2}
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.utils._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable

class CorrelateITCase extends StreamingMultipleProgramsTestBase {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

  @Before
  def clear(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testCrossJoin(): Unit = {
    val t = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0
    val pojoFunc0 = new PojoTableFunc()

    val result = t
      .join(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .join(pojoFunc0('c))
      .where('age > 20)
      .select('c, 'name, 'age)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val t = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .leftOuterJoin(func0('c) as('d, 'e))
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null", "Jack#22,Jack,22",
      "John#19,John,19", "Anna#44,Anna,44")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val t = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .join(func0('c) as('d, 'e))
      .where(Func18('d, "J"))
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "John#19,John,19")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    val tableFunc1 = new RichTableFunc1
    tableEnv.registerFunction("RichTableFunc1", tableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> " "))
    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tableEnv, 'a, 'b, 'c)
      .join(tableFunc1('c) as 's)
      .select('a, 's)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("3,Hello", "3,world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCrossJoinImplicitlyConverts(): Unit = {

    val in = testData2(env).toTable(tableEnv).as('a, 'b)
    val func1 = new TableFunc1

    // test implicitly converts
    val result = in
      .join(func1('a) as 'num)
      .select('b, 'num)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,num_0", "2,num_0", "2,num_1",
      "3,num_0", "3,num_1", "3,num_2")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithUserDefinedScalarFunction(): Unit = {
    val tableFunc1 = new RichTableFunc1
    val richFunc2 = new RichFunc2
    tableEnv.registerFunction("RichTableFunc1", tableFunc1)
    tableEnv.registerFunction("RichFunc2", richFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(
      env,
      Map("word_separator" -> "#", "string.value" -> "test"))
    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tableEnv, 'a, 'b, 'c)
      .join(tableFunc1(richFunc2('c)) as 's)
      .select('a, 's)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
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

  @Test
  def testTableFunctionConstructorWithParams(): Unit = {
    val t = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val config = Map("key1" -> "value1", "key2" -> "value2")
    val func30 = new TableFunc3(null)
    val func31 = new TableFunc3("OneConf_")
    val func32 = new TableFunc3("TwoConf_", config)

    val result = t
      .join(func30('c) as('d, 'e))
      .select('c, 'd, 'e)
      .join(func31('c) as ('f, 'g))
      .select('c, 'd, 'e, 'f, 'g)
      .join(func32('c) as ('h, 'i))
      .select('c, 'd, 'f, 'h, 'e, 'g, 'i)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "Anna#44,Anna,OneConf_Anna,TwoConf__key=key1_value=value1_Anna,44,44,44",
      "Anna#44,Anna,OneConf_Anna,TwoConf__key=key2_value=value2_Anna,44,44,44",
      "Jack#22,Jack,OneConf_Jack,TwoConf__key=key1_value=value1_Jack,22,22,22",
      "Jack#22,Jack,OneConf_Jack,TwoConf__key=key2_value=value2_Jack,22,22,22",
      "John#19,John,OneConf_John,TwoConf__key=key1_value=value1_John,19,19,19",
      "John#19,John,OneConf_John,TwoConf__key=key2_value=value2_John,19,19,19"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableFunctionWithVariableArguments(): Unit = {
    val varArgsFunc0 = new VarArgsFunc0
    tableEnv.registerFunction("VarArgsFunc0", varArgsFunc0)

    val result = testData(env)
      .toTable(tableEnv, 'a, 'b, 'c)
      .select('c)
      .join(varArgsFunc0("1", "2", 'c))

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "Anna#44,1",
      "Anna#44,2",
      "Anna#44,Anna#44",
      "Jack#22,1",
      "Jack#22,2",
      "Jack#22,Jack#22",
      "John#19,1",
      "John#19,2",
      "John#19,John#19",
      "nosharp,1",
      "nosharp,2",
      "nosharp,nosharp")
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

  private def testData2(
      env: StreamExecutionEnvironment)
  : DataStream[(Byte, Int)] = {

    val data = new mutable.MutableList[(Byte, Int)]
    data.+=((1.toByte, 1))
    data.+=((2.toByte, 2))
    data.+=((3.toByte, 3))
    env.fromCollection(data)
  }

}
