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

import java.lang.{Boolean => JBoolean}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types, ValidationException}
import org.apache.flink.table.expressions.utils.{Func18, Func20, RichFunc2}
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, _}
import org.apache.flink.table.utils._
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable

class CorrelateITCase extends AbstractTestBase {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

  @Before
  def clear(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testCrossJoin(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
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
  def testLeftOuterJoinWithoutPredicates(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
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

  /**
    * Common join predicates are temporarily forbidden (see FLINK-7865).
    */
  @Test (expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = t
      .leftOuterJoin(func0('c) as ('s, 'l), 'a === 'l)
      .select('c, 's, 'l)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = "John#19,null,null\n" + "John#22,null,null\n" + "Anna44,null,null\n" +
      "nosharp,null,null"
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
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
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> " "))
    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.getSmall3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc1('c) as 's)
      .select('a, 's)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("3,Hello", "3,world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedTableFunctionWithUserDefinedScalarFunction(): Unit = {
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
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
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
    tEnv.registerFunction("VarArgsFunc0", varArgsFunc0)

    val result = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
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

  @Test
  def testRowType(): Unit = {
    val row = Row.of(
      12.asInstanceOf[Integer],
      true.asInstanceOf[JBoolean],
      Row.of(1.asInstanceOf[Integer], 2.asInstanceOf[Integer], 3.asInstanceOf[Integer])
    )

    val rowType = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))
    val in = env.fromElements(row, row)(rowType).toTable(tEnv).as('a, 'b, 'c)

    val tableFunc5 = new TableFunc5()
    val result = in
      .join(tableFunc5('c) as ('f0, 'f1, 'f2))
      .select('c, 'f2)

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,2,3,3",
      "1,2,3,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableFunctionCollectorOpenClose(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0
    val func20 = new Func20

    val result = t
      .join(func0('c) as('d, 'e))
      .where(func20('e))
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq (
      "Jack#22,Jack,22",
      "John#19,John,19",
      "Anna#44,Anna,44"
    )

    assertEquals(
      expected.sorted,
      StreamITCase.testResults.sorted
    )
  }

  @Test
  def testTableFunctionCollectorInit(): Unit = {
    val t = testData(env).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    // this case will generate 'timestamp' member field and 'DateFormatter'
    val result = t
      .join(func0('c) as('d, 'e))
      .where(dateFormat(currentTimestamp(), "yyyyMMdd") === 'd)
      .select('c, 'd, 'e)
      .toAppendStream[Row]

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(
      Seq(),
      StreamITCase.testResults.sorted
    )
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
