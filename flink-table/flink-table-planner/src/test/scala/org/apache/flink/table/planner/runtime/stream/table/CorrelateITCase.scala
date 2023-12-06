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
package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.EachCallbackWrapper
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.expressions.utils.{Func18, FuncWithOpen, RichFunc2}
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils._
import org.apache.flink.table.utils.LegacyRowExtension
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.{ExtendWith, RegisterExtension}

import java.lang.{Boolean => JBoolean}

import scala.collection.mutable

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class CorrelateITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @RegisterExtension private val _: EachCallbackWrapper[LegacyRowExtension] =
    new EachCallbackWrapper[LegacyRowExtension](new LegacyRowExtension)

  @TestTemplate
  def testCrossJoin(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0
    val pojoFunc0 = new PojoTableFunc()

    val result = t
      .joinLateral(func0('c).as('d, 'e))
      .select('c, 'd, 'e)
      .joinLateral(pojoFunc0('c))
      .where('age > 20)
      .select('c, 'name, 'age)
      .toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "Anna#44,Anna,44")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testLeftOuterJoinWithoutPredicates(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0

    val result = t
      .leftOuterJoinLateral(func0('c).as('d, 'e))
      .select('c, 'd, 'e)

    val sink = new TestingAppendSink
    tEnv
      .toDataStream(result, DataTypes.ROW(DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()))
      .addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "nosharp,null,null",
      "Jack#22,Jack,22",
      "John#19,John,19",
      "Anna#44,Anna,44")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testLeftOuterJoinWithPredicates(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0

    assertThatExceptionOfType(classOf[ValidationException]).isThrownBy(
      () => {
        // Common join predicates are temporarily forbidden (see FLINK-7865).
        val result = t
          .leftOuterJoinLateral(func0('c).as('s, 'l), 'a === 'l)
          .select('c, 's, 'l)
          .toDataStream

        val sink = new TestingAppendSink
        result.addSink(sink)
        env.execute()

        val expected = "John#19,null,null\n" + "John#22,null,null\n" + "Anna44,null,null\n" +
          "nosharp,null,null"
        assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
      })
  }

  @TestTemplate
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0

    val result = t
      .joinLateral(func0('c).as('d, 'e))
      .where(Func18('d, "J"))
      .select('c, 'd, 'e)
      .toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Jack#22,Jack,22", "John#19,John,19")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    val tableFunc1 = new RichTableFunc1
    tEnv.createTemporarySystemFunction("RichTableFunc1", tableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> " "))

    val result = failingDataSource(smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .joinLateral(tableFunc1('c).as('s))
      .select('a, 's)

    val sink = new TestingAppendSink
    result.toDataStream.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("3,Hello", "3,world")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testUserDefinedTableFunctionWithUserDefinedScalarFunction(): Unit = {
    val tableFunc1 = new RichTableFunc1
    val richFunc2 = new RichFunc2
    tEnv.createTemporarySystemFunction("RichTableFunc1", tableFunc1)
    tEnv.createTemporarySystemFunction("RichFunc2", richFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(
      env,
      Map("word_separator" -> "#", "string.value" -> "test"))

    val result = failingDataSource(smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .joinLateral(tableFunc1(richFunc2('c)).as('s))
      .select('a, 's)

    val sink = new TestingAppendSink
    result.toDataStream.addSink(sink)
    env.execute()

    val expected =
      mutable.MutableList("1,Hi", "1,test", "2,Hello", "2,test", "3,Hello world", "3,test")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testTableFunctionConstructorWithParams(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val config = Map("key1" -> "value1", "key2" -> "value2")
    val func30 = new TableFunc3(null)
    val func31 = new TableFunc3("OneConf_")
    val func32 = new TableFunc3("TwoConf_", config)

    val result = t
      .joinLateral(func30('c).as('d, 'e))
      .select('c, 'd, 'e)
      .joinLateral(func31('c).as('f, 'g))
      .select('c, 'd, 'e, 'f, 'g)
      .joinLateral(func32('c).as('h, 'i))
      .select('c, 'd, 'f, 'h, 'e, 'g, 'i)
      .toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "Anna#44,Anna,OneConf_Anna,TwoConf__key=key1_value=value1_Anna,44,44,44",
      "Anna#44,Anna,OneConf_Anna,TwoConf__key=key2_value=value2_Anna,44,44,44",
      "Jack#22,Jack,OneConf_Jack,TwoConf__key=key1_value=value1_Jack,22,22,22",
      "Jack#22,Jack,OneConf_Jack,TwoConf__key=key2_value=value2_Jack,22,22,22",
      "John#19,John,OneConf_John,TwoConf__key=key1_value=value1_John,19,19,19",
      "John#19,John,OneConf_John,TwoConf__key=key2_value=value2_John,19,19,19"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testTableFunctionWithVariableArguments(): Unit = {
    val varArgsFunc0 = new VarArgsFunc0
    tEnv.createTemporarySystemFunction("VarArgsFunc0", varArgsFunc0)

    val result = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .select('c)
      .joinLateral(varArgsFunc0("1", "2", 'c))

    val sink = new TestingAppendSink
    result.toDataStream.addSink(sink)
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
      "nosharp,nosharp"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)

    val result1 = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .select('c)
      .joinLateral(varArgsFunc0("1", "2"))

    val sink1 = new TestingAppendSink
    result1.toDataStream.addSink(sink1)
    env.execute()

    val expected1 = mutable.MutableList(
      "Anna#44,1",
      "Anna#44,2",
      "Jack#22,1",
      "Jack#22,2",
      "John#19,1",
      "John#19,2",
      "nosharp,1",
      "nosharp,2")
    assertThat(sink1.getAppendResults.sorted).isEqualTo(expected1.sorted)

    // Test for empty cases
    val result2 = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .select('c)
      .joinLateral(varArgsFunc0())

    val sink2 = new TestingAppendSink
    result2.toDataStream.addSink(sink2)
    env.execute()
    assertThat(sink2.getAppendResults.isEmpty).isTrue()
  }

  @TestTemplate
  def testRowType(): Unit = {
    val row = Row.of(
      12.asInstanceOf[Integer],
      true.asInstanceOf[JBoolean],
      Row.of(1.asInstanceOf[Integer], 2.asInstanceOf[Integer], 3.asInstanceOf[Integer])
    )

    val rowType = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))
    val in = env.fromElements(row, row)(rowType).toTable(tEnv).as("a", "b", "c")

    val tableFunc = new TableFunc6()
    val result = in
      .joinLateral(tableFunc('c).as('f0, 'f1, 'f2))
      .select('c, 'f2)

    val sink = new TestingAppendSink
    result.toDataStream.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,2,3,3", "1,2,3,3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testTableFunctionCollectorOpenClose(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0
    val func26 = new FuncWithOpen
    tEnv.createTemporarySystemFunction("func26", func26)
    val result = t
      .joinLateral(func0('c).as('d, 'e))
      .where(func26('e))
      .select('c, 'd, 'e)
      .toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "Jack#22,Jack,22",
      "John#19,John,19",
      "Anna#44,Anna,44"
    )

    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testTableFunctionCollectorInit(): Unit = {
    val t = testData(env).toTable(tEnv).as("a", "b", "c")
    val func0 = new TableFunc0

    // this case will generate 'timestamp' member field and 'DateFormatter'
    val result = t
      .joinLateral(func0('c).as('d, 'e))
      .where(dateFormat(currentTimestamp(), "yyyyMMdd") === 'd)
      .select('c, 'd, 'e)
      .toDataStream

    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    assertThat(sink.getAppendResults.isEmpty).isTrue
  }

  @TestTemplate
  def testFlatMap(): Unit = {
    val ds = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
      // test non alias
      .flatMap(call(classOf[TableFunc2], 'c))
      .select('f0, 'f1)
      // test the output field name of flatMap is the same as the field name of the input table
      .flatMap(call(classOf[TableFunc2], concat('f0, "#")))
      .as("f0", "f1")
      .select('f0, 'f1)

    val sink = new TestingAppendSink
    ds.toDataStream.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Jack,4", "22,2", "John,4", "19,2", "Anna,4", "44,2")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testMultipleEvals(): Unit = {
    val rf = new RF
    val tf = new TableFunc7

    val row = Row.of(
      12.asInstanceOf[Integer],
      true.asInstanceOf[JBoolean],
      Row.of(1.asInstanceOf[Integer], 2.asInstanceOf[Integer], 3.asInstanceOf[Integer])
    )

    val rowType = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))
    val in = env.fromElements(row, row)(rowType).toTable(tEnv).as("a", "b", "c")
    val result = in.select(rf('a).as('d)).joinLateral(call(tf, 'd).as('e))

    val sink = new TestingAppendSink

    result.toDataStream.addSink(sink)
    env.execute()
  }

  private def testData(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }
}
