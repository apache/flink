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
package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.EachCallbackWrapper
import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.{Func1, Func18, FuncWithOpen, RichFunc2}
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, CollectionBatchExecTable, UserDefinedFunctionTestUtils}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.JavaTableFunc0
import org.apache.flink.table.planner.utils._
import org.apache.flink.table.utils.LegacyRowExtension
import org.apache.flink.test.util.TestBaseUtils

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._
import scala.collection.mutable

class CorrelateITCase extends BatchTestBase {

  @RegisterExtension private val _: EachCallbackWrapper[LegacyRowExtension] =
    new EachCallbackWrapper[LegacyRowExtension](new LegacyRowExtension)

  @Test
  def testCrossJoin(): Unit = {
    val in = testData.as("a", "b", "c")

    val func1 = new TableFunc1
    val result = in.joinLateral(func1('c).as('s)).select('c, 's)
    val results = executeQuery(result)
    val expected = "Jack#22,Jack\n" + "Jack#22,22\n" + "John#19,John\n" + "John#19,19\n" +
      "Anna#44,Anna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // with overloading
    val result2 = in.joinLateral(func1('c, "$").as('s)).select('c, 's)
    val results2 = executeQuery(result2)
    val expected2 = "Jack#22,$Jack\n" + "Jack#22,$22\n" + "John#19,$John\n" +
      "John#19,$19\n" + "Anna#44,$Anna\n" + "Anna#44,$44\n"
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)
  }

  @Test
  def testLeftOuterJoinWithoutPredicates(): Unit = {
    val in = testData.as("a", "b", "c")

    val func2 = new TableFunc2
    val result = in.leftOuterJoinLateral(func2('c).as('s, 'l)).select('c, 's, 'l)
    val results = executeQuery(result)
    val expected = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLeftOuterJoinWithSplit(): Unit = {
    tEnv.getConfig.setMaxGeneratedCodeLength(1) // split every field
    val in = testData.as("a", "b", "c")

    val func2 = new TableFunc2
    val result = in.leftOuterJoinLateral(func2('c).as('s, 'l)).select('c, 's, 'l)
    val results = executeQuery(result)
    val expected = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  /** Common join predicates are temporarily forbidden (see FLINK-7865). */
  @Test
  def testLeftOuterJoinWithPredicates(): Unit = {
    val in = testData.as("a", "b", "c")

    val func2 = new TableFunc2
    assertThatThrownBy(
      () => {
        val result = in
          .leftOuterJoinLateral(func2('c).as('s, 'l), 'a === 'l)
          .select('c, 's, 'l)
        val results = executeQuery(result)
        val expected = "John#19,19,2\n" + "nosharp,null,null"
        TestBaseUtils.compareResultAsText(results.asJava, expected)
      })
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  def testWithFilter(): Unit = {
    val in = testData.as("a", "b", "c")
    val func0 = new TableFunc0

    val result = in
      .joinLateral(func0('c).as('name, 'age))
      .select('c, 'name, 'age)
      .filter('age > 20)

    val results = executeQuery(result)
    val expected = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCustomReturnType(): Unit = {
    val in = testData.as("a", "b", "c")
    val func2 = new TableFunc2

    val result = in
      .joinLateral(func2('c).as('name, 'len))
      .select('c, 'name, 'len)

    val results = executeQuery(result)
    val expected = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testHierarchyType(): Unit = {
    val in = testData.as("a", "b", "c")

    val hierarchy = new HierarchyTableFunction
    val result = in
      .joinLateral(hierarchy('c).as('name, 'adult, 'len))
      .select('c, 'name, 'adult, 'len)

    val results = executeQuery(result)
    val expected = "Jack#22,Jack,true,22\n" + "John#19,John,false,19\n" +
      "Anna#44,Anna,true,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPojoType(): Unit = {
    val in = testData.as("a", "b", "c")

    val pojo = new PojoTableFunc()
    val result = in
      .joinLateral(pojo('c))
      .where('age > 20)
      .select('c, 'name, 'age)

    val results = executeQuery(result)
    val expected = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val in = testData.as("a", "b", "c")
    val func1 = new TableFunc1

    val result = in
      .joinLateral(func1('c.substring(2)).as('s))
      .select('c, 's)

    val results = executeQuery(result)
    val expected = "Jack#22,ack\n" + "Jack#22,22\n" + "John#19,ohn\n" + "John#19,19\n" +
      "Anna#44,nna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionInCondition(): Unit = {
    val in = testData.as("a", "b", "c")
    val func0 = new TableFunc0

    val result = in
      .joinLateral(func0('c))
      .where(Func18('name, "J") && (Func1('a) < 3) && Func1('age) > 20)
      .select('c, 'name, 'age)

    val results = executeQuery(result)
    val expected = "Jack#22,Jack,22"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLongAndTemporalTypes(): Unit = {
    val in = testData.as("a", "b", "c")
    val func0 = new JavaTableFunc0

    val result = in
      .where('a === 1)
      .select(
        Date.valueOf("1990-10-14").as('x),
        1000L.as('y),
        Timestamp.valueOf("1990-10-14 12:10:10").as('z))
      .joinLateral(func0('x, 'y, 'z).as('s))
      .select('s)

    val results = executeQuery(result)
    val expected = "1000\n" + "655906210000\n" + "7591\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testByteShortFloatArguments(): Unit = {
    val in = testData.as("a", "b", "c")
    val tFunc = new TableFunc4

    val result = in
      .select(
        'a.cast(DataTypes.TINYINT).as('a),
        'a.cast(DataTypes.SMALLINT).as('b),
        'b.cast(DataTypes.FLOAT).as('c))
      .joinLateral(
        tFunc('a.ifNull(0.toByte), 'b.ifNull(0.toShort), 'c.ifNull(0.toFloat)).as('a2, 'b2, 'c2))

    val results = executeQuery(result)
    val expected = Seq(
      "1,1,1.0,Byte=1,Short=1,Float=1.0",
      "2,2,2.0,Byte=2,Short=2,Float=2.0",
      "3,3,2.0,Byte=3,Short=3,Float=2.0",
      "4,4,3.0,Byte=4,Short=4,Float=3.0").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    val richTableFunc1 = new RichTableFunc1
    registerFunction("RichTableFunc1", richTableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> "#"))

    val result = testData
      .joinLateral(richTableFunc1('c).as('s))
      .select('a, 's)

    val expected = "1,Jack\n" + "1,22\n" + "2,John\n" + "2,19\n" + "3,Anna\n" + "3,44"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionWithParameters(): Unit = {
    val richTableFunc1 = new RichTableFunc1
    registerFunction("RichTableFunc1", richTableFunc1)
    val richFunc2 = new RichFunc2
    registerFunction("RichFunc2", richFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(
      env,
      Map("word_separator" -> "#", "string.value" -> "test"))

    val result = CollectionBatchExecTable
      .getSmall3TupleDataSet(tEnv, "a, b, c")
      .joinLateral(richTableFunc1(richFunc2('c)).as('s))
      .select('a, 's)

    val expected = "1,Hi\n1,test\n2,Hello\n2,test\n3,Hello world\n3,test"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableFunctionConstructorWithParams(): Unit = {
    val in = testData.as("a", "b", "c")
    val func30 = new TableFunc3(null)
    val func31 = new TableFunc3("OneConf_")
    val func32 = new TableFunc3("TwoConf_")

    val result = in
      .joinLateral(func30('c).as('d, 'e))
      .select('c, 'd, 'e)
      .joinLateral(func31('c).as('f, 'g))
      .select('c, 'd, 'e, 'f, 'g)
      .joinLateral(func32('c).as('h, 'i))
      .select('c, 'd, 'f, 'h, 'e, 'g, 'i)

    val results = executeQuery(result)

    val expected = "Anna#44,Anna,OneConf_Anna,TwoConf_Anna,44,44,44\n" +
      "Jack#22,Jack,OneConf_Jack,TwoConf_Jack,22,22,22\n" +
      "John#19,John,OneConf_John,TwoConf_John,19,19,19\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableFunctionWithVariableArguments(): Unit = {
    val varArgsFunc0 = new VarArgsFunc0
    registerFunction("VarArgsFunc0", varArgsFunc0)

    val result = testData
      .select('c)
      .joinLateral(varArgsFunc0("1", "2", 'c))

    val expected = "Anna#44,1\n" +
      "Anna#44,2\n" +
      "Anna#44,Anna#44\n" +
      "Jack#22,1\n" +
      "Jack#22,2\n" +
      "Jack#22,Jack#22\n" +
      "John#19,1\n" +
      "John#19,2\n" +
      "John#19,John#19\n" +
      "nosharp,1\n" +
      "nosharp,2\n" +
      "nosharp,nosharp"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
    //
    //    // Test for empty cases
    val result0 = testData
      .select('c)
      .joinLateral(varArgsFunc0())
    val results0 = executeQuery(result0)
    assertThat(results0.isEmpty).isTrue
  }

  @Test
  def testCountStarOnCorrelate(): Unit = {
    val in = testData.as("a", "b", "c")
    val func0 = new TableFunc0

    val result = in
      .joinLateral(func0('c).as('name, 'age))
      .select(0.count)

    val results = executeQuery(result)
    val expected = "3"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCountStarOnLeftCorrelate(): Unit = {
    val in = testData.as("a", "b", "c")
    val func0 = new TableFunc0

    val result = in
      .leftOuterJoinLateral(func0('c).as('name, 'age))
      .select(0.count)

    val results = executeQuery(result)
    val expected = "4"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableFunctionCollectorOpenClose(): Unit = {
    val t = testData.as("a", "b", "c")
    val func0 = new TableFunc0
    val func = new FuncWithOpen

    val result = t
      .joinLateral(func0('c).as('d, 'e))
      .where(func('e))
      .select('c, 'd, 'e)

    val results = executeQuery(result)

    val expected = Seq(
      "Jack#22,Jack,22",
      "John#19,John,19",
      "Anna#44,Anna,44"
    )

    TestBaseUtils.compareResultAsText(
      results.asJava,
      expected.sorted.mkString("\n")
    )
  }

  private def testData: Table = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c")
  }
}
