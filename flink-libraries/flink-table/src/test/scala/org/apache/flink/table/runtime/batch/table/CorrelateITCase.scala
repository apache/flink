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

package org.apache.flink.table.runtime.batch.table

import java.sql.{Date, Timestamp}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.utils.JavaUserDefinedTableFunctions.JavaTableFunc0
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{Func1, Func13, Func18, RichFunc2}
import org.apache.flink.table.runtime.utils.TableProgramsClusterTestBase
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class CorrelateITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
    extends TableProgramsClusterTestBase(mode, configMode) {

  @Test
  def testCrossJoin(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)

    val func1 = new TableFunc1
    val result = in.join(func1('c) as 's).select('c, 's).toDataSet[Row]
    val results = result.collect()
    val expected = "Jack#22,Jack\n" + "Jack#22,22\n" + "John#19,John\n" + "John#19,19\n" +
      "Anna#44,Anna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // with overloading
    val result2 = in.join(func1('c, "$") as 's).select('c, 's).toDataSet[Row]
    val results2 = result2.collect()
    val expected2 = "Jack#22,$Jack\n" + "Jack#22,$22\n" + "John#19,$John\n" +
      "John#19,$19\n" + "Anna#44,$Anna\n" + "Anna#44,$44\n"
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)

    val in2 = testData2(env).toTable(tableEnv).as('a, 'b)
    // test implicitly converts
    val result3 = in2.join(func1('a) as 'num).select('b, 'num).toDataSet[Row]
    val results3 = result3.collect()

    val expected3 = "1,num_0\n" + "2,num_0\n" + "2,num_1\n" +
      "3,num_0\n" + "3,num_1\n" + "3,num_2\n"
    TestBaseUtils.compareResultAsText(results3.asJava, expected3)

  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)

    val func2 = new TableFunc2
    val result = in.leftOuterJoin(func2('c) as ('s, 'l)).select('c, 's, 'l).toDataSet[Row]
    val results = result.collect()
    val expected = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testWithFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = in
      .join(func0('c) as ('name, 'age))
      .select('c, 'name, 'age)
      .filter('age > 20)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCustomReturnType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func2 = new TableFunc2

    val result = in
      .join(func2('c) as ('name, 'len))
      .select('c, 'name, 'len)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testHierarchyType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)

    val hierarchy = new HierarchyTableFunction
    val result = in
      .join(hierarchy('c) as ('name, 'adult, 'len))
      .select('c, 'name, 'adult, 'len)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,true,22\n" + "John#19,John,false,19\n" +
      "Anna#44,Anna,true,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPojoType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)

    val pojo = new PojoTableFunc()
    val result = in
      .join(pojo('c))
      .where('age > 20)
      .select('c, 'name, 'age)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunction(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func1 = new TableFunc1

    val result = in
      .join(func1('c.substring(2)) as 's)
      .select('c, 's)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,ack\n" + "Jack#22,22\n" + "John#19,ohn\n" + "John#19,19\n" +
      "Anna#44,nna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionInCondition(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = in
      .join(func0('c))
      .where(Func18('name, "J") && (Func1('a) < 3) && Func1('age) > 20)
      .select('c, 'name, 'age)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,22"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testLongAndTemporalTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new JavaTableFunc0

    val result = in
        .where('a === 1)
        .select(Date.valueOf("1990-10-14") as 'x,
                1000L as 'y,
                Timestamp.valueOf("1990-10-14 12:10:10") as 'z)
        .join(func0('x, 'y, 'z) as 's)
        .select('s)
        .toDataSet[Row]

    val results = result.collect()
    val expected = "1000\n" + "655906210000\n" + "7591\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val richTableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", richTableFunc1)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("word_separator" -> "#"))

    val result = testData(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(richTableFunc1('c) as 's)
      .select('a, 's)

    val expected = "1,Jack\n" + "1,22\n" + "2,John\n" + "2,19\n" + "3,Anna\n" + "3,44"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedTableFunctionWithScalarFunctionWithParameters(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val richTableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", richTableFunc1)
    val richFunc2 = new RichFunc2
    tEnv.registerFunction("RichFunc2", richFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(
      env,
      Map("word_separator" -> "#", "string.value" -> "test"))

    val result = CollectionDataSets.getSmall3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(richTableFunc1(richFunc2('c)) as 's)
      .select('a, 's)

    val expected = "1,Hi\n1,test\n2,Hello\n2,test\n3,Hello world\n3,test"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableFunctionConstructorWithParams(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func30 = new TableFunc3(null)
    val func31 = new TableFunc3("OneConf_")
    val func32 = new TableFunc3("TwoConf_")

    val result = in
      .join(func30('c) as('d, 'e))
      .select('c, 'd, 'e)
      .join(func31('c) as ('f, 'g))
      .select('c, 'd, 'e, 'f, 'g)
      .join(func32('c) as ('h, 'i))
      .select('c, 'd, 'f, 'h, 'e, 'g, 'i)
      .toDataSet[Row]

    val results = result.collect()

    val expected = "Anna#44,Anna,OneConf_Anna,TwoConf_Anna,44,44,44\n" +
      "Jack#22,Jack,OneConf_Jack,TwoConf_Jack,22,22,22\n" +
      "John#19,John,OneConf_John,TwoConf_John,19,19,19\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableFunctionWithVariableArguments(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val varArgsFunc0 = new VarArgsFunc0
    tableEnv.registerFunction("VarArgsFunc0", varArgsFunc0)

    val result = testData(env)
      .toTable(tableEnv, 'a, 'b, 'c)
      .select('c)
      .join(varArgsFunc0("1", "2", 'c))

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
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // Test for empty cases
    val result0 = testData(env)
      .toTable(tableEnv, 'a, 'b, 'c)
      .select('c)
      .join(varArgsFunc0())
    val results0 = result0.toDataSet[Row].collect()
    assertTrue(results0.isEmpty)
  }

  private def testData(
      env: ExecutionEnvironment)
    : DataSet[(Int, Long, String)] = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }

  private def testData2(
      env: ExecutionEnvironment)
  : DataSet[(Byte, Int)] = {
    val data = new mutable.MutableList[(Byte, Int)]
    data.+=((1.toByte, 1))
    data.+=((2.toByte, 2))
    data.+=((3.toByte, 3))
    env.fromCollection(data)
  }
}
