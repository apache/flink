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
package org.apache.flink.table.runtime.dataset

import java.sql.{Date, Timestamp}

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.utils.UserDefinedTableFunctions.JavaTableFunc0
import org.apache.flink.table.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class DataSetCorrelateITCase(
  mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

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
      .select('c, 'name, 'age)
      .toDataSet[Row]

    val results = result.collect()
    val expected = "Jack#22,Jack,22\n" + "John#19,John,19\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFWithScalarFunction(): Unit = {
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
}
