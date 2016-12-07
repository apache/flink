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
package org.apache.flink.api.scala.batch

import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils._
import org.apache.flink.api.table.{Row, Table, TableEnvironment}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class UserDefinedTableFunctionITCase(
  mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSQLCrossApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", new TableFunc1)

    val sqlQuery = "SELECT MyTable.c, t.s FROM MyTable, LATERAL TABLE(split(c)) AS t(s)"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack\n" + "Jack#22,22\n" + "John#19,John\n" + "John#19,19\n" +
      "Anna#44,Anna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSQLOuterApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", new TableFunc2)

    val sqlQuery = "SELECT MyTable.c, t.a, t.b  FROM MyTable LEFT JOIN LATERAL TABLE(split(c)) " +
      "AS t(a,b) ON TRUE"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableAPICrossApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)

    val func1 = new TableFunc1
    val result = in.crossApply(func1('c) as ('s)).select('c, 's).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack\n" + "Jack#22,22\n" + "John#19,John\n" + "John#19,19\n" +
      "Anna#44,Anna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // with overloading
    val result2 = in.crossApply(func1('c, "$") as ('s)).select('c, 's).toDataSet[Row]
    val results2 = result2.collect()
    val expected2: String = "Jack#22,$Jack\n" + "Jack#22,$22\n" + "John#19,$John\n" +
      "John#19,$19\n" + "Anna#44,$Anna\n" + "Anna#44,$44\n"
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)
  }


  @Test
  def testTableAPIOuterApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    val func2 = new TableFunc2
    val result = in.outerApply(func2('c) as ('s, 'l)).select('c, 's, 'l).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testCustomReturnType(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    val func2 = new TableFunc2

    val result = in
      .crossApply(func2('c) as ('name, 'len))
      .select('c, 'name, 'len)
      .toDataSet[Row]

    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testHierarchyType(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)

    val hierarchy = new HierarchyTableFunction
    val result = in
      .crossApply(hierarchy('c) as ('name, 'adult, 'len))
      .select('c, 'name, 'adult, 'len)
      .toDataSet[Row]

    val results = result.collect()
    val expected: String = "Jack#22,Jack,true,22\n" + "John#19,John,false,19\n" +
      "Anna#44,Anna,true,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPojoType(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)

    val pojo = new PojoTableFunc()
    val result = in
      .crossApply(pojo('c))
      .select('c, 'name, 'age)
      .toDataSet[Row]

    val results = result.collect()
    val expected: String = "Jack#22,Jack,22\n" + "John#19,John,19\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testTableAPIWithFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    val func0 = new TableFunc0

    val result = in
      .crossApply(func0('c) as ('name, 'age))
      .select('c, 'name, 'age)
      .filter('age > 20)
      .toDataSet[Row]

    val results = result.collect()
    val expected: String = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testUDTFWithScalarFunction(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    val func1 = new TableFunc1

    val result = in
      .crossApply(func1('c.substring(2)) as 's)
      .select('c, 's)
      .toDataSet[Row]

    val results = result.collect()
    val expected: String = "Jack#22,ack\n" + "Jack#22,22\n" + "John#19,ohn\n" + "John#19,19\n" +
      "Anna#44,nna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  private def getSmall3TupleDataSet(env: ExecutionEnvironment): DataSet[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }
}
