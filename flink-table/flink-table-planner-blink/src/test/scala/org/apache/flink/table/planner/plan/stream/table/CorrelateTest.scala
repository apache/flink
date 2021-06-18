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
package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.Func13
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.{JavaTableFuncTuple12, NonDeterministicTableFunc, StringSplit}
import org.apache.flink.table.planner.utils._

import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.Test

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val function = new TableFunc1
    util.addFunction("func1", function)

    val result1 = table.joinLateral(function('c) as 's).select('c, 's)
    util.verifyExecPlan(result1)
  }

  @Test
  def testCrossJoin2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val function = new TableFunc1
    util.addFunction("func1", function)
    // test overloading
    val result2 = table.joinLateral(function('c, "$") as 's).select('c, 's)
    util.verifyExecPlan(result2)
  }

  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.leftOuterJoinLateral(function('c) as 's, true).select('c, 's)
    util.verifyExecPlan(result)
  }

  @Test
  def testCustomType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    util.addFunction("func2", function)
    val scalarFunc = new Func13("pre")

    val result = table.joinLateral(
      function(scalarFunc('c)) as ('name, 'len)).select('c, 'name, 'len)

    util.verifyExecPlan(result)
  }

  @Test
  def testHierarchyType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val result = table.joinLateral(function('c) as ('name, 'adult, 'len))
    util.verifyExecPlan(result)
  }

  @Test
  def testPojoType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val result = table.joinLateral(function('c))
    util.verifyExecPlan(result)
  }

  @Test
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    util.addFunction("func2", function)

    val result = table
      .joinLateral(function('c) as ('name, 'len))
      .select('c, 'name, 'len)
      .filter('len > 2)
    util.verifyExecPlan(result)
  }

  @Test
  def testScalarFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.joinLateral(function('c.substring(2)) as 's)
    util.verifyExecPlan(result)
  }

  @Test
  def testCorrelateWithMultiFilter(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc0
    util.addFunction("func1", function)

    val result = sourceTable.select('a, 'b, 'c)
      .joinLateral(function('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    util.verifyExecPlan(result)
  }

  @Test
  def testCorrelateWithMultiFilterAndWithoutCalcMergeRules(): Unit = {
    val util = streamTestUtil()
    val programs = util.getStreamProgram()
    programs.getFlinkRuleSetProgram(FlinkStreamProgram.LOGICAL)
      .get.remove(
      RuleSets.ofList(
        CoreRules.CALC_MERGE,
        CoreRules.FILTER_CALC_MERGE,
        CoreRules.PROJECT_CALC_MERGE))
    // removing
    util.replaceStreamProgram(programs)

    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc0
    util.addFunction("func1", function)
    val result = sourceTable.select('a, 'b, 'c)
      .joinLateral(function('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    util.verifyExecPlan(result)
  }

  @Test
  def testFlatMap(): Unit = {
    val util = streamTestUtil()

    val func2 = new TableFunc2
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'f1, 'f2, 'f3)
    val resultTable = sourceTable
      .flatMap(func2('f3))
    util.verifyExecPlan(resultTable)
  }

  @Test
  def testCorrelatePythonTableFunction(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
    val func = new MockPythonTableFunction
    val result = sourceTable.joinLateral(func('a, 'b) as('x, 'y))

    util.verifyExecPlan(result)
  }

  @Test
  def testCorrelateTuple12(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new JavaTableFuncTuple12
    util.addTemporarySystemFunction("func1", function)
    val sql =
      """
        |SELECT *
        |FROM MyTable, LATERAL TABLE(func1(c)) AS T
        |""".stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinConstantFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("str_split", new StringSplit())
    val sql =
      """
        |SELECT *
        |FROM MyTable, LATERAL TABLE(str_split('Jack,John', ',')) AS T
        |""".stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftJoinConstantFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("str_split", new StringSplit())
    val sql =
      """
        |SELECT *
        |FROM MyTable Left JOIN LATERAL TABLE(str_split('Jack,John', ',')) AS T ON TRUE
        |""".stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinConstantNonDeterministicFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("str_split", new NonDeterministicTableFunc())
    val sql =
      """
        |SELECT *
        |FROM MyTable, LATERAL TABLE(str_split('Jack,John')) AS T
        |""".stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftJoinConstantNonDeterministicFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("str_split", new NonDeterministicTableFunc())
    val sql =
      """
        |SELECT *
        |FROM MyTable Left JOIN LATERAL TABLE(str_split('Jack,John')) AS T ON TRUE
        |""".stripMargin

    util.verifyExecPlan(sql)
  }
}
