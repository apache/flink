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

package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.utils.{MockPythonTableFunction, TableFunc0, TableFunc1, TableTestBase}
import org.apache.calcite.rel.rules.{CalcMergeRule, FilterCalcMergeRule, ProjectCalcMergeRule}
import org.apache.calcite.tools.RuleSets
import org.junit.Test

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc1
    util.addFunction("func1", func)

    val result1 = table.joinLateral(func('c) as 's).select('c, 's)

    util.verifyPlan(result1)
  }

  @Test
  def testCrossJoin2(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc1
    util.addFunction("func1", func)

    val result2 = table.joinLateral(func('c, "$") as 's).select('c, 's)
    util.verifyPlan(result2)
  }

  @Test
  def testLeftOuterJoinWithoutJoinPredicates(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc1
    util.addFunction("func1", func)

    val result = table.leftOuterJoinLateral(func('c) as 's).select('c, 's).where('s > "")
    util.verifyPlan(result)
  }

  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc1
    util.addFunction("func1", func)

    val result = table.leftOuterJoinLateral(func('c) as 's, true).select('c, 's)
    util.verifyPlan(result)
  }

  @Test
  def testCorrelateWithMultiFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc0
    util.addFunction("func1", func)

    val result = sourceTable.select('a, 'b, 'c)
      .joinLateral(func('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    util.verifyPlan(result)
  }

  @Test
  def testCorrelateWithMultiFilterAndWithoutCalcMergeRules(): Unit = {
    val util = batchTestUtil()
    val programs = util.getBatchProgram()
    programs.getFlinkRuleSetProgram(FlinkBatchProgram.LOGICAL)
      .get.remove(
      RuleSets.ofList(
        CalcMergeRule.INSTANCE,
        FilterCalcMergeRule.INSTANCE,
        ProjectCalcMergeRule.INSTANCE))
    // removing
    util.replaceBatchProgram(programs)

    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc0
    util.addFunction("func1", func)

    val result = sourceTable.select('a, 'b, 'c)
      .joinLateral(func('c) as('d, 'e))
      .select('c, 'd, 'e)
      .where('e > 10)
      .where('e > 20)
      .select('c, 'd)

    util.verifyPlan(result)
  }

  @Test
  def testCorrelatePythonTableFunction(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
    val func = new MockPythonTableFunction
    val result = sourceTable.joinLateral(func('a, 'b) as('x, 'y))

    util.verifyPlan(result)
  }
}
