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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.rules.{FlinkBatchRuleSets, FlinkStreamRuleSets}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.{BooleanPandasScalarFunction, BooleanPythonScalarFunction, PandasScalarFunction, PythonScalarFunction, RowPythonScalarFunction}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.{Before, Test}

/**
  * Test for [[PythonCalcSplitRule]].
  */
class PythonCalcSplitRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "logical",
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())
    programs.addLast(
      "logical_rewrite",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.LOGICAL_REWRITE)
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Int, Int, (Int, Int))]("MyTable", 'a, 'b, 'c, 'd)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.addFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))
    util.addFunction("pyFunc4", new BooleanPythonScalarFunction("pyFunc4"))
    util.addFunction("pyFunc5", new RowPythonScalarFunction("pyFunc5"))
    util.addFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))
    util.addFunction("pandasFunc2", new PandasScalarFunction("pandasFunc2"))
    util.addFunction("pandasFunc3", new PandasScalarFunction("pandasFunc3"))
    util.addFunction("pandasFunc4", new BooleanPandasScalarFunction("pandasFunc4"))
  }

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b) + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable WHERE pyFunc2(a, c) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable WHERE pyFunc4(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc3(pyFunc2(a + pyFunc1(a, c), b), c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT a, b FROM MyTable WHERE pyFunc4(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFieldNameUniquify(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable2", 'f0, 'f1, 'f2)
    val sqlQuery = "SELECT pyFunc1(f1, f2), f0 + 1 FROM MyTable2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLiteral(): Unit = {
    val sqlQuery = "SELECT a, b, pyFunc1(a, c), 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReorderPythonCalc(): Unit = {
    val sqlQuery = "SELECT a, pyFunc1(a, c), b FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionAsInputOfJavaFunction(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b) + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionMixedWithJavaFunction(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b), c + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b), c + 1 FROM MyTable WHERE pandasFunc2(a, c) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b) FROM MyTable WHERE pandasFunc4(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testChainingPandasFunction(): Unit = {
    val sqlQuery = "SELECT pandasFunc3(pandasFunc2(a + pandasFunc1(a, c), b), c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePandasFunction(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePandasFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT a, b FROM MyTable WHERE pandasFunc4(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionMixedWithGeneralPythonFunction(): Unit = {
    val sqlQuery = "SELECT pandasFunc1(a, b), pyFunc1(a, c) + 1, a + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionNotChainingWithGeneralPythonFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, pandasFunc1(a, b)) + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionWithCompositeInputs(): Unit = {
    val sqlQuery = "SELECT a, pyFunc1(b, d._1) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionWithCompositeInputsAndWhereClause(): Unit = {
    val sqlQuery = "SELECT a, pyFunc1(b, d._1) FROM MyTable WHERE a + 1 > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testChainingPythonFunctionWithCompositeInputs(): Unit = {
    val sqlQuery = "SELECT a, pyFunc1(b, pyFunc1(c, d._1)) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPandasFunctionWithCompositeInputs(): Unit = {
    val sqlQuery = "SELECT a, pandasFunc1(b, d._1) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionWithCompositeOutputs(): Unit = {
    val sqlQuery = "SELECT e.* FROM (SELECT pyFunc5(a) as e FROM MyTable) AS T"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionWithMultipleCompositeOutputs(): Unit = {
    val sqlQuery = "SELECT e.*, f.* FROM " +
      "(SELECT pyFunc5(a) as e, pyFunc5(b) as f FROM MyTable) AS T"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionWithCompositeInputsAndOutputs(): Unit = {
    val sqlQuery = "SELECT e.* FROM (SELECT pyFunc5(d._1) as e FROM MyTable) AS T"
    util.verifyPlan(sqlQuery)
  }
}
