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

import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions._
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.jupiter.api.{BeforeEach, Test}

/** Test for [[PythonAsyncCalcSplitRule]]. */
class PythonAsyncCalcSplitRuleTest extends TableTestBase {

  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[StreamOptimizeContext]()
    programs.addLast(
      "logical",
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build()
    )
    programs.addLast(
      "logical_rewrite",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.LOGICAL_REWRITE)
        .build()
    )
    util.replaceStreamProgram(programs)

    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("asyncFunc1", new AsyncPythonScalarFunction("asyncFunc1"))
    util.addTemporarySystemFunction("asyncFunc2", new AsyncPythonScalarFunction("asyncFunc2"))
    util.addTemporarySystemFunction("asyncFunc3", new AsyncPythonScalarFunction("asyncFunc3"))
    util.addTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addTemporarySystemFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
  }

  @Test
  def testAsyncPythonFunctionAsInputOfJavaFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b) + 1 FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionMixedWithJavaFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), c + 1 FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), c + 1 FROM MyTable WHERE asyncFunc2(a, c) > 0"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b) FROM MyTable WHERE asyncFunc2(a, c) > 0"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testChainingAsyncPythonFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc3(asyncFunc2(a + asyncFunc1(a, c), b), c) FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testOnlyOneAsyncPythonFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b) FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testOnlyOneAsyncPythonFunctionInWhereClause(): Unit = {
    val sqlQuery = "SELECT a, b FROM MyTable WHERE asyncFunc1(a, c) > 0"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultipleAsyncPythonFunctions(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), asyncFunc2(b, c) FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionMixedWithSyncPythonFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), pyFunc1(b, c) FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionNotChainingWithSyncPythonFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, asyncFunc1(a, b)) + 1 FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncAndSyncPythonFunctionInWhereClause(): Unit = {
    val sqlQuery =
      "SELECT asyncFunc1(a, b) FROM MyTable WHERE pyFunc1(a, c) > 0 AND asyncFunc2(b, c) > 0"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSameAsyncPythonFunctionUsedInBothSelectAndWhere(): Unit = {
    val sqlQuery = "SELECT a, asyncFunc1(a, c) FROM MyTable WHERE asyncFunc1(a, c) > 0"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testReorderAsyncPythonCalc(): Unit = {
    val sqlQuery = "SELECT a, asyncFunc1(a, c), b FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionWithLiteral(): Unit = {
    val sqlQuery = "SELECT a, b, asyncFunc1(a, c), 1 FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionWithMultipleInputs(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), asyncFunc2(b, c), a + b FROM MyTable"
    util.verifyRelPlan(sqlQuery)
  }
}
