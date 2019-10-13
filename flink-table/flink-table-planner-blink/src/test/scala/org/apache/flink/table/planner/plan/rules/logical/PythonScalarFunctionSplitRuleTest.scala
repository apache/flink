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

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{FunctionLanguage, ScalarFunction}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.rules.{FlinkBatchRuleSets, FlinkStreamRuleSets}
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}

/**
  * Test for [[PythonScalarFunctionSplitRule]].
  */
class PythonScalarFunctionSplitRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "table_ref",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.TABLE_REF_RULES)
        .build())
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
  }

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b) + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunction(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))

    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable WHERE pyFunc2(a, c) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionInWhereClause(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new BooleanPythonScalarFunction("pyFunc2"))

    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable WHERE pyFunc2(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.addFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))

    val sqlQuery = "SELECT pyFunc3(pyFunc2(a + pyFunc1(a, c), b), c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunction(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunctionInWhereClause(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new BooleanPythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT a, b FROM MyTable WHERE pyFunc1(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFieldNameUniquify(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'f0, 'f1, 'f2)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(f1, f2), f0 + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }
}

class PythonScalarFunction(name: String) extends ScalarFunction {
  def eval(i: Int, j: Int): Int = i + j

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    BasicTypeInfo.INT_TYPE_INFO

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON

  override def toString: String = name

}

class BooleanPythonScalarFunction(name: String) extends ScalarFunction {
  def eval(i: Int, j: Int): Boolean = i + j > 1

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    BasicTypeInfo.BOOLEAN_TYPE_INFO

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON

  override def toString: String = name
}
