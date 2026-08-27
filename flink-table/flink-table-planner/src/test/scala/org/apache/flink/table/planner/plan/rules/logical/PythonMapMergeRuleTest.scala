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
import org.apache.flink.table.planner.plan.rules.{FlinkBatchRuleSets, FlinkStreamRuleSets}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.{BooleanPythonScalarFunction, RowPandasScalarFunction, RowPythonScalarFunction}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.jupiter.api.{BeforeEach, Test}

class ResourceRowPythonScalarFunction(
    name: String,
    parallelism: Int = -1,
    maxArrowBatchSize: Int = -1)
  extends RowPythonScalarFunction(name) {
  override def getParallelism: Int = parallelism
  override def getMaxArrowBatchSize: Int = maxArrowBatchSize
}

/** Test for [[PythonMapMergeRule]]. */
class PythonMapMergeRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "logical",
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchRuleSets.LOGICAL_OPT_RULES)
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
    util.replaceBatchProgram(programs)
  }

  @Test
  def testMapOperationsChained(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val func = new RowPythonScalarFunction("pyFunc2")
    val result = sourceTable
      .map(func(withColumns('*)))
      .map(func(withColumns('*)))
      .map(func(withColumns('*)))
    util.verifyRelPlan(result)
  }

  @Test
  def testMapOperationMixedWithPandasUDFAndGeneralUDF(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val general_func = new RowPythonScalarFunction("general_func")
    val pandas_func = new RowPandasScalarFunction("pandas_func")

    val result = sourceTable
      .map(general_func(withColumns('*)))
      .map(pandas_func(withColumns('*)))
    util.verifyRelPlan(result)
  }

  @Test
  def testCompatibleExecutionOptionsAreMerged(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val explicit = new ResourceRowPythonScalarFunction("explicit", 2, 64)
    val unspecified = new ResourceRowPythonScalarFunction("unspecified")
    val result = sourceTable
      .map(explicit(withColumns('*)))
      .map(unspecified(withColumns('*)))
      .map(explicit(withColumns('*)))
    util.verifyRelPlan(result)
  }

  @Test
  def testNestedConflictingConcurrencyIsNotMerged(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val parallelism2 = new ResourceRowPythonScalarFunction("parallelism2", parallelism = 2)
    val unspecified = new ResourceRowPythonScalarFunction("unspecified")
    val parallelism4 = new ResourceRowPythonScalarFunction("parallelism4", parallelism = 4)
    val result = sourceTable
      .map(parallelism2(withColumns('*)))
      .map(unspecified(withColumns('*)))
      .map(parallelism4(withColumns('*)))
    util.verifyRelPlan(result)
  }

  @Test
  def testConflictingBatchSizesAreNotMerged(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val batch64 = new ResourceRowPythonScalarFunction("batch64", maxArrowBatchSize = 64)
    val batch128 = new ResourceRowPythonScalarFunction("batch128", maxArrowBatchSize = 128)
    val result = sourceTable
      .map(batch64(withColumns('*)))
      .map(batch128(withColumns('*)))
    util.verifyRelPlan(result)
  }

  @Test
  def testProjectWithOneField(): Unit = {
    val sourceTable = util.addTableSource[(Int, Int, Int)]("source", 'a, 'b, 'c)
    val booleanFunc = new BooleanPythonScalarFunction("boolean_func")
    val result = sourceTable.select('a).where(booleanFunc('a + 1, 'a))
    util.verifyRelPlan(result)
  }
}
