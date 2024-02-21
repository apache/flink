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

import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.python.PythonFunctionKind
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.plan.utils.{InputRefVisitor, PythonUtil, RexDefaultVisitor}
import org.apache.flink.table.planner.plan.utils.PythonUtil.{containsNonPythonCall, containsPythonCall, isNonPythonCall, isPythonCall}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexCorrelVariable, RexFieldAccess, RexInputRef, RexLocalRef, RexNode, RexProgram}
import org.apache.calcite.sql.validate.SqlValidatorUtil

import java.util.function.Function

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Rule that splits [[FlinkLogicalCalc]]s which contain both general Python functions and pandas
 * Python functions in the projection into multiple [[FlinkLogicalCalc]]s. After this rule is
 * applied, it will only contain general Python functions or pandas Python functions in the
 * projection of each [[FlinkLogicalCalc]].
 */
class PythonCalcSplitPandasInProjectionRule(callFinder: RemoteCalcCallFinder)
  extends RemoteCalcSplitProjectionRuleBase("PythonCalcSplitPandasInProjectionRule", callFinder) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)

    // matches if it contains both general Python functions and
    // pandas Python functions in the projection
    projects.exists(containsPythonCall(_, PythonFunctionKind.GENERAL)) &&
    projects.exists(containsPythonCall(_, PythonFunctionKind.PANDAS))
  }

  override def needConvert(
      program: RexProgram,
      node: RexNode,
      matchState: Option[Nothing]): Boolean = {
    program.getProjectList
      .map(program.expandLocalRef)
      .exists(isPythonCall(_, PythonFunctionKind.GENERAL)) == isPythonCall(
      node,
      PythonFunctionKind.PANDAS)
  }
}

class PythonRemoteCalcCallFinder extends RemoteCalcCallFinder {
  override def containsRemoteCall(node: RexNode): Boolean = {
    PythonUtil.containsPythonCall(node)
  }

  override def containsNonRemoteCall(node: RexNode): Boolean = {
    PythonUtil.containsNonPythonCall(node)
  }

  override def isRemoteCall(node: RexNode): Boolean = {
    PythonUtil.isPythonCall(node)
  }

  override def isNonRemoteCall(node: RexNode): Boolean = {
    PythonUtil.isNonPythonCall(node)
  }
}

object PythonCalcSplitRule {

  /**
   * These rules should be applied sequentially in the order of SPLIT_CONDITION, SPLIT_PROJECT,
   * SPLIT_PANDAS_IN_PROJECT, EXPAND_PROJECT, PUSH_CONDITION and REWRITE_PROJECT.
   */
  private val callFinder = new PythonRemoteCalcCallFinder()
  val SPLIT_CONDITION: RelOptRule = new RemoteCalcSplitConditionRule(callFinder)
  val SPLIT_PROJECT: RelOptRule = new RemoteCalcSplitProjectionRule(callFinder)
  val SPLIT_PANDAS_IN_PROJECT: RelOptRule = new PythonCalcSplitPandasInProjectionRule(callFinder)
  val SPLIT_PROJECTION_REX_FIELD: RelOptRule = new RemoteCalcSplitProjectionRexFieldRule(callFinder)
  val SPLIT_CONDITION_REX_FIELD: RelOptRule = new RemoteCalcSplitConditionRexFieldRule(callFinder)
  val EXPAND_PROJECT: RelOptRule = new RemoteCalcExpandProjectRule(callFinder)
  val PUSH_CONDITION: RelOptRule = new RemoteCalcPushConditionRule(callFinder)
  val REWRITE_PROJECT: RelOptRule = new RemoteCalcRewriteProjectionRule(callFinder)
}
