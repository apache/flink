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

import org.apache.flink.table.planner.plan.utils.PythonUtil

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rex.RexNode

class PythonRemoteCallFinder extends RemoteCallFinder {
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

  override def equals(obj: Any): Boolean = {
    obj != null && obj.isInstanceOf[PythonRemoteCallFinder]
  }

  override def hashCode(): Int = {
    this.getClass.hashCode()
  }

  override def getName: String = "Python"
}

object PythonCalcSplitRule {

  /**
   * These rules should be applied sequentially in the order of SPLIT_CONDITION,
   * CONDITION_PROJECTION_CSE, SPLIT_PROJECT, SPLIT_PANDAS_IN_PROJECT, EXPAND_PROJECT,
   * PUSH_CONDITION, REWRITE_PROJECT and PROJECTION_CSE.
   */
  private val callFinder = new PythonRemoteCallFinder()
  val SPLIT_CONDITION: RelOptRule = new RemoteCalcSplitConditionRule(callFinder)
  val CONDITION_PROJECTION_CSE: RelOptRule =
    RemoteCalcConditionProjectionCseRule.Config.DEFAULT.withRemoteCallFinder(callFinder).toRule()
  val SPLIT_PROJECT: RelOptRule = new RemoteCalcSplitProjectionRule(callFinder)
  val SPLIT_PANDAS_IN_PROJECT: RelOptRule = new PythonCalcSplitFunctionKindRule(callFinder)
  val SPLIT_PROJECTION_REX_FIELD: RelOptRule = new RemoteCalcSplitProjectionRexFieldRule(callFinder)
  val SPLIT_CONDITION_REX_FIELD: RelOptRule = new RemoteCalcSplitConditionRexFieldRule(callFinder)
  val EXPAND_PROJECT: RelOptRule = new RemoteCalcExpandProjectRule(callFinder)
  val PUSH_CONDITION: RelOptRule = new RemoteCalcPushConditionRule(callFinder)
  val REWRITE_PROJECT: RelOptRule = new RemoteCalcRewriteProjectionRule(callFinder)
  val PROJECTION_CSE: RelOptRule =
    RemoteCalcProjectionCseRule.Config.DEFAULT.withRemoteCallFinder(callFinder).toRule()
}
