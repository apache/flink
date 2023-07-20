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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc
import org.apache.flink.table.planner.plan.utils.FlinkRelUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.core.{Calc, RelFactories}
import org.apache.calcite.rex.{RexNode, RexOver}

/**
 * This rule is copied from Calcite's [[org.apache.calcite.rel.rules.CalcMergeRule]].
 *
 * Modification:
 *   - Condition in the merged program will be simplified if it exists.
 *   - If the two [[Calc]] can merge into one, each non-deterministic [[RexNode]] of bottom [[Calc]]
 *     should appear at most once in the project list and filter list of top [[Calc]].
 */

/**
 * Planner rule that merges a [[Calc]] onto a [[Calc]].
 *
 * <p>The resulting [[Calc]] has the same project list as the upper [[Calc]], but expressed in terms
 * of the lower [[Calc]]'s inputs.
 */
class FlinkCalcMergeRule[C <: Calc](calcClass: Class[C])
  extends RelOptRule(
    operand(calcClass, operand(calcClass, any)),
    RelFactories.LOGICAL_BUILDER,
    "FlinkCalcMergeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val topCalc: Calc = call.rel(0)
    val bottomCalc: Calc = call.rel(1)

    // Don't merge a calc which contains windowed aggregates onto a
    // calc. That would effectively be pushing a windowed aggregate down
    // through a filter.
    val topProgram = topCalc.getProgram
    if (RexOver.containsOver(topProgram)) {
      return false
    }

    FlinkRelUtil.isMergeable(topCalc, bottomCalc)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topCalc: Calc = call.rel(0)
    val bottomCalc: Calc = call.rel(1)

    val newCalc = FlinkRelUtil.merge(topCalc, bottomCalc)
    if (newCalc.getDigest == bottomCalc.getDigest) {
      // newCalc is equivalent to bottomCalc,
      // which means that topCalc
      // must be trivial. Take it out of the game.
      call.getPlanner.prune(topCalc)
    }
    call.transformTo(newCalc)
  }

}

object FlinkCalcMergeRule {
  val INSTANCE = new FlinkCalcMergeRule(classOf[Calc])
  val STREAM_PHYSICAL_INSTANCE = new FlinkCalcMergeRule(classOf[StreamPhysicalCalc])
}
