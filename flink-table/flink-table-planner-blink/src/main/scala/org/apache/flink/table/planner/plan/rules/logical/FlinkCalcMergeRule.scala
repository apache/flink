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


import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.RelOptUtil.InputFinder
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{Calc, RelFactories}
import org.apache.calcite.rex.{RexNode, RexOver, RexProgramBuilder, RexUtil}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import scala.collection.JavaConversions._

/**
  * This rule is copied from Calcite's [[org.apache.calcite.rel.rules.CalcMergeRule]].
  *
  * Modification:
  * - Condition in the merged program will be simplified if it exists.
  * - If the two [[Calc]] can merge into one, each non-deterministic [[RexNode]] of bottom [[Calc]]
  *   should appear at most once in the project list and filter list of top [[Calc]].
  */

/**
  * Planner rule that merges a [[Calc]] onto a [[Calc]].
  *
  * <p>The resulting [[Calc]] has the same project list as the upper [[Calc]],
  * but expressed in terms of the lower [[Calc]]'s inputs.
  */
class FlinkCalcMergeRule[C <: Calc](calcClass: Class[C]) extends RelOptRule(
  operand(calcClass,
    operand(calcClass, any)),
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

    isMergeable(topCalc, bottomCalc)
  }

  /**
   * Return two neighbouring [[Calc]] can merge into one [[Calc]] or not. If the two [[Calc]] can
   * merge into one, each non-deterministic [[RexNode]] of bottom [[Calc]] should appear at most
   * once in the project list and filter list of top [[Calc]].
   */
  private def isMergeable(topCalc: Calc, bottomCalc: Calc): Boolean = {
    val topProgram = topCalc.getProgram
    val bottomProgram = bottomCalc.getProgram

    val topProjectInputIndices = topProgram.getProjectList
      .map(r => topProgram.expandLocalRef(r))
      .map(r => InputFinder.bits(r).toArray)

    val topFilterInputIndices = if (topProgram.getCondition != null) {
      InputFinder.bits(topProgram.expandLocalRef(topProgram.getCondition)).toArray
    } else {
      new Array[Int](0)
    }

    val bottomProjectList = bottomProgram.getProjectList
      .map(r => bottomProgram.expandLocalRef(r))
      .toArray

    val topInputIndices = topProjectInputIndices :+ topFilterInputIndices

    bottomProjectList.zipWithIndex.forall {
      case (project: RexNode, index: Int) => {
        var nonDeterministicRexRefCnt = 0
        if (!RexUtil.isDeterministic(project)) {
          topInputIndices.foreach(
            indices => indices.foreach(
              ref => if (ref == index) {
                nonDeterministicRexRefCnt += 1
              }
            )
          )
        }
        nonDeterministicRexRefCnt <= 1
      }
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topCalc: Calc = call.rel(0)
    val bottomCalc: Calc = call.rel(1)

    val topProgram = topCalc.getProgram
    val rexBuilder = topCalc.getCluster.getRexBuilder
    // Merge the programs together.
    val mergedProgram = RexProgramBuilder.mergePrograms(
      topCalc.getProgram, bottomCalc.getProgram, rexBuilder)
    require(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)

    val newMergedProgram = if (mergedProgram.getCondition != null) {
      val condition = mergedProgram.expandLocalRef(mergedProgram.getCondition)
      val simplifiedCondition = FlinkRexUtil.simplify(rexBuilder, condition)
      if (simplifiedCondition.toString == condition.toString) {
        mergedProgram
      } else {
        val programBuilder = RexProgramBuilder.forProgram(mergedProgram, rexBuilder, true)
        programBuilder.clearCondition()
        programBuilder.addCondition(simplifiedCondition)
        programBuilder.getProgram(true)
      }
    } else {
      mergedProgram
    }

    val newCalc = topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, newMergedProgram)
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
