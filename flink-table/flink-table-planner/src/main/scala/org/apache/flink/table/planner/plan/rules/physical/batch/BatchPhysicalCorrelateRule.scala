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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalCalc, BatchPhysicalCorrelate}
import org.apache.flink.table.planner.plan.rules.physical.common.CorrelateProjectionUtils
import org.apache.flink.table.planner.plan.utils.PythonUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil

import java.util.Collections

class BatchPhysicalCorrelateRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // right node is a table function
      // return true if it is a non python table function
      case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
      // a calc (filter and/or projection) is pushed above the table function
      // return true if it is a non python table function
      case calc: FlinkLogicalCalc =>
        calc.getInput.asInstanceOf[RelSubset].getOriginal match {
          case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
          case _ => false
        }
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val convInput: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.BATCH_PHYSICAL)
    val right: RelNode = join.getInput(1)

    // Walk through the right subtree to find the underlying TableFunctionScan and the
    // Calc (if any) that sits between the Correlate and the TFS.
    val (scan, calcOpt) = unwrapToScan(right)

    val condition: Option[RexNode] = calcOpt.flatMap {
      calc =>
        val program = calc.getProgram
        Option(program.getCondition).map(program.expandLocalRef)
    }

    // The Correlate's output before any right-side projection is left ++ scan.rowType.
    // When the Calc above the TFS has a non-identity projection, we cannot fold it into
    // the physical Correlate's output rowType: the codegen concatenates left input with
    // the *full* TFS output positionally, so a narrower rowType would cause downstream
    // consumers to read the wrong fields. Build the physical Correlate with the full
    // combined rowType and apply the projection via a wrapping Calc on top.
    //
    // Restricted to INNER/LEFT because SEMI/ANTI Correlates produce only the left fields;
    // the right-side projection is only consumed by the join condition and never appears
    // in the Correlate's output, so the bug cannot manifest there.
    val needsProjectionAbove =
      calcOpt.exists(calc => !calc.getProgram.projectsOnlyIdentity()) &&
        (join.getJoinType == JoinRelType.INNER || join.getJoinType == JoinRelType.LEFT)

    if (needsProjectionAbove) {
      val combinedRowType = SqlValidatorUtil.deriveJoinRowType(
        convInput.getRowType,
        scan.getRowType,
        join.getJoinType,
        join.getCluster.getTypeFactory,
        null,
        Collections.emptyList())

      val physicalCorrelate = new BatchPhysicalCorrelate(
        join.getCluster,
        traitSet,
        convInput,
        scan,
        condition,
        combinedRowType,
        join.getJoinType)

      val wrappingProgram = CorrelateProjectionUtils.buildWrappingProgram(
        calcOpt.get.getProgram,
        combinedRowType,
        join.getRowType,
        convInput.getRowType.getFieldCount,
        join.getCluster.getRexBuilder)

      new BatchPhysicalCalc(
        join.getCluster,
        traitSet,
        physicalCorrelate,
        wrappingProgram,
        join.getRowType)
    } else {
      new BatchPhysicalCorrelate(
        join.getCluster,
        traitSet,
        convInput,
        scan,
        condition,
        join.getRowType,
        join.getJoinType)
    }
  }

  /**
   * Walks past planner shells to expose the {@link FlinkLogicalTableFunctionScan} at the bottom of
   * the right subtree, returning the immediate {@link FlinkLogicalCalc} above it (if any).
   */
  private def unwrapToScan(
      rel: RelNode): (FlinkLogicalTableFunctionScan, Option[FlinkLogicalCalc]) = {
    rel match {
      case subset: RelSubset => unwrapToScan(subset.getRelList.get(0))
      case calc: FlinkLogicalCalc =>
        val (scan, _) = unwrapToScan(calc.getInput.asInstanceOf[RelSubset].getOriginal)
        (scan, Some(calc))
      case scan: FlinkLogicalTableFunctionScan => (scan, None)
    }
  }

}

object BatchPhysicalCorrelateRule {
  val INSTANCE: RelOptRule = new BatchPhysicalCorrelateRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalCorrelate],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchPhysicalCorrelateRule"))
}
