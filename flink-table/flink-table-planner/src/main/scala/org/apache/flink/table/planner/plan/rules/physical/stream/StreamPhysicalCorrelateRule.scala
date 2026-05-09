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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalCorrelate}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalCorrelateRule.{getMergedCalc, getTableScan}
import org.apache.flink.table.planner.plan.utils.{AsyncUtil, PythonUtil}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexNode, RexProgram, RexProgramBuilder, RexShuttle}
import org.apache.calcite.sql.validate.SqlValidatorUtil

import java.util.Collections

import scala.collection.JavaConverters._

/** Rule that converts [[FlinkLogicalCorrelate]] to [[StreamPhysicalCorrelate]]. */
class StreamPhysicalCorrelateRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val correlate: FlinkLogicalCorrelate = call.rel(0)
    val right = correlate.getRight.asInstanceOf[RelSubset].getOriginal

    // find only calc and table function
    @scala.annotation.tailrec
    def findTableFunction(calc: FlinkLogicalCalc): Boolean = {
      val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
      child match {
        case scan: FlinkLogicalTableFunctionScan =>
          PythonUtil.isNonPythonCall(scan.getCall) && AsyncUtil.isNonAsyncCall(scan.getCall)
        case calc: FlinkLogicalCalc => findTableFunction(calc)
        case _ => false
      }
    }

    right match {
      // right node is a table function
      case scan: FlinkLogicalTableFunctionScan =>
        PythonUtil.isNonPythonCall(scan.getCall) && AsyncUtil.isNonAsyncCall(scan.getCall)
      // a calc (filter and/or projection) is pushed above the table function
      case calc: FlinkLogicalCalc => findTableFunction(calc)
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val correlate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val convInput: RelNode =
      RelOptRule.convert(correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL)
    val right: RelNode = correlate.getInput(1)

    // Find the underlying TFS and the merged Calc above it (if any).
    val (scan, mergedCalc) = unwrapToScan(right)

    val condition: Option[RexNode] = mergedCalc.flatMap {
      calc =>
        val program = calc.getProgram
        Option(program.getCondition).map(program.expandLocalRef)
    }

    // When the Calc above the TFS has a non-identity projection, build the physical
    // Correlate with the full combined rowType (left ++ scan.rowType) and apply the
    // projection via a wrapping Calc on top. Otherwise, the codegen would concatenate
    // the left input with the full TFS output positionally and downstream consumers
    // would read the wrong fields when the claimed rowType is narrower.
    //
    // Restricted to INNER/LEFT because SEMI/ANTI Correlates produce only the left
    // fields; the right-side projection is only consumed by the join condition and
    // never appears in the Correlate's output.
    val needsProjectionAbove =
      mergedCalc.exists(calc => !calc.getProgram.projectsOnlyIdentity()) &&
        (correlate.getJoinType == JoinRelType.INNER ||
          correlate.getJoinType == JoinRelType.LEFT)

    if (needsProjectionAbove) {
      val combinedRowType = SqlValidatorUtil.deriveJoinRowType(
        convInput.getRowType,
        scan.getRowType,
        correlate.getJoinType,
        correlate.getCluster.getTypeFactory,
        null,
        Collections.emptyList())

      val physicalCorrelate = new StreamPhysicalCorrelate(
        correlate.getCluster,
        traitSet,
        convInput,
        scan,
        condition,
        combinedRowType,
        correlate.getJoinType)

      val wrappingProgram = buildWrappingProgram(
        mergedCalc.get.getProgram,
        combinedRowType,
        correlate.getRowType,
        convInput.getRowType.getFieldCount,
        correlate.getCluster.getRexBuilder)

      new StreamPhysicalCalc(
        correlate.getCluster,
        traitSet,
        physicalCorrelate,
        wrappingProgram,
        correlate.getRowType)
    } else {
      new StreamPhysicalCorrelate(
        correlate.getCluster,
        traitSet,
        convInput,
        scan,
        condition,
        correlate.getRowType,
        correlate.getJoinType)
    }
  }

  /**
   * Walks past planner shells to expose the {@link FlinkLogicalTableFunctionScan} at the bottom of
   * the right subtree, returning the Calc (with chained Calcs already merged into one) immediately
   * above it (if any).
   */
  private def unwrapToScan(
      rel: RelNode): (FlinkLogicalTableFunctionScan, Option[FlinkLogicalCalc]) = {
    rel match {
      case subset: RelSubset => unwrapToScan(subset.getRelList.get(0))
      case calc: FlinkLogicalCalc =>
        val merged = getMergedCalc(calc)
        (getTableScan(merged), Some(merged))
      case scan: FlinkLogicalTableFunctionScan => (scan, None)
    }
  }

  /**
   * Builds a {@link RexProgram} that re-applies the Calc's original projection to the output of the
   * physical Correlate (left ++ scan.rowType). Left fields are passed through unchanged; right-side
   * projection expressions have their input references shifted by {@code leftFieldCount} so they
   * index the combined input row.
   */
  private def buildWrappingProgram(
      originalProgram: RexProgram,
      combinedRowType: RelDataType,
      outputRowType: RelDataType,
      leftFieldCount: Int,
      rexBuilder: RexBuilder): RexProgram = {

    val shifter = new RexShuttle {
      override def visitInputRef(inputRef: RexInputRef): RexNode =
        new RexInputRef(inputRef.getIndex + leftFieldCount, inputRef.getType)
    }

    val builder = new RexProgramBuilder(combinedRowType, rexBuilder)

    val combinedFields = combinedRowType.getFieldList
    for (i <- 0 until leftFieldCount) {
      val field = combinedFields.get(i)
      builder.addProject(new RexInputRef(i, field.getType), field.getName)
    }

    val originalProjects = originalProgram.getProjectList.asScala
    val outputFields = outputRowType.getFieldList
    require(
      leftFieldCount + originalProjects.size == outputFields.size,
      s"output field count ${outputFields.size} != left ($leftFieldCount) + projection " +
        s"(${originalProjects.size})"
    )
    for (i <- originalProjects.indices) {
      val expanded = originalProgram.expandLocalRef(originalProjects(i)).accept(shifter)
      val outputName = outputFields.get(leftFieldCount + i).getName
      builder.addProject(expanded, outputName)
    }

    builder.getProgram
  }
}

object StreamPhysicalCorrelateRule {
  val INSTANCE: RelOptRule = new StreamPhysicalCorrelateRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalCorrelate],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalCorrelateRule"))

  def getMergedCalc(calc: FlinkLogicalCalc): FlinkLogicalCalc = {
    val child = calc.getInput match {
      case relSubset: RelSubset => relSubset.getOriginal
      case hepRelVertex: HepRelVertex => hepRelVertex.getCurrentRel
    }
    child match {
      case calc1: FlinkLogicalCalc =>
        val bottomCalc = getMergedCalc(calc1)
        val topCalc = calc
        val topProgram: RexProgram = topCalc.getProgram
        val mergedProgram: RexProgram = RexProgramBuilder
          .mergePrograms(
            topCalc.getProgram,
            bottomCalc.getProgram,
            topCalc.getCluster.getRexBuilder)
        assert(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)
        topCalc
          .copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
          .asInstanceOf[FlinkLogicalCalc]
      case _ =>
        calc
    }
  }

  @scala.annotation.tailrec
  def getTableScan(calc: FlinkLogicalCalc): FlinkLogicalTableFunctionScan = {
    val child = calc.getInput match {
      case relSubset: RelSubset => relSubset.getOriginal
      case hepRelVertex: HepRelVertex => hepRelVertex.getCurrentRel
    }
    child match {
      case scan: FlinkLogicalTableFunctionScan => scan
      case calc: FlinkLogicalCalc => getTableScan(calc)
      case _ => throw new TableException("This must be a bug, could not find table scan")
    }
  }
}
