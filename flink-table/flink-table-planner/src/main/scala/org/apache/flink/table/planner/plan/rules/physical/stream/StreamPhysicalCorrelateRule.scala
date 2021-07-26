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
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalCorrelateRule.{getMergedCalc, getTableScan}
import org.apache.flink.table.planner.plan.utils.PythonUtil

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.{RexNode, RexProgram, RexProgramBuilder}

/**
 * Rule that converts [[FlinkLogicalCorrelate]] to [[StreamPhysicalCorrelate]].
 */
class StreamPhysicalCorrelateRule
  extends ConverterRule(
    classOf[FlinkLogicalCorrelate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalCorrelateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val correlate: FlinkLogicalCorrelate = call.rel(0)
    val right = correlate.getRight.asInstanceOf[RelSubset].getOriginal

    // find only calc and table function
    @scala.annotation.tailrec
    def findTableFunction(calc: FlinkLogicalCalc): Boolean = {
      val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
      child match {
        case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
        case calc: FlinkLogicalCalc => findTableFunction(calc)
        case _ => false
      }
    }

    right match {
      // right node is a table function
      case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
      // a filter is pushed above the table function
      case calc: FlinkLogicalCalc => findTableFunction(calc)
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val correlate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val convInput: RelNode = RelOptRule.convert(
      correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL)
    val right: RelNode = correlate.getInput(1)

    @scala.annotation.tailrec
    def convertToCorrelate(
        relNode: RelNode,
        condition: Option[RexNode]): StreamPhysicalCorrelate = {
      relNode match {
        case rel: RelSubset =>
          convertToCorrelate(rel.getRelList.get(0), condition)

        case calc: FlinkLogicalCalc =>
          val tableScan = getTableScan(calc)
          val newCalc = getMergedCalc(calc)
          convertToCorrelate(
            tableScan,
            Some(newCalc.getProgram.expandLocalRef(newCalc.getProgram.getCondition)))

        case scan: FlinkLogicalTableFunctionScan =>
          new StreamPhysicalCorrelate(
            rel.getCluster,
            traitSet,
            convInput,
            scan,
            condition,
            rel.getRowType,
            correlate.getJoinType)
      }
    }
    convertToCorrelate(right, None)
  }

}

object StreamPhysicalCorrelateRule {
  val INSTANCE: RelOptRule = new StreamPhysicalCorrelateRule

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
        topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
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
