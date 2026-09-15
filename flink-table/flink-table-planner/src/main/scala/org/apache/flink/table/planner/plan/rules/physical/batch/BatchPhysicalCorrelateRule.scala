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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalCalc, BatchPhysicalCorrelate}
import org.apache.flink.table.planner.plan.utils.PythonUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rex.{RexNode, RexProgram, RexUtil}
import org.apache.calcite.sql.validate.SqlValidatorUtil

import java.util.Collections

import scala.collection.JavaConverters._

class BatchPhysicalCorrelateRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // right node is a table function
      // return true if it is a non python table function
      case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
      // a filter is pushed above the table function
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
    val correlate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val cluster = correlate.getCluster
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val convInput: RelNode =
      RelOptRule.convert(correlate.getInput(0), FlinkConventions.BATCH_PHYSICAL)

    // matches() guarantees the right side is either a TableFunctionScan, or a single Calc
    // whose immediate input is a TableFunctionScan.
    @scala.annotation.tailrec
    def unwrap(
        relNode: RelNode): (FlinkLogicalTableFunctionScan, Option[Seq[RexNode]], Option[RexNode]) =
      relNode match {
        case rel: RelSubset => unwrap(rel.getRelList.get(0))
        case scan: FlinkLogicalTableFunctionScan => (scan, None, None)
        case calc: FlinkLogicalCalc =>
          val scan = calc.getInput
            .asInstanceOf[RelSubset]
            .getOriginal
            .asInstanceOf[FlinkLogicalTableFunctionScan]
          val program = calc.getProgram
          val condition =
            if (program.getCondition == null) None
            else Some(program.expandLocalRef(program.getCondition))
          val projects =
            if (program.projectsOnlyIdentity()) None
            else Some(program.getProjectList.asScala.map(program.expandLocalRef).toSeq)
          (scan, projects, condition)
      }

    val (scan, projectsOpt, condition) = unwrap(correlate.getInput(1))

    projectsOpt match {
      case None =>
        new BatchPhysicalCorrelate(
          cluster,
          traitSet,
          convInput,
          scan,
          condition,
          correlate.getRowType,
          correlate.getJoinType)
      case Some(projects) =>
        val innerRowType = SqlValidatorUtil.deriveJoinRowType(
          correlate.getLeft.getRowType,
          scan.getRowType,
          correlate.getJoinType,
          cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory],
          null,
          Collections.emptyList[RelDataTypeField]()
        )
        val innerCorrelate = new BatchPhysicalCorrelate(
          cluster,
          traitSet,
          convInput,
          scan,
          condition,
          innerRowType,
          correlate.getJoinType)
        val outerProgram = BatchPhysicalCorrelateRule.buildOuterProgram(
          cluster,
          correlate.getLeft.getRowType.getFieldCount,
          innerRowType,
          correlate.getRowType,
          projects)
        new BatchPhysicalCalc(cluster, traitSet, innerCorrelate, outerProgram, correlate.getRowType)
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

  /**
   * Builds the outer Calc program that sits on top of the inner correlate: passes the left input
   * through unchanged, then appends the right-side projections shifted by the left field count.
   */
  def buildOuterProgram(
      cluster: RelOptCluster,
      leftFieldCount: Int,
      innerRowType: RelDataType,
      outputRowType: RelDataType,
      rightProjects: Seq[RexNode]): RexProgram = {
    val rexBuilder = cluster.getRexBuilder
    val outerProjects = new java.util.ArrayList[RexNode]()
    val innerFields = innerRowType.getFieldList
    var i = 0
    while (i < leftFieldCount) {
      outerProjects.add(rexBuilder.makeInputRef(innerFields.get(i).getType, i))
      i += 1
    }
    rightProjects.foreach(p => outerProjects.add(RexUtil.shift(p, leftFieldCount)))
    RexProgram.create(innerRowType, outerProjects, null, outputRowType, rexBuilder)
  }
}
