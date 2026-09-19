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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalCorrelate}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalCorrelateRule.{getMergedCalc, getTableScan}
import org.apache.flink.table.planner.plan.utils.{AsyncUtil, FlinkRelUtil, PythonUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rex.{RexNode, RexProgram, RexUtil}
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
      // a filter is pushed above the table function
      case calc: FlinkLogicalCalc => findTableFunction(calc)
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val correlate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val cluster = correlate.getCluster
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val convInput: RelNode =
      RelOptRule.convert(correlate.getInput(0), FlinkConventions.STREAM_PHYSICAL)
    val right: RelNode = correlate.getInput(1)

    @scala.annotation.tailrec
    def unwrap(relNode: RelNode)
        : (FlinkLogicalTableFunctionScan, Option[Seq[RexNode]], Option[RexNode]) = {
      relNode match {
        case rel: RelSubset => unwrap(rel.getRelList.get(0))
        case calc: FlinkLogicalCalc =>
          val tableScan = getTableScan(calc)
          val newCalc = getMergedCalc(calc)
          val program = newCalc.getProgram
          val condition =
            if (program.getCondition == null) None
            else Some(program.expandLocalRef(program.getCondition))
          val projects =
            if (program.projectsOnlyIdentity()) None
            else Some(program.getProjectList.asScala.map(program.expandLocalRef).toSeq)
          (tableScan, projects, condition)
        case scan: FlinkLogicalTableFunctionScan =>
          (scan, None, None)
      }
    }

    val (scan, projectsOpt, condition) = unwrap(right)

    projectsOpt match {
      case None =>
        new StreamPhysicalCorrelate(
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
        val innerCorrelate = new StreamPhysicalCorrelate(
          cluster,
          traitSet,
          convInput,
          scan,
          condition,
          innerRowType,
          correlate.getJoinType)
        val outerProgram = StreamPhysicalCorrelateRule.buildOuterProgram(
          cluster,
          correlate.getLeft.getRowType.getFieldCount,
          innerRowType,
          correlate.getRowType,
          projects)
        new StreamPhysicalCalc(
          cluster,
          traitSet,
          innerCorrelate,
          outerProgram,
          correlate.getRowType)
    }
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
        FlinkRelUtil.merge(calc, bottomCalc).asInstanceOf[FlinkLogicalCalc]
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

  def buildOuterProgram(
      cluster: RelOptCluster,
      leftFieldCount: Int,
      innerRowType: RelDataType,
      outputRowType: RelDataType,
      rightProjects: Seq[RexNode]): RexProgram = {
    val rexBuilder = cluster.getRexBuilder
    val builder = new java.util.ArrayList[RexNode]()
    val leftFields = innerRowType.getFieldList
    var i = 0
    while (i < leftFieldCount) {
      builder.add(rexBuilder.makeInputRef(leftFields.get(i).getType, i))
      i += 1
    }
    rightProjects.foreach(p => builder.add(RexUtil.shift(p, leftFieldCount)))
    RexProgram.create(innerRowType, builder, null, outputRowType, rexBuilder)
  }
}
