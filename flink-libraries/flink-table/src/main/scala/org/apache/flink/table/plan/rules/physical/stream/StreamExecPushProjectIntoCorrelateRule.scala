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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecCalc, StreamExecCorrelate}
import org.apache.flink.table.plan.util.CorrelateUtil._

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

class StreamExecPushProjectIntoCorrelateRule extends RelOptRule(
  operand(
    classOf[StreamExecCalc],
    operand(
      classOf[StreamExecCorrelate],
      operand(classOf[RelNode], any))), "StreamExecPushProjectIntoCorrelateRule")  {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: StreamExecCalc = call.rel(0)
    projectable(calc.getProgram)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: StreamExecCalc = call.rel(0)
    val calcProgram = calc.getProgram
    val calcOutputType = calcProgram.getOutputRowType
    val calcInputType = calcProgram.getInputRowType
    val correlate: StreamExecCorrelate = call.rel(1)
    val correlateInputFieldCnt = correlate.getInput.getRowType.getFieldCount
    val refs = calcProgram.getReferenceCounts

    val projectableFieldSet = getProjectableFieldSet(refs, calcProgram, correlateInputFieldCnt)
    val (correlateNewType, selects) = projectCorrelateOutputType(
      correlate.getRowType, projectableFieldSet)
    val projectProgram = createProjectProgram(
      calcInputType,
      correlate.getCluster.getRexBuilder,
      selects)


    val (shiftProjects, shiftCondition) = shiftProjectsAndCondition(
      refs,
      calcProgram,
      projectableFieldSet,
      correlateNewType)
    val newProgram = RexProgram.create(
      correlateNewType,
      shiftProjects,
      shiftCondition,
      calcOutputType,
      calc.getCluster.getRexBuilder)

    if (newProgram.isTrivial) {
      // create new correlate
      val newCorrelate = correlate.copy(
        correlate.getTraitSet,
        correlate.getInput,
        Some(projectProgram),
        calcOutputType)
      // do not create another StreamExecCalcRemoveRule
      // correlate cannot carry collation or distribution currently.
      call.transformTo(newCorrelate)
    } else {
     // TODO FIXME Update new node's traitSet when this rule placed in the physical cbo-phase
     /* val reservedProjects = calcProgram.getProjectList.map {
        case (ref: RexLocalRef) =>
          (calcProgram.expandLocalRef(ref), ref.getIndex)
      }.filter {
        case (rex: RexNode, idx: Int) =>
          !projectableFieldSet.contains(idx)
      }.map(_._1)

      def getProjectMapping(projects: List[RexNode]): Mapping = {
        val mapping = Mappings
          .create(MappingType., calcInputType.getFieldCount, projects.size)
        projects.zipWithIndex.foreach {
          case (project, index) =>
            project match {
              case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
              case call: RexCall if call.getKind == SqlKind.AS =>
                call.getOperands.head match {
                  case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
                  case _ => // ignore
                }
              case _ => // ignore
            }
        }
        mapping.inverse()
      }
      val projectMapping = getProjectMapping(reservedProjects.toList)
      val traitSet = calc.getTraitSet
      val collation = traitSet.getTrait(RelCollationTraitDef.INSTANCE)
      val distribution = traitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
      val newCollation = TraitSetHelper.apply(collation, projectMapping)
      val newDistribution = distribution.apply(projectMapping)
      traitSet.replace(newDistribution)
      traitSet.replace(newCollation)*/

      // create new correlate
      val newCorrelate = correlate.copy(
        correlate.getTraitSet,
        correlate.getInput,
        Some(projectProgram),
        correlateNewType)
      val newCalc = calc.copy(calc.getTraitSet, newCorrelate, newProgram)
      call.transformTo(newCalc)
    }
  }
}

object StreamExecPushProjectIntoCorrelateRule {
  val INSTANCE = new StreamExecPushProjectIntoCorrelateRule
}
