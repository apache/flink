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

import com.google.common.collect.{ImmutableList, Maps}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, AggregateCall, Calc, Project, RelFactories}
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rex.{RexInputRef, RexNode, RexProgram, RexUtil}
import org.apache.calcite.runtime.Utilities
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mappings

import java.util

import scala.collection.JavaConversions._

/** Planner rule that removes unreferenced AggregateCall from Aggregate */
abstract class PruneAggregateCallRule[T <: RelNode](topClass: Class[T])
  extends RelOptRule(
    operand(topClass, operand(classOf[Aggregate], any)),
    RelFactories.LOGICAL_BUILDER,
    s"PruneAggregateCallRule_${topClass.getCanonicalName}") {

  protected def getInputRefs(relOnAgg: T): ImmutableBitSet

  override def matches(call: RelOptRuleCall): Boolean = {
    val relOnAgg: T = call.rel(0)
    val agg: Aggregate = call.rel(1)
    if (
      agg.getGroupType != Group.SIMPLE || agg.getAggCallList.isEmpty ||
      // at least output one column
      (agg.getGroupCount == 0 && agg.getAggCallList.size() == 1)
    ) {
      return false
    }
    val inputRefs = getInputRefs(relOnAgg)
    val unrefAggCallIndices = getUnrefAggCallIndices(inputRefs, agg)
    unrefAggCallIndices.nonEmpty
  }

  private def getUnrefAggCallIndices(inputRefs: ImmutableBitSet, agg: Aggregate): Array[Int] = {
    val groupCount = agg.getGroupCount
    agg.getAggCallList.indices
      .flatMap {
        index =>
          val aggCallOutputIndex = groupCount + index
          if (inputRefs.get(aggCallOutputIndex)) {
            Array.empty[Int]
          } else {
            Array(index)
          }
      }
      .toArray[Int]
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val relOnAgg: T = call.rel(0)
    val agg: Aggregate = call.rel(1)
    val inputRefs = getInputRefs(relOnAgg)
    var unrefAggCallIndices = getUnrefAggCallIndices(inputRefs, agg)
    require(unrefAggCallIndices.nonEmpty)

    val newAggCalls: util.List[AggregateCall] = new util.ArrayList(agg.getAggCallList)
    // remove unreferenced AggCall from original aggCalls
    unrefAggCallIndices.sorted.reverse.foreach(i => newAggCalls.remove(i))

    if (newAggCalls.isEmpty && agg.getGroupCount == 0) {
      // at least output one column
      newAggCalls.add(agg.getAggCallList.get(0))
      unrefAggCallIndices = unrefAggCallIndices.slice(1, unrefAggCallIndices.length)
    }

    val newAgg = agg.copy(
      agg.getTraitSet,
      agg.getInput,
      agg.getGroupSet,
      ImmutableList.of(agg.getGroupSet),
      newAggCalls
    )

    var newFieldIndex = 0
    // map old agg output index to new agg output index
    val mapOldToNew = Maps.newHashMap[Integer, Integer]()
    val fieldCountOfOldAgg = agg.getRowType.getFieldCount
    val unrefAggCallOutputIndices = unrefAggCallIndices.map(_ + agg.getGroupCount)
    (0 until fieldCountOfOldAgg).foreach {
      i =>
        if (!unrefAggCallOutputIndices.contains(i)) {
          mapOldToNew.put(i, newFieldIndex)
          newFieldIndex += 1
        }
    }
    require(mapOldToNew.size() == newAgg.getRowType.getFieldCount)

    val mapping = Mappings.target(mapOldToNew, fieldCountOfOldAgg, newAgg.getRowType.getFieldCount)
    val newRelOnAgg = createNewRel(mapping, relOnAgg, newAgg)
    call.transformTo(newRelOnAgg)
  }

  protected def createNewRel(mapping: Mappings.TargetMapping, project: T, newAgg: RelNode): RelNode
}

class ProjectPruneAggregateCallRule extends PruneAggregateCallRule(classOf[Project]) {
  override protected def getInputRefs(relOnAgg: Project): ImmutableBitSet = {
    RelOptUtil.InputFinder.bits(relOnAgg.getProjects, null)
  }

  override protected def createNewRel(
      mapping: Mappings.TargetMapping,
      project: Project,
      newAgg: RelNode): RelNode = {
    val newProjects = RexUtil.apply(mapping, project.getProjects).toList
    if (
      projectsOnlyIdentity(newProjects, newAgg.getRowType.getFieldCount) &&
      Utilities.compare(project.getRowType.getFieldNames, newAgg.getRowType.getFieldNames) == 0
    ) {
      newAgg
    } else {
      project.copy(project.getTraitSet, newAgg, newProjects, project.getRowType)
    }
  }

  private def projectsOnlyIdentity(projects: util.List[RexNode], inputFieldCount: Int): Boolean = {
    if (projects.size != inputFieldCount) {
      return false
    }
    projects.zipWithIndex.forall {
      case (project, index) =>
        project match {
          case r: RexInputRef => r.getIndex == index
          case _ => false
        }
    }
  }
}

class CalcPruneAggregateCallRule extends PruneAggregateCallRule(classOf[Calc]) {
  override protected def getInputRefs(relOnAgg: Calc): ImmutableBitSet = {
    val program = relOnAgg.getProgram
    val condition = if (program.getCondition != null) {
      program.expandLocalRef(program.getCondition)
    } else {
      null
    }
    val projects = program.getProjectList.map(program.expandLocalRef)
    RelOptUtil.InputFinder.bits(projects, condition)
  }

  override protected def createNewRel(
      mapping: Mappings.TargetMapping,
      calc: Calc,
      newAgg: RelNode): RelNode = {
    val program = calc.getProgram
    val newCondition = if (program.getCondition != null) {
      RexUtil.apply(mapping, program.expandLocalRef(program.getCondition))
    } else {
      null
    }
    val projects = program.getProjectList.map(program.expandLocalRef)
    val newProjects = RexUtil.apply(mapping, projects).toList
    val newProgram = RexProgram.create(
      newAgg.getRowType,
      newProjects,
      newCondition,
      program.getOutputRowType.getFieldNames,
      calc.getCluster.getRexBuilder
    )
    if (
      newProgram.isTrivial &&
      Utilities.compare(calc.getRowType.getFieldNames, newAgg.getRowType.getFieldNames) == 0
    ) {
      newAgg
    } else {
      calc.copy(calc.getTraitSet, newAgg, newProgram)
    }
  }
}

object PruneAggregateCallRule {
  val PROJECT_ON_AGGREGATE = new ProjectPruneAggregateCallRule
  val CALC_ON_AGGREGATE = new CalcPruneAggregateCallRule
}
