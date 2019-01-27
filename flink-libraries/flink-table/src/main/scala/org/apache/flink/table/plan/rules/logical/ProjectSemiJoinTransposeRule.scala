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

package org.apache.flink.table.plan.rules.logical

import java.util
import java.util.function.IntFunction

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.{JoinInfo, Project, RelFactories, SemiJoin}
import org.apache.calcite.rel.rules.PushProjector
import org.apache.calcite.rex.{RexInputRef, RexNode, RexShuttle}
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mappings

import scala.collection.JavaConversions._

/**
  * Planner rule that pushes a [[Project]] past a [[SemiJoin]]
  * by splitting the projection into a projection on top of left child of the SemiJoin.
  */
class ProjectSemiJoinTransposeRule(
  preserveExprCondition: PushProjector.ExprCondition,
  relFactory: RelBuilderFactory)
  extends RelOptRule(
    operand(
      classOf[Project],
      operand(classOf[SemiJoin], any)),
    relFactory, "ProjectSemiJoinTransposeRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: Project = call.rel(0)
    val semiJoin: SemiJoin = call.rel(1)

    // 1. calculate every inputs reference fields
    val joinCondFields = RelOptUtil.InputFinder.bits(semiJoin.getCondition)
    val projectFields = RelOptUtil.InputFinder.bits(project.getProjects, null)
    val allNeededFields = joinCondFields.union(projectFields)

    val leftFieldCount = semiJoin.getLeft.getRowType.getFieldCount
    val allInputFieldCount = leftFieldCount + semiJoin.getRight.getRowType.getFieldCount
    if (allNeededFields.equals(ImmutableBitSet.range(0, allInputFieldCount))) {
      return
    }

    val leftNeededFields = ImmutableBitSet.range(0, leftFieldCount).intersect(allNeededFields)
    val rightNeededFields = ImmutableBitSet.range(leftFieldCount, allInputFieldCount)
      .intersect(allNeededFields)

    // 2. new SemiJoin inputs
    val newLeftInput = createNewSemiJoinInput(call.builder, semiJoin.getLeft, leftNeededFields, 0)
    val newRightInput = createNewSemiJoinInput(
      call.builder, semiJoin.getRight, rightNeededFields, leftFieldCount)

    // mapping origin field index to new field index,
    // used to rewrite SemiJoin condition and top project
    val mapping = Mappings.target(
      new IntFunction[Integer]() {
        def apply(i: Int): Integer = allNeededFields.indexOf(i)
      },
      allInputFieldCount, allNeededFields.cardinality())

    // 3. create new SemiJoin
    val newSemiJoin = createNewSemiJoin(semiJoin, mapping, newLeftInput, newRightInput)

    // 4. create top project
    val newProjects = createNewProjects(project, newSemiJoin, mapping)
    val topProject = call.builder
      .push(newSemiJoin)
      .project(newProjects, project.getRowType.getFieldNames)
      .build

    call.transformTo(topProject)
  }

  private def createNewSemiJoinInput(
    relBuilder: RelBuilder,
    originInput: RelNode,
    inputNeededFields: ImmutableBitSet,
    offset: Int): RelNode = {
    val rexBuilder = originInput.getCluster.getRexBuilder
    val typeBuilder = new RelDataTypeFactory.FieldInfoBuilder(relBuilder.getTypeFactory)
    val newProjects: util.List[RexNode] = new util.ArrayList[RexNode]()
    val newFieldNames: util.List[String] = new util.ArrayList[String]()
    inputNeededFields.toList.foreach { i =>
      newProjects.add(rexBuilder.makeInputRef(originInput, i - offset))
      newFieldNames.add(originInput.getRowType.getFieldNames.get(i - offset))
      typeBuilder.add(originInput.getRowType.getFieldList.get(i - offset))
    }
    relBuilder.push(originInput).project(newProjects, newFieldNames).build
  }

  private def createNewSemiJoin(
    originSemiJoin: SemiJoin,
    mapping: Mappings.TargetMapping,
    newLeftInput: RelNode,
    newRightInput: RelNode): SemiJoin = {
    val newCondition = rewriteJoinCondition(originSemiJoin, mapping)
    val joinInfo = JoinInfo.of(newLeftInput, newRightInput, newCondition)
    SemiJoin.create(newLeftInput, newRightInput, newCondition, joinInfo
      .leftKeys, joinInfo.rightKeys, originSemiJoin.isAnti)
  }

  private def rewriteJoinCondition(
    originSemiJoin: SemiJoin,
    mapping: Mappings.TargetMapping): RexNode = {
    val rexBuilder = originSemiJoin.getCluster.getRexBuilder
    val rexShuttle = new RexShuttle() {
      override def visitInputRef(ref: RexInputRef): RexNode = {
        val leftFieldCount = originSemiJoin.getLeft.getRowType.getFieldCount
        val fieldType: RelDataType = if (ref.getIndex < leftFieldCount) {
          originSemiJoin.getLeft.getRowType.getFieldList.get(ref.getIndex).getType
        } else {
          originSemiJoin.getRight.getRowType.getFieldList.get(ref.getIndex - leftFieldCount).getType
        }
        rexBuilder.makeInputRef(fieldType, mapping.getTarget(ref.getIndex))
      }
    }
    originSemiJoin.getCondition.accept(rexShuttle)
  }

  private def createNewProjects(
    originProject: Project,
    newInput: RelNode,
    mapping: Mappings.TargetMapping): Seq[RexNode] = {
    val rexBuilder = originProject.getCluster.getRexBuilder
    val projectShuffle = new RexShuttle() {
      override def visitInputRef(ref: RexInputRef): RexNode = {
        rexBuilder.makeInputRef(newInput, mapping.getTarget(ref.getIndex))
      }
    }
    originProject.getProjects.map(_.accept(projectShuffle))
  }
}

object ProjectSemiJoinTransposeRule {
  val INSTANCE = new ProjectSemiJoinTransposeRule(
    PushProjector.ExprCondition.TRUE, RelFactories.LOGICAL_BUILDER)
}
