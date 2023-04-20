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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType, Project}
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalProject}
import org.apache.calcite.rex.{RexInputRef, RexNode, RexShuttle}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mappings

import java.util
import java.util.function.IntFunction

import scala.collection.JavaConversions._

/**
 * Planner rule that pushes a [[Project]] down in a tree past a semi/anti [[Join]] by splitting the
 * projection into a projection on top of left child of the Join.
 */
class ProjectSemiAntiJoinTransposeRule
  extends RelOptRule(
    operand(classOf[LogicalProject], operand(classOf[LogicalJoin], any)),
    "ProjectSemiAntiJoinTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(1)
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI => true
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: LogicalProject = call.rel(0)
    val join: LogicalJoin = call.rel(1)

    // 1. calculate every inputs reference fields
    val joinCondFields = RelOptUtil.InputFinder.bits(join.getCondition)
    val projectFields = RelOptUtil.InputFinder.bits(project.getProjects, null)
    val allNeededFields = if (projectFields.isEmpty) {
      // if project does not reference any input fields, at least one field needs to be output
      joinCondFields.union(ImmutableBitSet.of(0))
    } else {
      joinCondFields.union(projectFields)
    }

    val leftFieldCount = join.getLeft.getRowType.getFieldCount
    val allInputFieldCount = leftFieldCount + join.getRight.getRowType.getFieldCount
    if (allNeededFields.equals(ImmutableBitSet.range(0, allInputFieldCount))) {
      return
    }

    val leftNeededFields = ImmutableBitSet.range(0, leftFieldCount).intersect(allNeededFields)
    val rightNeededFields = ImmutableBitSet
      .range(leftFieldCount, allInputFieldCount)
      .intersect(allNeededFields)

    // 2. new join inputs
    val newLeftInput = createNewJoinInput(call.builder, join.getLeft, leftNeededFields, 0)
    val newRightInput =
      createNewJoinInput(call.builder, join.getRight, rightNeededFields, leftFieldCount)

    // mapping origin field index to new field index,
    // used to rewrite join condition and top project
    val mapping = Mappings.target(
      new IntFunction[Integer]() {
        def apply(i: Int): Integer = allNeededFields.indexOf(i)
      },
      allInputFieldCount,
      allNeededFields.cardinality())

    // 3. create new join
    val newJoin = createNewJoin(join, mapping, newLeftInput, newRightInput)

    // 4. create top project
    val newProjects = createNewProjects(project, newJoin, mapping)
    val topProject = call.builder
      .push(newJoin)
      .project(newProjects, project.getRowType.getFieldNames)
      .build

    call.transformTo(topProject)
  }

  private def createNewJoinInput(
      relBuilder: RelBuilder,
      originInput: RelNode,
      inputNeededFields: ImmutableBitSet,
      offset: Int): RelNode = {
    val rexBuilder = originInput.getCluster.getRexBuilder
    val typeBuilder = relBuilder.getTypeFactory.builder()
    val newProjects: util.List[RexNode] = new util.ArrayList[RexNode]()
    val newFieldNames: util.List[String] = new util.ArrayList[String]()
    inputNeededFields.toList.foreach {
      i =>
        newProjects.add(rexBuilder.makeInputRef(originInput, i - offset))
        newFieldNames.add(originInput.getRowType.getFieldNames.get(i - offset))
        typeBuilder.add(originInput.getRowType.getFieldList.get(i - offset))
    }
    relBuilder.push(originInput).project(newProjects, newFieldNames).build
  }

  private def createNewJoin(
      originJoin: Join,
      mapping: Mappings.TargetMapping,
      newLeftInput: RelNode,
      newRightInput: RelNode): Join = {
    val newCondition = rewriteJoinCondition(originJoin, mapping)
    LogicalJoin.create(
      newLeftInput,
      newRightInput,
      java.util.Collections.emptyList(),
      newCondition,
      originJoin.getVariablesSet,
      originJoin.getJoinType)
  }

  private def rewriteJoinCondition(originJoin: Join, mapping: Mappings.TargetMapping): RexNode = {
    val rexBuilder = originJoin.getCluster.getRexBuilder
    val rexShuttle = new RexShuttle() {
      override def visitInputRef(ref: RexInputRef): RexNode = {
        val leftFieldCount = originJoin.getLeft.getRowType.getFieldCount
        val fieldType: RelDataType = if (ref.getIndex < leftFieldCount) {
          originJoin.getLeft.getRowType.getFieldList.get(ref.getIndex).getType
        } else {
          originJoin.getRight.getRowType.getFieldList.get(ref.getIndex - leftFieldCount).getType
        }
        rexBuilder.makeInputRef(fieldType, mapping.getTarget(ref.getIndex))
      }
    }
    originJoin.getCondition.accept(rexShuttle)
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

object ProjectSemiAntiJoinTransposeRule {
  val INSTANCE = new ProjectSemiAntiJoinTransposeRule
}
