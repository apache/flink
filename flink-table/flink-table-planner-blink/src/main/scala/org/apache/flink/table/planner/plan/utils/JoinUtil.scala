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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.plan.nodes.exec.utils.JoinSpec
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo}
import org.apache.calcite.rex.{RexNode, RexUtil}
import org.apache.calcite.util.ImmutableIntList

import java.util
import java.util.Optional

import scala.collection.JavaConversions._

/**
  * Util for [[Join]]s.
  */
object JoinUtil {

  /**
   * Create [[JoinSpec]] according to the given join.
   */
  def createJoinSpec(join: Join): JoinSpec = {
    val filterNulls = new util.ArrayList[java.lang.Boolean]
    val joinInfo = createJoinInfo(join.getLeft, join.getRight, join.getCondition, filterNulls)
    val nonEquiCondition =
        RexUtil.composeConjunction(join.getCluster.getRexBuilder, joinInfo.nonEquiConditions)
    new JoinSpec(
        JoinTypeUtil.getFlinkJoinType(join.getJoinType),
        joinInfo.leftKeys.toIntArray,
        joinInfo.rightKeys.toIntArray,
        filterNulls.map(_.booleanValue()).toArray,
        nonEquiCondition)
  }

  /**
   * Validates that join keys in [[JoinSpec]] is compatible in both sides of join.
   */
  def validateJoinSpec(
      joinSpec: JoinSpec,
      leftType: RowType,
      rightType: RowType,
      allowEmptyKey: Boolean = false): Unit = {
    if (joinSpec.getLeftKeys.isEmpty && !allowEmptyKey) {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tLeft type: $leftType\n\tright type: $rightType\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
    }

    val leftKeys = joinSpec.getLeftKeys
    val rightKeys = joinSpec.getRightKeys
    (0 until joinSpec.getJoinKeySize).foreach { idx =>
        val leftKeyType = leftType.getTypeAt(leftKeys(idx))
        val rightKeyType = rightType.getTypeAt(rightKeys(idx))

        // check if keys are compatible
        if (!PlannerTypeUtils.isInteroperable(leftKeyType, rightKeyType)) {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft type: $leftType\n\tright type: $rightType\n" +
              "please re-check the join statement.")
        }
    }
  }

  /**
    * Creates a [[JoinInfo]] by analyzing a condition.
    *
    * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]],
    * the only difference is that the methods could return `filterNulls`.
    */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining = RelOptUtil.splitJoinCondition(
      left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

  def generateConditionFunction(
      config: TableConfig,
      joinSpec: JoinSpec,
      leftType: LogicalType,
      rightType: LogicalType): GeneratedJoinCondition = {
    generateConditionFunction(
        config,
        joinSpec.getNonEquiCondition().orElse(null),
        leftType,
        rightType)
  }

  def generateConditionFunction(
        config: TableConfig,
        nonEquiCondition: RexNode,
        leftType: LogicalType,
        rightType: LogicalType): GeneratedJoinCondition = {
    val ctx = CodeGeneratorContext(config)
    // should consider null fields
    val exprGenerator = new ExprCodeGenerator(ctx, false)
        .bindInput(leftType)
        .bindSecondInput(rightType)

    val body = if (nonEquiCondition == null) {
      // only equality condition
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(nonEquiCondition)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)
  }
}
