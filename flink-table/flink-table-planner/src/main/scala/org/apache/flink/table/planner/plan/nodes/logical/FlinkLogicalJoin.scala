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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.JList
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind

import scala.collection.JavaConversions._

/**
 * Sub-class of [[Join]] that is a relational expression which combines two relational expressions
 * according to some condition in Flink.
 */
class FlinkLogicalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    hints: JList[RelHint],
    joinType: JoinRelType)
  extends Join(cluster, traitSet, hints, left, right, condition, Set.empty[CorrelationId], joinType)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new FlinkLogicalJoin(cluster, traitSet, left, right, conditionExpr, getHints, joinType)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val leftRowSize = mq.getAverageRowSize(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)

    joinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        val rightRowSize = mq.getAverageRowSize(getRight)
        val ioCost = (leftRowCnt * leftRowSize) + (rightRowCnt * rightRowSize)
        val cpuCost = leftRowCnt + rightRowCnt
        val rowCnt = leftRowCnt + rightRowCnt
        planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
      case _ =>
        val cpuCost = leftRowCnt + rightRowCnt
        val ioCost = (leftRowCnt * leftRowSize) + rightRowCnt
        planner.getCostFactory.makeCost(leftRowCnt, cpuCost, ioCost)
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val leftFieldCount = getLeft.getRowType.getFieldCount
    val rightFieldCount = getRight.getRowType.getFieldCount
    val leftFieldNames = getLeft.getRowType.getFieldNames.toList
    val rightFieldNames = getRight.getRowType.getFieldNames.toList

    // Format join condition with field names
    val conditionStr = formatJoinCondition(
      getCondition,
      leftFieldNames,
      rightFieldNames,
      leftFieldCount
    )

    pw.input("left", getLeft)
      .input("right", getRight)
      .item("Type", joinType.toString)
      .item("Condition", conditionStr)
  }

  /**
   * Formats a join condition into a readable string with field names. Converts expressions like
   * "=($0, $3)" into "orders.user_id = users.id"
   */
  private def formatJoinCondition(
      condition: RexNode,
      leftFieldNames: List[String],
      rightFieldNames: List[String],
      leftFieldCount: Int): String = {

    condition match {
      case call: RexCall if call.getKind == SqlKind.EQUALS =>
        // Handle simple equality: field1 = field2
        val operands = call.getOperands
        if (operands.size == 2) {
          val left = formatOperand(operands.get(0), leftFieldNames, rightFieldNames, leftFieldCount)
          val right =
            formatOperand(operands.get(1), leftFieldNames, rightFieldNames, leftFieldCount)
          s"$left = $right"
        } else {
          FlinkRexUtil.getExpressionString(condition, leftFieldNames ++ rightFieldNames)
        }

      case call: RexCall if call.getKind == SqlKind.AND =>
        // Handle composite conditions: cond1 AND cond2
        val operands = call.getOperands
        val formattedOperands = operands.map {
          operand => formatJoinCondition(operand, leftFieldNames, rightFieldNames, leftFieldCount)
        }
        formattedOperands.mkString(" AND ")

      case call: RexCall if call.getKind == SqlKind.OR =>
        // Handle OR conditions
        val operands = call.getOperands
        val formattedOperands = operands.map {
          operand => formatJoinCondition(operand, leftFieldNames, rightFieldNames, leftFieldCount)
        }
        formattedOperands.mkString(" OR ")

      case _ =>
        // Fallback to default formatting for complex expressions
        FlinkRexUtil.getExpressionString(condition, leftFieldNames ++ rightFieldNames)
    }
  }

  /** Formats a single operand (field reference) with table context. */
  private def formatOperand(
      operand: RexNode,
      leftFieldNames: List[String],
      rightFieldNames: List[String],
      leftFieldCount: Int): String = {

    operand match {
      case inputRef: RexInputRef =>
        val index = inputRef.getIndex
        if (index < leftFieldCount) {
          // Field from left table
          leftFieldNames(index)
        } else {
          // Field from right table
          rightFieldNames(index - leftFieldCount)
        }
      case _ =>
        // For complex expressions, use default formatting
        FlinkRexUtil.getExpressionString(operand, leftFieldNames ++ rightFieldNames)
    }
  }

}

/** Support all joins. */
private class FlinkLogicalJoinConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[LogicalJoin]
    val newLeft = RelOptRule.convert(join.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(join.getRight, FlinkConventions.LOGICAL)
    FlinkLogicalJoin.create(newLeft, newRight, join.getCondition, join.getHints, join.getJoinType)
  }
}

object FlinkLogicalJoin {
  val CONVERTER: ConverterRule = new FlinkLogicalJoinConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalJoin],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalJoinConverter"))

  def create(
      left: RelNode,
      right: RelNode,
      conditionExpr: RexNode,
      hints: JList[RelHint],
      joinType: JoinRelType): FlinkLogicalJoin = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalJoin(cluster, traitSet, left, right, conditionExpr, hints, joinType)
  }
}
