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

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.JDouble
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil.satisfyIntervalJoin
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.satisfyTemporalJoin
import org.apache.flink.table.planner.plan.utils.WindowJoinUtil.satisfyWindowJoin
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.ImmutableIntList

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/** Util for [[Join]]s. */
object JoinUtil {

  /** Create [[JoinSpec]] according to the given join. */
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

  /** Validates that join keys in [[JoinSpec]] is compatible in both sides of join. */
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
    (0 until joinSpec.getJoinKeySize).foreach {
      idx =>
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
   * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]], the only difference is
   * that the methods could return `filterNulls`.
   */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining =
      RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

  def generateConditionFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      joinSpec: JoinSpec,
      leftType: LogicalType,
      rightType: LogicalType): GeneratedJoinCondition = {
    generateConditionFunction(
      tableConfig,
      classLoader,
      joinSpec.getNonEquiCondition.orElse(null),
      leftType,
      rightType)
  }

  def generateConditionFunction(
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      nonEquiCondition: RexNode,
      leftType: LogicalType,
      rightType: LogicalType): GeneratedJoinCondition = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
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

    FunctionCodeGenerator.generateJoinCondition(ctx, "ConditionFunction", body)
  }

  def analyzeJoinInput(
      classLoader: ClassLoader,
      inputTypeInfo: InternalTypeInfo[RowData],
      joinKeys: Array[Int],
      uniqueKeys: util.List[Array[Int]]): JoinInputSideSpec = {

    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      JoinInputSideSpec.withoutUniqueKey
    } else {
      val joinKeySet = new util.HashSet[Integer]
      joinKeys.map(Int.box).foreach(joinKeySet.add)
      val uniqueKeysContainedByJoinKey = uniqueKeys
        .filter((uk: Array[Int]) => joinKeySet.containsAll(uk.toList))

      if (uniqueKeysContainedByJoinKey.isEmpty) {
        val smallestUniqueKey = getSmallestKey(uniqueKeys)
        val uniqueKeySelector =
          KeySelectorUtil.getRowDataSelector(classLoader, smallestUniqueKey, inputTypeInfo)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKey(uniqueKeyTypeInfo, uniqueKeySelector)
      } else {
        // join key contains unique key
        val smallestUniqueKey = getSmallestKey(uniqueKeysContainedByJoinKey)
        val uniqueKeySelector =
          KeySelectorUtil.getRowDataSelector(classLoader, smallestUniqueKey, inputTypeInfo)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(uniqueKeyTypeInfo, uniqueKeySelector)
      }
    }
  }

  private def getSmallestKey(keys: util.List[Array[Int]]) = {
    keys.reduce((k1, k2) => if (k1.length <= k2.length) k1 else k2)
  }

  /**
   * Checks if an expression accesses a time attribute.
   *
   * @param expr
   *   The expression to check.
   * @param inputType
   *   The input type of the expression.
   * @return
   *   True, if the expression accesses a time attribute. False otherwise.
   */
  def accessesTimeAttribute(expr: RexNode, inputType: RelDataType): Boolean = {
    expr match {
      case ref: RexInputRef =>
        val accessedType = inputType.getFieldList.get(ref.getIndex).getType
        FlinkTypeFactory.isTimeIndicatorType(accessedType)
      case c: RexCall =>
        c.operands.exists(accessesTimeAttribute(_, inputType))
      case _ => false
    }
  }

  /**
   * Combines join inputs' RowType. For SEMI/ANTI join, the result is different from join's RowType.
   * For other joinType, the result is same with join's RowType.
   *
   * @param join
   * @return
   */
  def combineJoinInputsRowType(join: Join): RelDataType = join.getJoinType match {
    case JoinRelType.SEMI | JoinRelType.ANTI =>
      SqlValidatorUtil.createJoinType(
        join.getCluster.getTypeFactory,
        join.getLeft.getRowType,
        join.getRight.getRowType,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case _ =>
      join.getRowType
  }

  /**
   * Check whether input join node satisfy preconditions to convert into regular join.
   *
   * @param join
   *   input join to analyze.
   * @param newLeft
   *   new left child of join
   * @param newRight
   *   new right child of join
   *
   * @return
   *   True if input join node satisfy preconditions to convert into regular join, else false.
   */
  def satisfyRegularJoin(join: FlinkLogicalJoin, newLeft: RelNode, newRight: RelNode): Boolean = {
    if (newRight.isInstanceOf[FlinkLogicalSnapshot]) {
      // exclude lookup join
      false
    } else if (satisfyTemporalJoin(join, newLeft, newRight)) {
      // exclude temporal table join
      false
    } else if (satisfyIntervalJoin(join, newLeft, newRight)) {
      // exclude interval join
      false
    } else if (satisfyWindowJoin(join, newLeft, newRight)) {
      // exclude window join
      false
    } else {
      true
    }
  }

  def binaryRowRelNodeSize(relNode: RelNode): JDouble = {
    val mq = relNode.getCluster.getMetadataQuery
    val rowCount = mq.getRowCount(relNode)
    if (rowCount == null) {
      null
    } else {
      rowCount * FlinkRelMdUtil.binaryRowAverageSize(relNode)
    }
  }
}
