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

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties
import org.apache.flink.table.planner.utils.Logging

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Util for window join.
 */
object WindowJoinUtil extends Logging {

  /**
   * Get window properties of left and right child.
   *
   * @param join input join
   * @return window properties of left and right child.
   */
  def getChildWindowProperties(
      join: FlinkLogicalJoin): (RelWindowProperties, RelWindowProperties) = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(join.getCluster.getMetadataQuery)
    (fmq.getRelWindowProperties(join.getLeft), fmq.getRelWindowProperties(join.getRight))
  }

  /**
   * Checks whether join could be translated to windowJoin. it needs satisfy all the following
   * 4 conditions:
   * 1) both two input nodes has window properties
   * 2) time attribute type of left input node is equals to the one of right input node
   * 3) window specification of left input node is equals to the one of right input node
   * 4) join condition contains window starts equality of input tables and window
   * ends equality of input tables
   *
   * @param join input join
   * @return True if join condition contains window starts equality of input tables and window
   *         ends equality of input tables, else false.
   */
  def satisfyWindowJoin(join: FlinkLogicalJoin): Boolean = {
    satisfyWindowJoin(join, join.getLeft, join.getRight)
  }

  def satisfyWindowJoin(join: FlinkLogicalJoin, newLeft: RelNode, newRight: RelNode): Boolean = {
    excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join, newLeft, newRight) match {
      case Some((windowStartEqualityLeftKeys, windowEndEqualityLeftKeys, _, _)) =>
        windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty
      case _ => false
    }
  }

  /**
   * Excludes window starts equality and window ends equality from join info.
   *
   * @param join a join which could be translated to window join, please see
   *             [[WindowJoinUtil.satisfyWindowJoin()]].
   * @return Remain join information after excludes window starts equality and window ends
   *         equality from join.
   *         The first element is left join keys of window starts equality,
   *         the second element is left join keys of window ends equality,
   *         the third element is right join keys of window starts equality,
   *         the forth element is right join keys of window ends equality,
   *         the fifth element is remain left join keys,
   *         the sixth element is remain right join keys,
   *         the last element is remain join condition which includes remain equal condition and
   *         non-equal condition.
   */
  def excludeWindowStartEqualityAndEndEqualityFromWindowJoinCondition(
      join: FlinkLogicalJoin): (
    Array[Int], Array[Int], Array[Int], Array[Int], Array[Int], Array[Int], RexNode) = {
    val analyzeResult =
      excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join)
    if (analyzeResult.isEmpty) {
      throw new IllegalArgumentException(
        "Pleas give a Join which could be translated to window join!")
    }
    val (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys) = analyzeResult.get
    val joinSpec = JoinUtil.createJoinSpec(join)
    val (remainLeftKeys, remainRightKeys, remainCondition) = if (
      windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val leftChildFieldsType = join.getLeft.getRowType.getFieldList
      val rightChildFieldsType = join.getRight.getRowType.getFieldList
      val leftFieldCnt = join.getLeft.getRowType.getFieldCount
      val rexBuilder = join.getCluster.getRexBuilder
      val remainingConditions = mutable.ArrayBuffer[RexNode]()
      val remainLeftKeysArray = mutable.ArrayBuffer[Int]()
      val remainRightKeysArray = mutable.ArrayBuffer[Int]()
      // convert remain pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS calls
      // or SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
      joinSpec.getLeftKeys.zip(joinSpec.getRightKeys).
        zip(joinSpec.getFilterNulls).foreach { case ((source, target), filterNull) =>
        if (!windowStartEqualityLeftKeys.contains(source) &&
          !windowEndEqualityLeftKeys.contains(source)) {
          val leftFieldType = leftChildFieldsType.get(source).getType
          val leftInputRef = new RexInputRef(source, leftFieldType)
          val rightFieldType = rightChildFieldsType.get(target).getType
          val rightIndex = leftFieldCnt + target
          val rightInputRef = new RexInputRef(rightIndex, rightFieldType)
          val op = if (filterNull) {
            SqlStdOperatorTable.EQUALS
          } else {
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
          }
          val remainEqual = rexBuilder.makeCall(op, leftInputRef, rightInputRef)
          remainingConditions += remainEqual
          remainLeftKeysArray += source
          remainRightKeysArray += target
        }
      }
      val notEquiCondition = joinSpec.getNonEquiCondition
      if (notEquiCondition.isPresent) {
        remainingConditions += notEquiCondition.get()
      }
      (
        remainLeftKeysArray.toArray,
        remainRightKeysArray.toArray,
        // build a new condition
        RexUtil.composeConjunction(rexBuilder, remainingConditions.toList))
    } else {
      (joinSpec.getLeftKeys, joinSpec.getRightKeys, join.getCondition)
    }

    (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys,
      remainLeftKeys,
      remainRightKeys,
      remainCondition)
  }

  /**
   * Analyzes input join node. If the join could be translated to windowJoin, excludes window
   * start equality and window end equality from JoinInfo pairs.
   *
   * @param join  the join to split
   * @return If the join could not translated to window join, the result
   *         is [[Option.empty]]; else the result contains a tuple with 4 arrays.
   *         1) The first array contains left join keys of window starts equality,
   *         2) the second array contains left join keys of window ends equality,
   *         3) the third array contains right join keys of window starts equality,
   *         4) the forth array contains right join keys of window ends equality.
   */
  private def excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(
      join: FlinkLogicalJoin): Option[(Array[Int], Array[Int], Array[Int], Array[Int])] = {
    excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join, join.getLeft, join.getRight)
  }

  private def excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(
      join: FlinkLogicalJoin,
      newLeft: RelNode,
      newRight: RelNode): Option[(Array[Int], Array[Int], Array[Int], Array[Int])] = {
    val joinInfo = join.analyzeCondition()
    val (leftWindowProperties, rightWindowProperties) = getChildWindowProperties(join)

    if (leftWindowProperties == null || rightWindowProperties == null) {
      return Option.empty
    }
    val windowStartEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowStartEqualityRightKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityRightKeys = mutable.ArrayBuffer[Int]()

    val leftWindowStartColumns = leftWindowProperties.getWindowStartColumns
    val rightWindowStartColumns = rightWindowProperties.getWindowStartColumns
    val leftWindowEndColumns = leftWindowProperties.getWindowEndColumns
    val rightWindowEndColumns = rightWindowProperties.getWindowEndColumns

    joinInfo.pairs().foreach { pair =>
      val leftKey = pair.source
      val rightKey = pair.target
      if (leftWindowStartColumns.get(leftKey) && rightWindowStartColumns.get(rightKey)) {
        windowStartEqualityLeftKeys.add(leftKey)
        windowStartEqualityRightKeys.add(rightKey)
      } else if (leftWindowEndColumns.get(leftKey) && rightWindowEndColumns.get(rightKey)) {
        windowEndEqualityLeftKeys.add(leftKey)
        windowEndEqualityRightKeys.add(rightKey)
      }
    }

    // Validate join
    if (windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty) {
      if (
        leftWindowProperties.getTimeAttributeType != rightWindowProperties.getTimeAttributeType) {
        LOG.warn(
          "Currently, window join doesn't support different time attribute type of left and " +
            "right inputs.\n" +
            s"The left time attribute type is " +
            s"${leftWindowProperties.getTimeAttributeType}.\n" +
            s"The right time attribute type is " +
            s"${rightWindowProperties.getTimeAttributeType}.")
        Option.empty
      } else if (leftWindowProperties.getWindowSpec != rightWindowProperties.getWindowSpec) {
        LOG.warn(
          "Currently, window join doesn't support different window table function of left and " +
            "right inputs.\n" +
            s"The left window table function is ${leftWindowProperties.getWindowSpec}.\n" +
            s"The right window table function is ${rightWindowProperties.getWindowSpec}.")
        Option.empty
      } else {
        Option(
          windowStartEqualityLeftKeys.toArray,
          windowEndEqualityLeftKeys.toArray,
          windowStartEqualityRightKeys.toArray,
          windowEndEqualityRightKeys.toArray
        )
      }
    } else if (windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val leftFieldNames = newLeft.getRowType.getFieldNames.toList
      val rightFieldNames = newRight.getRowType.getFieldNames.toList
      val inputFieldNames = leftFieldNames ++ rightFieldNames
      val condition = join.getExpressionString(
        join.getCondition,
        inputFieldNames,
        None,
        ExpressionFormat.Infix)
      LOG.warn(
        "Currently, window join requires JOIN ON condition must contain both window starts " +
          "equality of input tables and window ends equality of input tables.\n" +
          s"But the current JOIN ON condition is $condition.")
      Option.empty
    } else {
      Option.empty
    }
  }
}
