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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties

import org.apache.calcite.rex.{RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableIntList
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Util for window join.
 */
object WindowJoinUtil {

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
   * Checks whether join condition contains window starts equality of input tables and window
   * ends equality of input tables.
   *
   * @param join input join
   * @return True if join condition contains window starts equality of input tables and window
   *         ends equality of input tables, else false.
   */
  def containsWindowStartEqualityAndEndEquality(join: FlinkLogicalJoin): Boolean = {
    val (windowStartEqualityLeftKeys, windowEndEqualityLeftKeys, _, _) =
      excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join)
    windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty
  }

  /**
   * Excludes window starts equality and window ends equality from join info.
   *
   * @param join input join
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
  def excludeWindowStartEqualityAndEndEqualityFromJoinCondition(
      join: FlinkLogicalJoin): (
    ImmutableIntList,
    ImmutableIntList,
    ImmutableIntList,
    ImmutableIntList,
    ImmutableIntList,
    ImmutableIntList,
    RexNode) = {
    val (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys) =
      excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join)

    val joinInfo = join.analyzeCondition()
    val (remainLeftKeys, remainRightKeys, remainCondition) = if (
      windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val joinFieldsType = join.getRowType.getFieldList
      val leftFieldCnt = join.getLeft.getRowType.getFieldCount
      val rexBuilder = join.getCluster.getRexBuilder
      val remainEquals = mutable.ArrayBuffer[RexNode]()
      val remainLeftKeysArray = mutable.ArrayBuffer[Int]()
      val remainRightKeysArray = mutable.ArrayBuffer[Int]()
      // convert remain pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS calls
      joinInfo.pairs().foreach { p =>
        if (!windowStartEqualityLeftKeys.contains(p.source) &&
          !windowEndEqualityLeftKeys.contains(p.source)) {
          val leftFieldType = joinFieldsType.get(p.source).getType
          val leftInputRef = new RexInputRef(p.source, leftFieldType)
          val rightIndex = leftFieldCnt + p.target
          val rightFieldType = joinFieldsType.get(rightIndex).getType
          val rightInputRef = new RexInputRef(rightIndex, rightFieldType)
          val remainEqual = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            leftInputRef,
            rightInputRef)
          remainEquals.add(remainEqual)
          remainLeftKeysArray.add(p.source)
          remainRightKeysArray.add(p.target)
        }
      }
      val remainAnds = remainEquals ++ joinInfo.nonEquiConditions
      (
        toImmutableIntList(remainLeftKeysArray),
        toImmutableIntList(remainRightKeysArray),
        // build a new condition
        RexUtil.composeConjunction(rexBuilder, remainAnds.toList))
    } else {
      (joinInfo.leftKeys, joinInfo.rightKeys, join.getCondition)
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
   * Excludes window start equality and window end equality from JoinInfo pairs.
   *
   * @param join  the join to split
   * @return The remain part of JoinInfo pairs after excludes window start equality and window
   *         end equality from JoinInfo pairs.
   *         The first element is left join keys of window starts equality,
   *         the second element is left join keys of window ends equality,
   *         the third element is right join keys of window starts equality,
   *         the forth element is right join keys of window ends equality.
   */
  private def excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(
      join: FlinkLogicalJoin): (
    ImmutableIntList, ImmutableIntList, ImmutableIntList, ImmutableIntList) = {
    val joinInfo = join.analyzeCondition()
    val (leftWindowProperties, rightWindowProperties) = getChildWindowProperties(join)

    val windowStartEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowStartEqualityRightKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityRightKeys = mutable.ArrayBuffer[Int]()

    if (leftWindowProperties != null && rightWindowProperties != null) {
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
    }

    // Validate join
    if (windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty) {
      if (
        leftWindowProperties.getTimeAttributeType != rightWindowProperties.getTimeAttributeType) {
        throw new TableException(
          "Currently, window join doesn't support different time attribute type of left and " +
            "right inputs.\n" +
            s"The left time attribute type is " +
            s"${leftWindowProperties.getTimeAttributeType}.\n" +
            s"The right time attribute type is " +
            s"${rightWindowProperties.getTimeAttributeType}.")
      } else if (leftWindowProperties.getWindowSpec != rightWindowProperties.getWindowSpec) {
        throw new TableException(
          "Currently, window join doesn't support different window table function of left and " +
            "right inputs.\n" +
            s"The left window table function is ${leftWindowProperties.getWindowSpec}.\n" +
            s"The right window table function is ${rightWindowProperties.getWindowSpec}.")
      }
    } else if (windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val leftFieldNames = join.getLeft.getRowType.getFieldNames.toList
      val rightFieldNames = join.getRight.getRowType.getFieldNames.toList
      val inputFieldNames = leftFieldNames ++ rightFieldNames
      val condition = join.getExpressionString(
        join.getCondition,
        inputFieldNames,
        None,
        ExpressionFormat.Infix)
      throw new TableException(
        "Currently, window join requires JOIN ON condition must contain both window starts " +
          "equality of input tables and window ends equality of input tables.\n" +
          s"But the current JOIN ON condition is $condition.")
    }

    (
      toImmutableIntList(windowStartEqualityLeftKeys),
      toImmutableIntList(windowEndEqualityLeftKeys),
      toImmutableIntList(windowStartEqualityRightKeys),
      toImmutableIntList(windowEndEqualityRightKeys)
    )
  }

  private def toImmutableIntList(seq: Seq[Int]): ImmutableIntList = ImmutableIntList.of(seq: _*)

}
