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
import org.apache.flink.table.planner.JArrayList
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec.GroupSpec
import org.apache.flink.table.planner.plan.nodes.exec.spec.{OverSpec, PartitionSpec}

import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexWindowBound}
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object OverAggregateUtil {

  /**
   * Convert [[Window]] to [[OverSpec]].
   */
  def createOverSpec(logicalWindow: Window): OverSpec = {
    val groups = logicalWindow.groups.asList()
    val partition = new PartitionSpec(groups.head.keys.toArray)
    groups.tail.foreach { g =>
      if (!partition.equals(new PartitionSpec(g.keys.toArray))) {
        throw new TableException("OverSpec requires all groups should have same partition key.")
      }
    }

    new OverSpec(
      partition,
      groups.map(createGroupSpec(_, logicalWindow)),
      logicalWindow.constants.asList(),
      calcOriginalInputFields(logicalWindow)
    )
  }

  /**
   * Convert [[Window.Group]] to [[GroupSpec]].
   */
  def createGroupSpec(windowGroup: Window.Group, window: Window): GroupSpec = {
    new GroupSpec(
      SortUtil.getSortSpec(windowGroup.orderKeys.getFieldCollations),
      windowGroup.isRows,
      windowGroup.lowerBound,
      windowGroup.upperBound,
      windowGroup.getAggregateCalls(window))
  }

  /**
   * Split the given window groups into different groups based on window framing flag.
   */
  def splitOutOffsetOrInsensitiveGroup(windowGroups: Seq[Window.Group]): Seq[Window.Group] = {

    def compareAggCall(c1: Window.RexWinAggCall, c2: Window.RexWinAggCall): Boolean = {
      val allowsFraming1 = c1.getOperator.allowsFraming
      val allowsFraming2 = c2.getOperator.allowsFraming
      if (!allowsFraming1 && !allowsFraming2) {
        c1.getOperator.getClass == c2.getOperator.getClass
      } else {
        allowsFraming1 == allowsFraming2
      }
    }

    def createNewGroup(
        windowGroup: Window.Group,
        newAggCalls: Seq[Window.RexWinAggCall]): Window.Group = {
      new Window.Group(
        windowGroup.keys,
        windowGroup.isRows,
        windowGroup.lowerBound,
        windowGroup.upperBound,
        windowGroup.orderKeys,
        newAggCalls)
    }

    val newWindowGroups = ArrayBuffer[Window.Group]()
    windowGroups.foreach { group =>
      var lastAggCall: Window.RexWinAggCall = null
      val aggCallsBuffer = ArrayBuffer[Window.RexWinAggCall]()
      group.aggCalls.foreach { aggCall =>
        if (lastAggCall != null && !compareAggCall(lastAggCall, aggCall)) {
          newWindowGroups.add(createNewGroup(group, aggCallsBuffer))
          aggCallsBuffer.clear()
        }
        aggCallsBuffer.add(aggCall)
        lastAggCall = aggCall
      }
      if (aggCallsBuffer.nonEmpty) {
        newWindowGroups.add(createNewGroup(group, aggCallsBuffer))
        aggCallsBuffer.clear()
      }
    }
    newWindowGroups
  }

  def calcOriginalInputFields(logicWindow: Window): Int = {
    logicWindow.getRowType.getFieldCount - logicWindow.groups.flatMap(_.aggCalls).size
  }

  /**
   * Calculate the bound value and cast to long value.
   */
  def getLongBoundary(overSpec: OverSpec, windowBound: RexWindowBound): Long = {
    getBoundary(overSpec, windowBound).asInstanceOf[Long].longValue()
  }

  /**
   * Calculate the bound value.
   * The return type only is Long for the ROWS OVER WINDOW.
   * The return type can be Long or BigDecimal for the RANGE OVER WINDOW.
   * NOTE: returns a signed value, considering whether it is preceding.
   */
  def getBoundary(logicWindow: Window, windowBound: RexWindowBound): Any = {
    getBoundary(windowBound, logicWindow.getConstants, calcOriginalInputFields(logicWindow))
  }

  /**
   * Calculate the bound value.
   * The return type only is Long for the ROWS OVER WINDOW.
   * The return type can be Long or BigDecimal for the RANGE OVER WINDOW.
   * NOTE: returns a signed value, considering whether it is preceding.
   */
  def getBoundary(overSpec: OverSpec, windowBound: RexWindowBound): Any = {
    getBoundary(windowBound, overSpec.getConstants, overSpec.getOriginalInputFields)
  }

  /**
   * Calculate the bound value.
   * The return type only is Long for the ROWS OVER WINDOW.
   * The return type can be Long or BigDecimal for the RANGE OVER WINDOW.
   * NOTE: returns a signed value, considering whether it is preceding.
   */
  private def getBoundary(
      windowBound: RexWindowBound,
      constants: Seq[RexLiteral],
      originalInputFields: Int): Any = {
    if (windowBound.isCurrentRow) {
      0L
    } else {
      val ref = windowBound.getOffset.asInstanceOf[RexInputRef]
      val boundIndex = ref.getIndex - originalInputFields
      val flag = if (windowBound.isPreceding) -1 else 1
      val literal = constants.get(boundIndex)
      literal.getType.getSqlTypeName match {
        case _@SqlTypeName.DECIMAL =>
          literal.getValue3.asInstanceOf[java.math.BigDecimal].multiply(
            new java.math.BigDecimal(flag))
        case _ => literal.getValueAs(classOf[java.lang.Long]) * flag
      }
    }
  }

  def createCollation(windowGroup: Window.Group): RelCollation = {
    val groupSet: Array[Int] = windowGroup.keys.toArray
    val collations = windowGroup.orderKeys.getFieldCollations
    val orderKeyIndexes = SortUtil.getSortSpec(collations).getFieldIndices
    if (groupSet.nonEmpty || orderKeyIndexes.nonEmpty) {
      val collectionIndexes = collations.map(_.getFieldIndex)
      val intersectIds = orderKeyIndexes.intersect(groupSet)
      val groupCollation = groupSet.map { idx =>
        if (intersectIds.contains(idx)) {
          val index = collectionIndexes.indexOf(idx)
          val collation = collations.get(index)
          (collation.getFieldIndex, collation.getDirection, collation.nullDirection)
        } else {
          (idx, Direction.ASCENDING, NullDirection.FIRST)
        }
      }
      // orderCollation should filter those order keys which are contained by groupSet.
      val orderCollation = collations.filter { c =>
        !intersectIds.contains(c.getFieldIndex)
      }.map { c =>
        (c.getFieldIndex, c.getDirection, c.nullDirection)
      }
      val fields = new JArrayList[RelFieldCollation]()
      for (field <- groupCollation ++ orderCollation) {
        fields.add(FlinkRelOptUtil.ofRelFieldCollation(field._1, field._2, field._3))
      }
      RelCollations.of(fields)
    } else {
      RelCollations.EMPTY
    }
  }

  def needCollationTrait(logicalWindow: Window, windowGroup: Window.Group): Boolean = {
    if (windowGroup.lowerBound.isPreceding ||
      windowGroup.upperBound.isFollowing || !windowGroup.isRows) {
      true
    } else {
      // rows over window
      val offsetLower = getBoundary(
        windowGroup.lowerBound,
        logicalWindow.constants.toList,
        calcOriginalInputFields(logicalWindow)).asInstanceOf[Long]
      val offsetUpper = getBoundary(
        windowGroup.upperBound,
        logicalWindow.constants.toList,
        calcOriginalInputFields(logicalWindow)).asInstanceOf[Long]
      if (offsetLower == 0L && offsetUpper == 0L &&
        windowGroup.orderKeys.getFieldCollations.isEmpty) {
        false
      } else {
        true
      }
    }
  }
}
