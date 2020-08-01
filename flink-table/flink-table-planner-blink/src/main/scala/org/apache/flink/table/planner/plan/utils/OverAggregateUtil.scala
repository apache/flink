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

import org.apache.flink.table.planner.JArrayList

import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation}
import org.apache.calcite.rex.{RexInputRef, RexWindowBound}
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.JavaConversions._

object OverAggregateUtil {

  def calcOriginInputRows(logicWindow: Window): Int = {
    logicWindow.getRowType.getFieldCount - logicWindow.groups.flatMap(_.aggCalls).size
  }

  /**
    * Calculate the bound value and cast to long value.
    */
  def getLongBoundary(logicWindow: Window, windowBound: RexWindowBound): Long = {
    getBoundary(logicWindow, windowBound).asInstanceOf[Long].longValue()
  }

  /**
    * Calculate the bound value.
    * The return type only is Long for the ROWS OVER WINDOW.
    * The return type can be Long or BigDecimal for the RANGE OVER WINDOW.
    * NOTE: returns a signed value, considering whether it is preceding.
    */
  def getBoundary(logicWindow: Window, windowBound: RexWindowBound): Any = {
    if (windowBound.isCurrentRow) {
      0L
    } else {
      val ref = windowBound.getOffset.asInstanceOf[RexInputRef]
      val boundIndex = ref.getIndex - calcOriginInputRows(logicWindow)
      val flag = if (windowBound.isPreceding) -1 else 1
      val literal = logicWindow.constants.get(boundIndex)
      literal.getType.getSqlTypeName match {
        case _@SqlTypeName.DECIMAL =>
          literal.getValue3.asInstanceOf[java.math.BigDecimal].multiply(
            new java.math.BigDecimal(flag))
        case _ => literal.getValueAs(classOf[java.lang.Long]) * flag
      }
    }
  }

  def createCollation(group: Group): RelCollation = {
    val groupSet: Array[Int] = group.keys.toArray
    val collations = group.orderKeys.getFieldCollations
    val (orderKeyIndexes, _, _) = SortUtil.getKeysAndOrders(collations)
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

  def needCollationTrait(overWindow: Window, group: Group): Boolean = {
    if (group.lowerBound.isPreceding || group.upperBound.isFollowing || !group.isRows) {
      true
    } else {
      // rows over window
      val offsetLower = getBoundary(overWindow, group.lowerBound).asInstanceOf[Long]
      val offsetUpper = getBoundary(overWindow, group.upperBound).asInstanceOf[Long]
      if (offsetLower == 0L && offsetUpper == 0L && group.orderKeys.getFieldCollations.isEmpty) {
        false
      } else {
        true
      }
    }
  }
}
