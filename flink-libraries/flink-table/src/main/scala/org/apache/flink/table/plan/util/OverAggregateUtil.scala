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

package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.util.AggregateUtil._
import org.apache.flink.table.runtime.aggregate.RelFieldCollations

import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.{RelCollation, RelCollations, RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexWindowBound}
import org.apache.calcite.sql.`type`.SqlTypeName

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object OverAggregateUtil {

  def partitionToString(inputType: RelDataType, partition: Array[Int]): String = {
    val inFields = inputType.getFieldNames.asScala
    partition.map( inFields(_) ).mkString(", ")
  }

  def orderingToString(
      inputType: RelDataType,
      orderFields: java.util.List[RelFieldCollation]): String = {
    val inFields = inputType.getFieldList.asScala
    val orderingString = orderFields.asScala.map {
      x => s"${inFields(x.getFieldIndex).getName} ${x.direction.shortString}"
    }.mkString(", ")
    orderingString
  }

  def windowRangeToString(
      logicWindow: Window,
      groupWindow: Group): String = {

    def boundString(bound: RexWindowBound): String = {
      if (bound.getOffset != null) {
        val ref = bound.getOffset.asInstanceOf[RexInputRef]
        val boundIndex = ref.getIndex - calcOriginInputRows(logicWindow)
        val offset = logicWindow.constants.get(boundIndex).getValue2
        val offsetKind = if (bound.isPreceding) "PRECEDING" else "FOLLOWING"
        s"$offset $offsetKind"
      } else {
        bound.toString
      }
    }

    val buf = new StringBuilder
    buf.append(if (groupWindow.isRows) " ROWS " else " RANG ")
    val lowerBound = groupWindow.lowerBound
    val upperBound = groupWindow.upperBound
    if (lowerBound != null) {
      if (upperBound != null) {
        buf.append("BETWEEN ")
        buf.append(boundString(lowerBound))
        buf.append(" AND ")
        buf.append(boundString(upperBound))
      }
      else {
        buf.append(boundString(lowerBound))
      }
    } else if (upperBound != null) {
      buf.append(boundString(upperBound))
    }
    buf.toString
  }

  def aggregationToString(
      inputType: RelDataType,
      constants: Seq[RexLiteral],
      rowType: RelDataType,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]],
      outputInputName: Boolean = true,
      rowTypeOffset: Int = 0): String = {

    val inFields = inputType.getFieldNames.asScala
    val outFields = rowType.getFieldNames.asScala

    val aggStrings = namedAggregates.map(_.getKey).map(
      a => s"${a.getAggregation}(${
        val prefix = if (a.isDistinct) {
          "DISTINCT "
        } else {
          ""
        }
        prefix + (if (a.getArgList.size() > 0) {
          a.getArgList.asScala.map { arg =>
            // index to constant
            if (arg >= inputType.getFieldCount) {
              constants(arg - inputType.getFieldCount)
            }
            // index to input field
            else {
              inFields(arg)
            }
          }.mkString(", ")
        } else {
          "*"
        })
      })")

    val output = if (outputInputName) inFields ++ aggStrings else aggStrings
    output.zip(outFields.drop(rowTypeOffset)).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

  def calcOriginInputRows(logicWindow: Window): Int = {
    logicWindow.getRowType.getFieldCount - logicWindow.groups.flatMap(_.aggCalls).size
  }

  /**
   * Calculate the bound value.
   * The return type only is Long for the ROWS OVER WINDOW.
   * The return type can be Long or BigDecimal for the RANGE OVER WINDOW.
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

  def createFlinkRelCollation(group: Group): RelCollation = {
    val groupSet: Array[Int] = group.keys.toArray
    val collections = group.orderKeys.getFieldCollations
    val (orderKeyIdxs, _, _) = SortUtil.getKeysAndOrders(collections)
    if (groupSet.nonEmpty || orderKeyIdxs.nonEmpty) {
      val collectionIndexes = collections.map(_.getFieldIndex)
      val intersectIds = orderKeyIdxs.intersect(groupSet)
      val groupCollation = groupSet.map { idx =>
        if (intersectIds.contains(idx)) {
          val index = collectionIndexes.indexOf(idx)
          (collections.get(index).getFieldIndex, collections.get(index).getDirection,
              collections.get(index).nullDirection)
        } else {
          (idx, Direction.ASCENDING, NullDirection.FIRST)
        }
      }
      //orderCollation should filter those order keys which are contained by groupSet.
      val orderCollation = collections.filter(collection =>
        !intersectIds.contains(collection.getFieldIndex)).map { collo =>
        (collo.getFieldIndex, collo.getDirection, collo.nullDirection)
      }
      val fields = new JArrayList[RelFieldCollation]()
      for (field <- groupCollation ++ orderCollation) {
        fields.add(RelFieldCollations.of(field._1, field._2, field._3))
      }
      RelCollations.of(fields)
    } else {
      RelCollations.EMPTY
    }
  }

  private[flink] def needCollationTrait(
      input: RelNode,
      overWindow: Window,
      group: Group): Boolean = {
    if (group.lowerBound.isPreceding || group.upperBound.isFollowing || !group.isRows) {
      true
    } else {
      //rows over window
      val offsetLower = OverAggregateUtil.getBoundary(
        overWindow, group.lowerBound).asInstanceOf[Long]
      val offsetUpper = OverAggregateUtil.getBoundary(
        overWindow, group.upperBound).asInstanceOf[Long]
      if (offsetLower == 0L && offsetUpper == 0L && group.orderKeys.getFieldCollations.isEmpty) {
        false
      } else {
        true
      }
    }
  }

  def isDeterministic(groups: JList[Window.Group]): Boolean = {
    groups.forall(g => g.aggCalls.forall(FlinkRexUtil.isDeterministicOperator))
  }

}
