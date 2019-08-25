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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.{RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexInputRef, RexLiteral}
import org.apache.flink.table.runtime.aggregate.AggregateUtil._

import scala.collection.JavaConverters._

trait OverAggregate {

  private[flink] def partitionToString(inputType: RelDataType, partition: Array[Int]): String = {
    val inFields = inputType.getFieldNames.asScala
    partition.map( inFields(_) ).mkString(", ")
  }

  private[flink] def orderingToString(
      inputType: RelDataType,
      orderFields: java.util.List[RelFieldCollation]): String = {

    val inFields = inputType.getFieldList.asScala

    val orderingString = orderFields.asScala.map {
      x => inFields(x.getFieldIndex).getName
    }.mkString(", ")

    orderingString
  }

  private[flink] def windowRange(
      logicWindow: Window,
      overWindow: Group,
      input: RelNode): String = {
    if (overWindow.lowerBound.isPreceding && !overWindow.lowerBound.isUnbounded) {
      s"BETWEEN ${getLowerBoundary(logicWindow, overWindow, input)} PRECEDING " +
          s"AND ${overWindow.upperBound}"
    } else {
      s"BETWEEN ${overWindow.lowerBound} AND ${overWindow.upperBound}"
    }
  }

  private[flink] def aggregationToString(
      inputType: RelDataType,
      constants: Seq[RexLiteral],
      rowType: RelDataType,
      namedAggregates: Seq[CalcitePair[AggregateCall, String]]): String = {

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

    (inFields ++ aggStrings).zip(outFields).map {
      case (f, o) => if (f == o) {
        f
      } else {
        s"$f AS $o"
      }
    }.mkString(", ")
  }

  private[flink] def getLowerBoundary(
      logicWindow: Window,
      overWindow: Group,
      input: RelNode): Long = {

    val ref: RexInputRef = overWindow.lowerBound.getOffset.asInstanceOf[RexInputRef]
    val lowerBoundIndex = ref.getIndex - input.getRowType.getFieldCount
    val lowerBound = logicWindow.constants.get(lowerBoundIndex).getValue2
    lowerBound match {
      case x: java.math.BigDecimal => x.asInstanceOf[java.math.BigDecimal].longValue()
      case _ => lowerBound.asInstanceOf[Long]
    }
  }

}
