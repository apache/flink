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

import org.apache.flink.table.plan.nodes.ExpressionFormat
import org.apache.flink.table.plan.nodes.ExpressionFormat.ExpressionFormat

import org.apache.calcite.rex.{RexNode, RexProgram}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CalcUtil {

  private[flink] def calcToString(
    calcProgram: RexProgram,
    f: (RexNode, List[String], Option[List[RexNode]], ExpressionFormat) => String): String = {
    val inFields = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList
    val selectionStr = selectionToString(calcProgram, f, ExpressionFormat.Infix)
    val cond = calcProgram.getCondition
    val name = s"${
      if (cond != null) {
        s"where: ${
          f(cond, inFields, Some(localExprs), ExpressionFormat.Infix)}, "
      } else {
        ""
      }
    }select: ($selectionStr)"
    s"Calc($name)"
  }

  private[flink] def conditionToString(
    calcProgram: RexProgram,
    expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    val cond = calcProgram.getCondition
    val inFields = calcProgram.getInputRowType.getFieldNames.asScala.toList
    val localExprs = calcProgram.getExprList.asScala.toList

    if (cond != null) {
      expression(cond, inFields, Some(localExprs))
    } else {
      ""
    }
  }

  private[flink] def selectionToString(
    calcProgram: RexProgram,
    expression: (RexNode, List[String], Option[List[RexNode]], ExpressionFormat) => String,
    expressionFormat: ExpressionFormat = ExpressionFormat.Prefix): String = {

    val proj = calcProgram.getProjectList.asScala.toList
    val inFields = calcProgram.getInputRowType.getFieldNames.asScala.toList
    val localExprs = calcProgram.getExprList.asScala.toList
    val outFields = calcProgram.getOutputRowType.getFieldNames.asScala.toList

    proj
      .map(expression(_, inFields, Some(localExprs), expressionFormat))
      .zip(outFields).map { case (e, o) =>
      if (e != o) {
        e + " AS " + o
      } else {
        e
      }
    }.mkString(", ")
  }

  private[flink] def isDeterministic(program: RexProgram): Boolean = {
    if (program.getCondition != null) {
      val condition = program.expandLocalRef(program.getCondition)
      if (!FlinkRexUtil.isDeterministicOperator(condition)) {
        return false
      }
    }
    val projection = program.getProjectList.map(program.expandLocalRef)
    projection.forall(p => FlinkRexUtil.isDeterministicOperator(p))
  }
}
