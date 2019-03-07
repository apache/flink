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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner}
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode, RexProgram}

import scala.collection.JavaConversions._

/**
  * Utility methods for calc operator.
  */
object CalcUtil {

  private[flink] def computeCost(
      calcProgram: RexProgram,
      planner: RelOptPlanner,
      mq: RelMetadataQuery,
      calc: Calc): RelOptCost = {
    // compute number of expressions that do not access a field or literal, i.e. computations,
    // conditions, etc. We only want to account for computations, not for simple projections.
    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
    // in normalization stage. So we should ignore CASTs here in optimization stage.
    val compCnt = calcProgram.getProjectList.map(calcProgram.expandLocalRef).toList.count {
      case _: RexInputRef => false
      case _: RexLiteral => false
      case c: RexCall if c.getOperator.getName.equals("CAST") => false
      case _ => true
    }
    val newRowCnt = mq.getRowCount(calc)
    // TODO use inputRowCnt to compute cpu cost
    planner.getCostFactory.makeCost(newRowCnt, newRowCnt * compCnt, 0)
  }

  private[flink] def conditionToString(
      calcProgram: RexProgram,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    val cond = calcProgram.getCondition
    val inFields = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList

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

    val proj = calcProgram.getProjectList.toList
    val inFields = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList
    val outFields = calcProgram.getOutputRowType.getFieldNames.toList

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

}
