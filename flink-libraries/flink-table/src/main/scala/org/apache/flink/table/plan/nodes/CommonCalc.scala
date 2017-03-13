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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedFunction}
import org.apache.flink.table.runtime.FlatMapRunner
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait CommonCalc {

  private[flink] def functionBody(
      generator: CodeGenerator,
      inputType: TypeInformation[Row],
      rowType: RelDataType,
      calcProgram: RexProgram,
      config: TableConfig)
    : String = {

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(rowType)

    val condition = calcProgram.getCondition
    val expandedExpressions = calcProgram.getProjectList.map(
      expr => calcProgram.expandLocalRef(expr))
    val projection = generator.generateResultExpression(
      returnType,
      rowType.getFieldNames,
      expandedExpressions)

    // only projection
    if (condition == null) {
      s"""
        |${projection.code}
        |${generator.collectorTerm}.collect(${projection.resultTerm});
        |""".stripMargin
    }
    else {
      val filterCondition = generator.generateExpression(
        calcProgram.expandLocalRef(calcProgram.getCondition))
      // only filter
      if (projection == null) {
        s"""
          |${filterCondition.code}
          |if (${filterCondition.resultTerm}) {
          |  ${generator.collectorTerm}.collect(${generator.input1Term});
          |}
          |""".stripMargin
      }
      // both filter and projection
      else {
        s"""
          |${filterCondition.code}
          |if (${filterCondition.resultTerm}) {
          |  ${projection.code}
          |  ${generator.collectorTerm}.collect(${projection.resultTerm});
          |}
          |""".stripMargin
      }
    }
  }

  private[flink] def calcMapFunction(
      genFunction: GeneratedFunction[FlatMapFunction[Row, Row], Row])
    : RichFlatMapFunction[Row, Row] = {

    new FlatMapRunner[Row, Row](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)
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
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {

    val proj = calcProgram.getProjectList.asScala.toList
    val inFields = calcProgram.getInputRowType.getFieldNames.asScala.toList
    val localExprs = calcProgram.getExprList.asScala.toList
    val outFields = calcProgram.getOutputRowType.getFieldNames.asScala.toList

    proj
      .map(expression(_, inFields, Some(localExprs)))
      .zip(outFields).map { case (e, o) =>
        if (e != o) {
          e + " AS " + o
        } else {
          e
        }
    }.mkString(", ")
  }

  private[flink] def calcOpName(
      calcProgram: RexProgram,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String) = {

    val conditionStr = conditionToString(calcProgram, expression)
    val selectionStr = selectionToString(calcProgram, expression)

    s"${if (calcProgram.getCondition != null) {
      s"where: ($conditionStr), "
    } else {
      ""
    }}select: ($selectionStr)"
  }

  private[flink] def calcToString(
      calcProgram: RexProgram,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String) = {

    val name = calcOpName(calcProgram, expression)
    s"Calc($name)"
  }

  private[flink] def computeSelfCost(
      calcProgram: RexProgram,
      planner: RelOptPlanner,
      rowCnt: Double): RelOptCost = {

    // compute number of expressions that do not access a field or literal, i.e. computations,
    // conditions, etc. We only want to account for computations, not for simple projections.
    // CASTs in RexProgram are reduced as far as possible by ReduceExpressionsRule
    // in normalization stage. So we should ignore CASTs here in optimization stage.
    val compCnt = calcProgram.getExprList.asScala.toList.count {
      case i: RexInputRef => false
      case l: RexLiteral => false
      case c: RexCall if c.getOperator.getName.equals("CAST") => false
      case _ => true
    }

    val newRowCnt = estimateRowCount(calcProgram, rowCnt)
    planner.getCostFactory.makeCost(newRowCnt, newRowCnt * compCnt, 0)
  }

  private[flink] def estimateRowCount(
      calcProgram: RexProgram,
      rowCnt: Double): Double = {

    if (calcProgram.getCondition != null) {
      // we reduce the result card to push filters down
      (rowCnt * 0.75).min(1.0)
    } else {
      rowCnt
    }
  }
}
