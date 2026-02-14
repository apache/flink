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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.common.CommonCalc
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode, RelWriter}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.metadata.RelMdCollation
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.calcite.sql.SqlKind

import java.util
import java.util.function.Supplier

import scala.collection.JavaConversions._

/**
 * Sub-class of [[Calc]] that is a relational expression which computes project expressions and also
 * filters in Flink.
 */
class FlinkLogicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    calcProgram: RexProgram)
  extends CommonCalc(cluster, traitSet, input, calcProgram)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new FlinkLogicalCalc(cluster, traitSet, child, program)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = calcProgram.getInputRowType.getFieldNames.toList
    val localExprs = calcProgram.getExprList.toList

    // Format the where condition with field names if present
    val formattedWhere = if (calcProgram.getCondition != null) {
      val condition = calcProgram.expandLocalRef(calcProgram.getCondition)
      formatFilterCondition(condition, inputFieldNames)
    } else {
      null
    }

    pw.input("input", getInput)
      .item(
        "select",
        projectionToString(
          org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat(pw),
          pw.getDetailLevel))
      .itemIf("where", formattedWhere, formattedWhere != null)
  }

  /**
   * Formats a filter condition into a readable string with field names. Converts expressions like
   * ">($2, 100)" into "amount > 100"
   */
  private def formatFilterCondition(condition: RexNode, fieldNames: List[String]): String = {

    condition match {
      case call: RexCall if call.getKind == SqlKind.GREATER_THAN =>
        // Handle greater than: field > value
        formatBinaryCondition(call, fieldNames, " > ")

      case call: RexCall if call.getKind == SqlKind.LESS_THAN =>
        // Handle less than: field < value
        formatBinaryCondition(call, fieldNames, " < ")

      case call: RexCall if call.getKind == SqlKind.GREATER_THAN_OR_EQUAL =>
        // Handle greater than or equal: field >= value
        formatBinaryCondition(call, fieldNames, " >= ")

      case call: RexCall if call.getKind == SqlKind.LESS_THAN_OR_EQUAL =>
        // Handle less than or equal: field <= value
        formatBinaryCondition(call, fieldNames, " <= ")

      case call: RexCall if call.getKind == SqlKind.EQUALS =>
        // Handle equals: field = value
        formatBinaryCondition(call, fieldNames, " = ")

      case call: RexCall if call.getKind == SqlKind.NOT_EQUALS =>
        // Handle not equals: field <> value
        formatBinaryCondition(call, fieldNames, " <> ")

      case call: RexCall if call.getKind == SqlKind.AND =>
        // Handle composite conditions: cond1 AND cond2
        val operands = call.getOperands
        val formattedOperands = operands.map {
          operand => formatFilterCondition(operand, fieldNames)
        }
        formattedOperands.mkString(" AND ")

      case call: RexCall if call.getKind == SqlKind.OR =>
        // Handle OR conditions
        val operands = call.getOperands
        val formattedOperands = operands.map {
          operand => formatFilterCondition(operand, fieldNames)
        }
        formattedOperands.mkString(" OR ")

      case call: RexCall if call.getKind == SqlKind.NOT =>
        // Handle NOT condition
        val operand = call.getOperands.get(0)
        s"NOT ${formatFilterCondition(operand, fieldNames)}"

      case _ =>
        // Fallback to default formatting for complex expressions
        FlinkRexUtil.getExpressionString(condition, fieldNames)
    }
  }

  /** Formats a binary condition (e.g., field > value) */
  private def formatBinaryCondition(
      call: RexCall,
      fieldNames: List[String],
      operator: String): String = {
    val operands = call.getOperands
    if (operands.size == 2) {
      val left = formatOperand(operands.get(0), fieldNames)
      val right = formatOperand(operands.get(1), fieldNames)
      s"$left$operator$right"
    } else {
      FlinkRexUtil.getExpressionString(call, fieldNames)
    }
  }

  /** Formats a single operand (field reference or literal) */
  private def formatOperand(operand: RexNode, fieldNames: List[String]): String = {

    operand match {
      case inputRef: RexInputRef =>
        val index = inputRef.getIndex
        fieldNames(index)
      case _ =>
        // For literals and complex expressions, use default formatting
        FlinkRexUtil.getExpressionString(operand, fieldNames)
    }
  }

}

private class FlinkLogicalCalcConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[LogicalCalc]
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalCalc.create(newInput, calc.getProgram)
  }
}

object FlinkLogicalCalc {
  val CONVERTER: ConverterRule = new FlinkLogicalCalcConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalCalc],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalCalcConverter"))

  def create(input: RelNode, calcProgram: RexProgram): FlinkLogicalCalc = {
    val cluster = input.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster
      .traitSetOf(FlinkConventions.LOGICAL)
      .replaceIfs(
        RelCollationTraitDef.INSTANCE,
        new Supplier[util.List[RelCollation]]() {
          def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, calcProgram)
        })
      .simplify()
    new FlinkLogicalCalc(cluster, traitSet, input, calcProgram)
  }
}
