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

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.plan.util.PythonUtil.containsFunctionOf
import org.apache.flink.table.plan.util.RexDefaultVisitor

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Rule that split [[FlinkLogicalCalc]] into multiple [[FlinkLogicalCalc]]s. This is to ensure
  * that the Python [[ScalarFunction]]s which could be executed in a batch are grouped into
  * the same [[FlinkLogicalCalc]] node.
  */
class PythonScalarFunctionSplitRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc], any),
  "PythonScalarFunctionSplitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram
    program.getExprList.asScala.exists(containsFunctionOf(_, FunctionLanguage.PYTHON)) &&
    program.getExprList.asScala.exists(containsFunctionOf(_, FunctionLanguage.JVM))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val input = calc.getInput
    val rexBuilder = call.builder().getRexBuilder
    val program = calc.getProgram
    val extractedRexCalls = new mutable.ArrayBuffer[RexCall]()

    val outerCallContainsJavaFuntion =
      program.getProjectList
        .map(program.expandLocalRef)
        .exists(containsFunctionOf(_, FunctionLanguage.JVM, recursive = false)) ||
      Option(program.getCondition)
        .map(program.expandLocalRef)
        .exists(containsFunctionOf(_, FunctionLanguage.JVM, recursive = false))

    val splitter = new ScalarFunctionSplitter(
      input.getRowType.getFieldCount,
      extractedRexCalls,
      outerCallContainsJavaFuntion)

    val newProjects = program.getProjectList
      .map(program.expandLocalRef)
      .map(_.accept(splitter))

    val newCondition = Option(program.getCondition)
      .map(program.expandLocalRef)
      .map(_.accept(splitter))

    val bottomCalcProjects =
      input.getRowType.getFieldList.indices.map(RexInputRef.of(_, input.getRowType)) ++
      extractedRexCalls
    val bottomCalcFieldNames = SqlValidatorUtil.uniquify(
      input.getRowType.getFieldNames ++
        extractedRexCalls.indices.map("f" + _),
      rexBuilder.getTypeFactory.getTypeSystem.isSchemaCaseSensitive)

    val bottomCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      input,
      RexProgram.create(
        input.getRowType,
        bottomCalcProjects,
        null,
        bottomCalcFieldNames,
        rexBuilder))

    val topCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      bottomCalc,
      RexProgram.create(
        bottomCalc.getRowType,
        newProjects,
        newCondition.orNull,
        calc.getRowType,
        rexBuilder))

    call.transformTo(topCalc)
  }
}

private class ScalarFunctionSplitter(
    pythonFunctionOffset: Int,
    extractedRexCalls: mutable.ArrayBuffer[RexCall],
    convertPythonFunction: Boolean)
  extends RexDefaultVisitor[RexNode] {

  override def visitCall(call: RexCall): RexNode = {
    call.getOperator match {
      case sfc: ScalarSqlFunction if sfc.getScalarFunction.getLanguage ==
        FunctionLanguage.PYTHON =>
        visit(convertPythonFunction, call)

      case _ =>
        visit(!convertPythonFunction, call)
    }
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode

  private def visit(needConvert: Boolean, call: RexCall): RexNode = {
    if (needConvert) {
      val newNode = new RexInputRef(
        pythonFunctionOffset + extractedRexCalls.length, call.getType)
      extractedRexCalls.append(call)
      newNode
    } else {
      call.clone(
        call.getType,
        call.getOperands.asScala.map(_.accept(
          new ScalarFunctionSplitter(
            pythonFunctionOffset,
            extractedRexCalls,
            convertPythonFunction))))
    }
  }
}

object PythonScalarFunctionSplitRule {
  val INSTANCE: RelOptRule = new PythonScalarFunctionSplitRule
}
