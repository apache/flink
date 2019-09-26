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
import org.apache.flink.table.plan.util.{InputRefVisitor, RexDefaultVisitor}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Rule that splits [[FlinkLogicalCalc]] into multiple [[FlinkLogicalCalc]]s. After this rule
  * is applied, each [[FlinkLogicalCalc]] will only contain Python [[ScalarFunction]]s or Java
  * [[ScalarFunction]]s. This is to ensure that the Python [[ScalarFunction]]s which could be
  * executed in a batch are grouped into the same [[FlinkLogicalCalc]] node.
  */
class PythonScalarFunctionSplitRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc], any),
  "PythonScalarFunctionSplitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val program = calc.getProgram

    // This rule matches if one of the following cases is met:
    // 1. There are Python functions and Java functions mixed in the Calc
    // 2. There are Python functions in the condition of the Calc
    (program.getExprList.exists(containsFunctionOf(_, FunctionLanguage.PYTHON)) &&
      program.getExprList.exists(containsFunctionOf(_, FunctionLanguage.JVM))) ||
    Option(program.getCondition)
      .map(program.expandLocalRef)
      .exists(containsFunctionOf(_, FunctionLanguage.PYTHON))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val input = calc.getInput
    val rexBuilder = call.builder().getRexBuilder
    val program = calc.getProgram
    val extractedRexCalls = new mutable.ArrayBuffer[RexCall]()

    val convertPythonFunction =
      program.getProjectList
        .map(program.expandLocalRef)
        .exists(containsFunctionOf(_, FunctionLanguage.JVM, recursive = false)) ||
      Option(program.getCondition)
        .map(program.expandLocalRef)
        .exists(expr =>
          containsFunctionOf(expr, FunctionLanguage.JVM, recursive = false) ||
            containsFunctionOf(expr, FunctionLanguage.PYTHON))

    val extractedFunctionOffset = input.getRowType.getFieldCount
    val splitter = new ScalarFunctionSplitter(
      extractedFunctionOffset,
      extractedRexCalls,
      convertPythonFunction)

    val newProjects = program.getProjectList.map(program.expandLocalRef(_).accept(splitter))
    val newCondition = Option(program.getCondition).map(program.expandLocalRef(_).accept(splitter))
    val accessedFields = extractRefInputFields(newProjects, newCondition, extractedFunctionOffset)

    val bottomCalcProjects =
      accessedFields.map(RexInputRef.of(_, input.getRowType)) ++ extractedRexCalls
    val bottomCalcFieldNames = SqlValidatorUtil.uniquify(
      accessedFields.map(i => input.getRowType.getFieldNames.get(i)).toSeq ++
        extractedRexCalls.indices.map("f" + _),
      rexBuilder.getTypeFactory.getTypeSystem.isSchemaCaseSensitive)

    val bottomCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      input,
      RexProgram.create(
        input.getRowType,
        bottomCalcProjects.toList,
        null,
        bottomCalcFieldNames,
        rexBuilder))

    val inputRewriter = new ExtractedFunctionInputRewriter(extractedFunctionOffset, accessedFields)
    val topCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      bottomCalc,
      RexProgram.create(
        bottomCalc.getRowType,
        newProjects.map(_.accept(inputRewriter)),
        newCondition.map(_.accept(inputRewriter)).orNull,
        calc.getRowType,
        rexBuilder))

    call.transformTo(topCalc)
  }

  /**
    * Extracts the indices of the input fields referred by the specified projects and condition.
    */
  private def extractRefInputFields(
      projects: Seq[RexNode],
      condition: Option[RexNode],
      inputFieldsCount: Int): Array[Int] = {
    val visitor = new InputRefVisitor

    // extract referenced input fields from projections
    projects.foreach(exp => exp.accept(visitor))

    // extract referenced input fields from condition
    condition.foreach(_.accept(visitor))

    // fields of indexes greater than inputFieldsCount is the extracted functions and
    // should be filtered as they are not from the original input
    visitor.getFields.filter(_ < inputFieldsCount)
  }
}

private class ScalarFunctionSplitter(
    extractedFunctionOffset: Int,
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
        extractedFunctionOffset + extractedRexCalls.length, call.getType)
      extractedRexCalls.append(call)
      newNode
    } else {
      call.clone(call.getType, call.getOperands.asScala.map(_.accept(this)))
    }
  }
}

/**
  * Rewrite field accesses of a RexNode as not all the fields from the original input are forwarded:
  * 1) Fields of index greater than or equal to extractedFunctionOffset refer to the
  *    extracted function.
  * 2) Fields of index less than extractedFunctionOffset refer to the original input field.
  *
  * @param extractedFunctionOffset the original start offset of the extracted functions
  * @param accessedFields the accessed fields which will be forwarded
  */
private class ExtractedFunctionInputRewriter(
    extractedFunctionOffset: Int,
    accessedFields: Array[Int])
  extends RexDefaultVisitor[RexNode] {

  /** old input fields ref index -> new input fields ref index mappings */
  private val fieldMap: Map[Int, Int] = accessedFields.zipWithIndex.toMap

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    if (inputRef.getIndex >= extractedFunctionOffset) {
      new RexInputRef(
        inputRef.getIndex - extractedFunctionOffset + accessedFields.length,
        inputRef.getType)
    } else {
      new RexInputRef(
        fieldMap.getOrElse(inputRef.getIndex,
          throw new IllegalArgumentException("input field contains invalid index")),
        inputRef.getType)
    }
  }

  override def visitCall(call: RexCall): RexNode = {
    call.clone(call.getType, call.getOperands.asScala.map(_.accept(this)))
  }

  override def visitNode(rexNode: RexNode): RexNode = rexNode
}

object PythonScalarFunctionSplitRule {
  val INSTANCE: RelOptRule = new PythonScalarFunctionSplitRule
}
