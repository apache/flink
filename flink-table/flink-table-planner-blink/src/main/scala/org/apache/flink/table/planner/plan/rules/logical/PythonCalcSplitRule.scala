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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexProgram}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.plan.utils.PythonUtil.{containsPythonCall, containsNonPythonCall, isPythonCall, isNonPythonCall}
import org.apache.flink.table.planner.plan.utils.{InputRefVisitor, RexDefaultVisitor}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Base rule that splits [[FlinkLogicalCalc]] into multiple [[FlinkLogicalCalc]]s.
  * It is mainly to ensure that each [[FlinkLogicalCalc]] only contains Java/Scala
  * [[ScalarFunction]]s or Python [[ScalarFunction]]s.
  */
abstract class PythonCalcSplitRuleBase(description: String)
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc], any),
    description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val input = calc.getInput
    val rexBuilder = call.builder().getRexBuilder
    val program = calc.getProgram
    val extractedRexCalls = new mutable.ArrayBuffer[RexCall]()

    val extractedFunctionOffset = input.getRowType.getFieldCount
    val splitter = new ScalarFunctionSplitter(
      extractedFunctionOffset,
      extractedRexCalls,
      isConvertPythonFunction(program))

    val (bottomCalcCondition, topCalcCondition, topCalcProjects) = split(program, splitter)
    val accessedFields = extractRefInputFields(
      topCalcProjects, topCalcCondition, extractedFunctionOffset)

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
        bottomCalcCondition.orNull,
        bottomCalcFieldNames,
        rexBuilder))

    val inputRewriter = new ExtractedFunctionInputRewriter(extractedFunctionOffset, accessedFields)
    val topCalc = new FlinkLogicalCalc(
      calc.getCluster,
      calc.getTraitSet,
      bottomCalc,
      RexProgram.create(
        bottomCalc.getRowType,
        topCalcProjects.map(_.accept(inputRewriter)),
        topCalcCondition.map(_.accept(inputRewriter)).orNull,
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

  /**
    * Returns true if converting Python functions.
    */
  def isConvertPythonFunction(program: RexProgram): Boolean

  /**
    * Splits the specified [[RexProgram]] using the specified [[ScalarFunctionSplitter]].
    * It returns a triple of (bottom calc condition, top calc condition, top calc projects)
    * as the split result.
    */
  def split(program: RexProgram, splitter: ScalarFunctionSplitter)
      : (Option[RexNode], Option[RexNode], Seq[RexNode])
}

/**
  * Rule that splits [[FlinkLogicalCalc]]s which contain Python functions in the condition
  * into multiple [[FlinkLogicalCalc]]s. After this rule is applied, there will be no
  * Python functions in the condition of the [[FlinkLogicalCalc]]s.
  */
object PythonCalcSplitConditionRule extends PythonCalcSplitRuleBase(
  "PythonCalcSplitConditionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]

    // matches if it contains Python functions in condition
    Option(calc.getProgram.getCondition)
      .map(calc.getProgram.expandLocalRef).exists(containsPythonCall)
  }

  override def isConvertPythonFunction(program: RexProgram): Boolean = true

  override def split(program: RexProgram, splitter: ScalarFunctionSplitter)
      : (Option[RexNode], Option[RexNode], Seq[RexNode]) = {
    (None, Option(program.getCondition).map(program.expandLocalRef(_).accept(splitter)),
      program.getProjectList.map(program.expandLocalRef))
  }
}

/**
  * Rule that splits [[FlinkLogicalCalc]]s which contain both Java functions and Python functions
  * in the projection into multiple [[FlinkLogicalCalc]]s. After this rule is applied, it will
  * only contain Python functions or Java functions in the projection of each [[FlinkLogicalCalc]].
  */
object PythonCalcSplitProjectionRule extends PythonCalcSplitRuleBase(
  "PythonCalcSplitProjectionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)

    // matches if it contains both Python functions and Java functions in the projection
    projects.exists(containsPythonCall) && projects.exists(containsNonPythonCall)
  }

  override def isConvertPythonFunction(program: RexProgram): Boolean = {
    program.getProjectList.map(program.expandLocalRef).exists(isNonPythonCall)
  }

  override def split(program: RexProgram, splitter: ScalarFunctionSplitter)
      : (Option[RexNode], Option[RexNode], Seq[RexNode]) = {
    (Option(program.getCondition).map(program.expandLocalRef), None,
      program.getProjectList.map(program.expandLocalRef(_).accept(splitter)))
  }
}

/**
  * Rule that pushes the condition of [[FlinkLogicalCalc]]s before it for the
  * [[FlinkLogicalCalc]]s which contain Python functions in the projection.
  */
object PythonCalcPushConditionRule extends PythonCalcSplitRuleBase(
  "PythonCalcPushConditionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)

    // matches if all the following conditions hold true:
    // 1) the condition is not null
    // 2) it contains Python functions in the projection
    calc.getProgram.getCondition != null && projects.exists(containsPythonCall)
  }

  override def isConvertPythonFunction(program: RexProgram): Boolean = false

  override def split(program: RexProgram, splitter: ScalarFunctionSplitter)
      : (Option[RexNode], Option[RexNode], Seq[RexNode]) = {
    (Option(program.getCondition).map(program.expandLocalRef), None,
      program.getProjectList.map(program.expandLocalRef))
  }
}

/**
  * Rule that ensures that it only contains [[RexInputRef]]s at the beginning of
  * the project list and [[RexCall]]s at the end of the project list for [[FlinkLogicalCalc]]s
  * which contain Python functions in the projection. This rule exists to keep
  * DataStreamPythonCalc as simple as possible and ensures that it only needs to
  * handle the Python function execution.
  */
object PythonCalcRewriteProjectionRule extends PythonCalcSplitRuleBase(
  "PythonCalcRewriteProjectionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)

    // matches if all the following conditions hold true:
    // 1) it contains Python functions in the projection
    // 2) it contains RexNodes besides RexInputRef and RexCall or
    //    not all the RexCalls lying at the end of the project list
    projects.exists(containsPythonCall) &&
      (projects.exists(expr => !expr.isInstanceOf[RexCall] && !expr.isInstanceOf[RexInputRef]) ||
        projects.indexWhere(_.isInstanceOf[RexCall]) <
          projects.lastIndexWhere(_.isInstanceOf[RexInputRef]))
  }

  override def isConvertPythonFunction(program: RexProgram): Boolean = true

  override def split(program: RexProgram, splitter: ScalarFunctionSplitter)
      : (Option[RexNode], Option[RexNode], Seq[RexNode]) = {
    (None, None, program.getProjectList.map(program.expandLocalRef(_).accept(splitter)))
  }
}

private class ScalarFunctionSplitter(
    extractedFunctionOffset: Int,
    extractedRexCalls: mutable.ArrayBuffer[RexCall],
    convertPythonFunction: Boolean)
  extends RexDefaultVisitor[RexNode] {

  override def visitCall(call: RexCall): RexNode = {
    visit(if (isPythonCall(call)) convertPythonFunction else !convertPythonFunction, call)
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

object PythonCalcSplitRule {
  /**
    * These rules should be applied sequentially in the order of
    * SPLIT_CONDITION, SPLIT_PROJECT, PUSH_CONDITION and REWRITE_PROJECT.
    */
  val SPLIT_CONDITION: RelOptRule = PythonCalcSplitConditionRule
  val SPLIT_PROJECT: RelOptRule = PythonCalcSplitProjectionRule
  val PUSH_CONDITION: RelOptRule = PythonCalcPushConditionRule
  val REWRITE_PROJECT: RelOptRule = PythonCalcRewriteProjectionRule
}
