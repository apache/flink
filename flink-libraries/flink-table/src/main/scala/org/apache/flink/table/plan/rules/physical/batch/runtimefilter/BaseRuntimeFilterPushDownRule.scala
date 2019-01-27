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

package org.apache.flink.table.plan.rules.physical.batch.runtimefilter

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexNode, _}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that pushes a [[SqlRuntimeFilterFunction]] or [[SqlRuntimeFilterBuilderFunction]]
  * past a [[RelNode]].
  */
abstract class BaseRuntimeFilterPushDownRule[T <: RelNode](
    inputClass: Class[T],
    description: String) extends RelOptRule(
  operand(classOf[BatchExecCalc], operand(inputClass, any)), description) {

  /**
    * Whether it can push down RfFunction, according to the different relNodes
    * have different strategies.
    */
  def canPush(rel: T, rCols: ImmutableBitSet, cond: RexNode): Boolean

  /**
    * Get field adjustments before and after pushing down.
    */
  def getFieldAdjustments(rel: T): Array[Int]

  /**
    * Update row count and ndv of RfFunction after push down.
    */
  def updateRfFunction(filterInput: RelNode, program: RexProgram): Unit

  /**
    * Get input of current input, default for single relNode.
    */
  def getInputOfInput(input: T): RelNode

  /**
    * Replace current input with filter, default for single relNode.
    */
  def replaceInput(input: T, filter: BatchExecCalc): RelNode

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = call.rel(0)
    val inputRel: T = call.rel(1)
    val conditions = RelOptUtil.conjunctions(
      calc.getProgram.expandLocalRef(calc.getProgram.getCondition))
    val builder = call.builder()
    val rexBuilder = calc.getCluster.getRexBuilder
    val origFields = inputRel.getRowType.getFieldList
    val adjustments = getFieldAdjustments(inputRel)
    val pushedConditions = new mutable.ArrayBuffer[RexNode]()
    val remainingConditions = new mutable.ArrayBuffer[RexNode]()

    for (condition <- conditions) {
      val rCols = RelOptUtil.InputFinder.bits(condition)
      if (canPush(inputRel, rCols, condition)) {
        pushedConditions.add(condition.accept(
          new RelOptUtil.RexInputConverter(
            rexBuilder,
            origFields,
            getInputOfInput(inputRel).getRowType.getFieldList,
            adjustments)))
      } else {
        remainingConditions.add(condition)
      }
    }

    if (pushedConditions.nonEmpty) {
      val filterInput = getInputOfInput(inputRel)
      val rexProgramBuilder = new RexProgramBuilder(filterInput.getRowType, rexBuilder)
      rexProgramBuilder.addCondition(builder.and(pushedConditions))
      InsertRuntimeFilterRule.projectAllFields(filterInput, rexProgramBuilder)
      val newProgram = rexProgramBuilder.getProgram

      updateRfFunction(filterInput, newProgram)

      val pushFilter = new BatchExecCalc(
        filterInput.getCluster,
        filterInput.getTraitSet,
        filterInput,
        filterInput.getRowType,
        newProgram,
        "BatchExecCalc")

      val rel = replaceInput(inputRel, pushFilter)
      val pBuilder = RexProgramBuilder.forProgram(calc.getProgram, rexBuilder, true)
      pBuilder.clearCondition()
      if (remainingConditions.nonEmpty) {
        pBuilder.addCondition(builder.and(remainingConditions))
      }
      call.transformTo(calc.copy(calc.getTraitSet, rel, pBuilder.getProgram))
    }
  }
}

object BaseRuntimeFilterPushDownRule {

  /**
    * Find all [[SqlRuntimeFilterFunction]].
    */
  def findRuntimeFilters(program: RexProgram): Seq[RexCall] = {
    if (program.getCondition == null) {
      return Seq()
    }
    val conditions = RelOptUtil.conjunctions(program.expandLocalRef(program.getCondition))
    conditions.filter {
      case call: RexCall => call.getOperator.isInstanceOf[SqlRuntimeFilterFunction]
      case _ => false
    }.asInstanceOf[Seq[RexCall]]
  }

  /**
    * Find all [[SqlRuntimeFilterBuilderFunction]].
    */
  def findRfBuilders(program: RexProgram): Seq[RexCall] = {
    if (program.getCondition == null) {
      return Seq()
    }
    val conditions = RelOptUtil.conjunctions(program.expandLocalRef(program.getCondition))
    conditions.filter {
      case call: RexCall => call.getOperator.isInstanceOf[SqlRuntimeFilterBuilderFunction]
      case _ => false
    }.asInstanceOf[Seq[RexCall]]
  }
}
