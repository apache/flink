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

package org.apache.flink.api.table.plan.rules.util

import org.apache.calcite.rex.{RexCall, RexInputRef, RexLocalRef, RexNode, RexShuttle, RexSlot, RexVisitorImpl}
import org.apache.flink.api.table.plan.nodes.dataset.DataSetCalc

import scala.collection.JavaConversions._
import scala.collection.mutable

object DataSetCalcConverter {

  /**
    * extract used input fields index of DataSetCalc RelNode
    *
    * @param calc the DataSetCalc which to analyze
    * @return used input fields indices
    */
  def extractRefInputFields(calc: DataSetCalc): Array[Int] = {
    val visitor = new RefFieldsVisitor
    val calcProgram = calc.calcProgram
    // extract input fields from project expressions
    calcProgram.getProjectList.foreach(exp => calcProgram.expandLocalRef(exp).accept(visitor))
    val condition = calcProgram.getCondition
    // extract input fields from condition expression
    if (condition != null) {
      calcProgram.expandLocalRef(condition).accept(visitor)
    }
    visitor.getFields
  }

  /**
    * rewrite DataSetCal project expressions and condition expression based on new input fields
    *
    * @param calc            the DataSetCalc which to rewrite
    * @param usedInputFields input fields index of DataSetCalc RelNode
    * @return a tuple which contain 2 elements, the first one is rewritten project expressions;
    *         the second one is rewritten condition expression,
    *         Note: if origin condition expression is null, the second value is None
    */
  def rewriteCalcExprs(
      calc: DataSetCalc,
      usedInputFields: Array[Int]): (List[RexNode], Option[RexNode]) = {
    val inputRewriter = new InputRewriter(usedInputFields)
    val calcProgram = calc.calcProgram
    val newProjectExpressions = calcProgram.getProjectList.map(
      exp => calcProgram.expandLocalRef(exp).accept(inputRewriter)
    ).toList

    val oldCondition = calcProgram.getCondition
    val newConditionExpression = {
      oldCondition match {
        case ref: RexLocalRef => Some(calcProgram.expandLocalRef(ref).accept(inputRewriter))
        case _ => None         // null does not match any type
      }
    }
    (newProjectExpressions, newConditionExpression)
  }
}

/**
  * A RexVisitor to extract used input fields
  */
class RefFieldsVisitor extends RexVisitorImpl[Unit](true) {
  private var fields = mutable.LinkedHashSet[Int]()

  def getFields: Array[Int] = fields.toArray

  override def visitInputRef(inputRef: RexInputRef): Unit = fields += inputRef.getIndex

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}

/**
  * This class is responsible for rewrite input
  *
  * @param fields fields mapping
  */
class InputRewriter(fields: Array[Int]) extends RexShuttle {

  /** old input fields ref index -> new input fields ref index mappings */
  private val fieldMap: Map[Int, Int] =
    fields.zipWithIndex.toMap

  override def visitInputRef(inputRef: RexInputRef): RexNode =
    new RexInputRef(relNodeIndex(inputRef), inputRef.getType)

  override def visitLocalRef(localRef: RexLocalRef): RexNode =
    new RexInputRef(relNodeIndex(localRef), localRef.getType)

  private def relNodeIndex(ref: RexSlot): Int =
    fieldMap.getOrElse(ref.getIndex,
      throw new IllegalArgumentException("input field contains invalid index"))
}
