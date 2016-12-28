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

package org.apache.flink.table.plan.rules.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.JavaConverters._

object RexProgramProjectExtractor {

  /**
    * Extracts the indexes of input fields accessed by the RexProgram.
    *
    * @param rexProgram The RexProgram to analyze
    * @return The indexes of accessed input fields
    */
  def extractRefInputFields(rexProgram: RexProgram): Array[Int] = {
    val visitor = new RefFieldsVisitor
    // extract input fields from project expressions
    rexProgram.getProjectList.foreach(exp => rexProgram.expandLocalRef(exp).accept(visitor))
    val condition = rexProgram.getCondition
    // extract input fields from condition expression
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }
    visitor.getFields
  }

  /**
    * Generates a new RexProgram based on mapped input fields.
    *
    * @param rexProgram      original RexProgram
    * @param inputRowType    input row type
    * @param usedInputFields indexes of used input fields
    * @param rexBuilder      builder for Rex expressions
    *
    * @return A RexProgram with mapped input field expressions.
    */
  def rewriteRexProgram(
      rexProgram: RexProgram,
      inputRowType: RelDataType,
      usedInputFields: Array[Int],
      rexBuilder: RexBuilder): RexProgram = {

    val inputRewriter = new InputRewriter(usedInputFields)
    val newProjectExpressions = rexProgram.getProjectList.map(
      exp => rexProgram.expandLocalRef(exp).accept(inputRewriter)
    ).toList.asJava

    val oldCondition = rexProgram.getCondition
    val newConditionExpression = {
      oldCondition match {
        case ref: RexLocalRef => rexProgram.expandLocalRef(ref).accept(inputRewriter)
        case _ => null // null does not match any type
      }
    }
    RexProgram.create(
      inputRowType,
      newProjectExpressions,
      newConditionExpression,
      rexProgram.getOutputRowType,
      rexBuilder
    )
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
  * A RexShuttle to rewrite field accesses of a RexProgram.
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
