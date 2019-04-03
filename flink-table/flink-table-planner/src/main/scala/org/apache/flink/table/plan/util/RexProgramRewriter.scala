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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object RexProgramRewriter {

  /**
    * Generates a new RexProgram with used input fields. The used fields maybe
    * a subset of total input fields, so we need to convert the field index in
    * new RexProgram based on given fields.
    *
    * @param rexProgram   original RexProgram
    * @param inputRowType input row type
    * @param rexBuilder   builder for Rex expressions
    * @param usedFields   indices of used input fields
    * @return A new RexProgram with only used input fields
    */
  def rewriteWithFieldProjection(
      rexProgram: RexProgram,
      inputRowType: RelDataType,
      rexBuilder: RexBuilder,
      usedFields: Array[Int]): RexProgram = {

    val inputRewriter = new InputRewriter(usedFields)

    // rewrite input field in projections
    val newProjectExpressions = rexProgram.getProjectList.map(
      exp => rexProgram.expandLocalRef(exp).accept(inputRewriter)
    ).toList.asJava

    // rewrite input field in condition
    val newConditionExpression = {
      rexProgram.getCondition match {
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
  * A RexShuttle to rewrite field accesses of a RexProgram.
  *
  * @param fields used input fields
  */
class InputRewriter(fields: Array[Int]) extends RexShuttle {

  /** old input fields ref index -> new input fields ref index mappings */
  private val fieldMap: Map[Int, Int] =
    fields.zipWithIndex.toMap

  override def visitInputRef(inputRef: RexInputRef): RexNode =
    new RexInputRef(refNewIndex(inputRef), inputRef.getType)

  override def visitLocalRef(localRef: RexLocalRef): RexNode =
    new RexInputRef(refNewIndex(localRef), localRef.getType)

  private def refNewIndex(ref: RexSlot): Int =
    fieldMap.getOrElse(ref.getIndex,
      throw new IllegalArgumentException("input field contains invalid index"))
}
