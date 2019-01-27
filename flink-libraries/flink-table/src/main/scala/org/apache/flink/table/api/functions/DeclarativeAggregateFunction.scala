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

package org.apache.flink.table.api.functions

import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.expressions.{Expression, UnresolvedAggBufferReference, UnresolvedFieldReference}

/**
 * API for aggregation functions that are expressed in terms of expressions.
 *
 * When implementing a new expression-based aggregate function, you should first decide how many
 * operands your function will have by implementing `inputCount` method. And then you can use
 * `operands` fields to represent your operand, like `operands(0)`, `operands(2)`.
 *
 * Then you should declare all your buffer attributes by implementing `aggBufferAttributes`. You
 * should declare all buffer attributes as `UnresolvedAggBufferReference`, and make sure the name
 * of your attributes are unique within the function. You can then use these attributes when
 * defining `initialValuesExpressions`, `accumulateExpressions`, `mergeExpressions` and
 * `getValueExpression`.
 *
 * See an full example: [[org.apache.flink.table.codegen.expr.AvgAggFunction]]
 */
trait DeclarativeAggregateFunction extends UserDefinedFunction {

  /**
   * How many inputs your function will deal with.
   */
  def inputCount: Int

  /**
   * All fields of the aggregate buffer.
   */
  def aggBufferAttributes: Seq[UnresolvedAggBufferReference]

  /**
   * The result type of the function
   */
  def getResultType: InternalType

  /**
   * Expressions for initializing empty aggregation buffers.
   */
  def initialValuesExpressions: Seq[Expression]

  /**
   * Expressions for accumulating the mutable aggregation buffer based on an input row.
   */
  def accumulateExpressions: Seq[Expression]

  /**
    * Expressions for retracting the mutable aggregation buffer based on an input row.
    */
  def retractExpressions: Seq[Expression] = ???

  /**
   * A sequence of expressions for merging two aggregation buffers together. When defining these
   * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
   * to the attributes corresponding to each of the buffers being merged (this magic is enabled
   * by the [[RichAggregateBufferAttribute]] implicit class).
   */
  def mergeExpressions: Seq[Expression]

  /**
   * An expression which returns the final value for this aggregate function.
   */
  def getValueExpression: Expression

  /**
   * A helper class for representing an attribute used in merging two
   * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
   * we merge buffer values and then update bufferLeft.
   */
  implicit class RichAggregateBufferAttribute(a: UnresolvedAggBufferReference) {
    /** Represents this attribute at the aggregate buffer side. */
    def left: UnresolvedAggBufferReference = a

    /** Represents this attribute at the input side. */
    def right: UnresolvedFieldReference = inputAggBufferAttributes(aggBufferAttributes.indexOf(a))
  }

  final val operands: Array[UnresolvedFieldReference] = {
    require(inputCount >= 0, "inputCount must be greater than or equal to 0.")
    (0 until inputCount).map { index => UnresolvedFieldReference(index.toString) }.toArray
  }

  final val aggBufferSchema: Seq[InternalType] = aggBufferAttributes.map(_.resultType)

  final val inputAggBufferAttributes: Seq[UnresolvedFieldReference] = {
    aggBufferAttributes.indices.map { index => UnresolvedFieldReference(index.toString) }
  }

}
