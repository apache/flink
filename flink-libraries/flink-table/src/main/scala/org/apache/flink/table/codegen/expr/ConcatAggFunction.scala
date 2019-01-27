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

package org.apache.flink.table.codegen.expr

import org.apache.flink.table.api.functions.DeclarativeAggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions._

class ConcatAggFunction(inputs: Int) extends DeclarativeAggregateFunction {

  protected lazy val acc =
    UnresolvedAggBufferReference("concatAcc", DataTypes.STRING)

  protected lazy val accDelimiter =
    UnresolvedAggBufferReference("accDelimiter", DataTypes.STRING)

  private lazy val delimiter =
    if(inputCount == 1) Literal("\n", DataTypes.STRING) else operands(0)

  private lazy val operand =
    if(inputCount == 1) operands(0) else operands(1)

  /**
    * How many inputs your function will deal with.
    */
  override def inputCount: Int = inputs

  /**
    * All fields of the aggregate buffer.
    */
  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(accDelimiter, acc)

  /**
    * The result type of the function
    */
  override def getResultType: InternalType = DataTypes.STRING

  /**
    * Expressions for initializing empty aggregation buffers.
    */
  override def initialValuesExpressions: Seq[Expression] = Seq(
    Literal("\n", DataTypes.STRING),
    Null(DataTypes.STRING)
  )

  /**
    * Expressions for accumulating the mutable aggregation buffer based on an input row.
    */
  override def accumulateExpressions: Seq[Expression] = Seq(
    delimiter,
    If(IsNull(operand),
      acc,
      If(IsNull(acc), operand, concat(concat(acc, delimiter), operand))
    )
  )

  /**
    * A sequence of expressions for merging two aggregation buffers together. When defining these
    * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
    * to the attributes corresponding to each of the buffers being merged (this magic is enabled
    * by the [[RichAggregateBufferAttribute]] implicit class).
    */
  override def mergeExpressions: Seq[Expression] = Seq(
    accDelimiter.right,
    If(IsNull(acc.right),
      acc.left,
      If(IsNull(acc.left), acc.right, concat(concat(acc.left, accDelimiter.right), acc.right))
    )
  )

  /**
    * An expression which returns the final value for this aggregate function.
    */
  override def getValueExpression: Expression = acc
}
