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
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.expressions.{IsNull, Literal, _}
import org.apache.flink.table.dataformat.Decimal

/**
  * built-in sum with retract aggregate function
  */
abstract class SumWithRetractAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val sum = UnresolvedAggBufferReference("sum", getResultType)
  private lazy val count = UnresolvedAggBufferReference("count", DataTypes.LONG)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(sum, count)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum = */ Null(sum.resultType),
    /* count = */ Literal(0L)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    /* sum = */ IsNull(operands(0)) ? (sum, IsNull(sum) ? (operands(0), sum + operands(0))),
    /* count = */ IsNull(operands(0)) ? (count, count + 1)
  )

  override def retractExpressions: Seq[Expression] = Seq(
    /* sum = */ IsNull(operands(0)) ? (
      sum, IsNull(sum) ? (zeroLiteral - operands(0), sum - operands(0))),
    /* count = */ IsNull(operands(0)) ? (count, count - 1)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    /* sum = */ If(IsNull(sum.right), sum.left,
       If(IsNull(sum.left), sum.right, sum.left + sum.right)),
    /* count = */ count.left + count.right
  )

  override def getValueExpression: Expression =
    EqualTo(count, Literal(0L)) ? (Null(sum.resultType), sum)

  def zeroLiteral: Expression
}

/**
  * Built-in Int Sum with retract aggregate function
  */
class IntSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.INT

  override def zeroLiteral: Expression = 0
}

/**
  * Built-in Byte Sum with retract aggregate function
  */
class ByteSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.BYTE

  override def zeroLiteral: Expression = 0.toByte
}

/**
  * Built-in Short Sum with retract aggregate function
  */
class ShortSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.SHORT

  override def zeroLiteral: Expression = 0.toShort
}

/**
  * Built-in Long Sum with retract aggregate function
  */
class LongSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.LONG

  override def zeroLiteral: Expression = 0L
}

/**
  * Built-in Float Sum with retract aggregate function
  */
class FloatSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.FLOAT

  override def zeroLiteral: Expression = 0.0F
}

/**
  * Built-in Double Sum with retract aggregate function
  */
class DoubleSumWithRetractAggFunction extends SumWithRetractAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE

  override def zeroLiteral: Expression = 0.0
}

/**
  * Built-in Decimal Sum with retract aggregate function
  */
class DecimalSumWithRetractAggFunction(argType: DecimalType)
      extends SumWithRetractAggFunction {
  override def getResultType: InternalType =
    Decimal.inferAggSumType(argType.precision, argType.scale)

  override def zeroLiteral: Expression = 0
}
