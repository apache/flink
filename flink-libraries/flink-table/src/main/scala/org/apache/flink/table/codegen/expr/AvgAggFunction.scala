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

import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.table.api.functions.DeclarativeAggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.expressions._
import org.apache.flink.table.dataformat.Decimal

/**
  * built-in avg aggregate function
  */
abstract class AvgAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  protected lazy val sum = UnresolvedAggBufferReference("sum", DataTypes.LONG)
  protected lazy val count = UnresolvedAggBufferReference("count", DataTypes.LONG)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(sum, count)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum = */ Literal(0L, sum.resultType),
    /* count = */ Literal(0L)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    /* sum = */ IsNull(operands(0)) ? (sum, sum + operands(0)),
    /* count = */ IsNull(operands(0)) ? (count, count + 1)
  )

  override def retractExpressions: Seq[Expression] = Seq(
    /* sum = */ IsNull(operands(0)) ? (sum, sum - operands(0)),
    /* count = */ IsNull(operands(0)) ? (count, count - 1)
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    /* sum = */ sum.left + sum.right,
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get null after the division.

  override def getValueExpression: Expression =
    If(count === Literal(0L),
      Null(getResultType),
      sum / count)
}

/**
 * Built-in Int Avg aggregate function for integral arguments,
 * including BYTE, SHORT, INT, LONG.
 * The result type is DOUBLE.
 */
class IntegralAvgAggFunction extends AvgAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

/**
  * Built-in Double Avg aggregate function
  */
class DoubleAvgAggFunction extends AvgAggFunction {
  override lazy val sum = UnresolvedAggBufferReference("sum", DataTypes.DOUBLE)
  override def getResultType: InternalType = DataTypes.DOUBLE

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum = */ Literal(0.0d, sum.resultType),
    /* count = */ Literal(0L)
  )
}

/**
  * Built-in Decimal Avg aggregate function
  */
class DecimalAvgAggFunction(argType: DecimalType)
  extends AvgAggFunction {

  override lazy val sum = UnresolvedAggBufferReference("sum", getSumType)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum = */ Literal(new JBigDecimal(0), sum.resultType),
    /* count = */ Literal(0L)
  )

  def getSumType: InternalType =
    Decimal.inferAggSumType(argType.precision, argType.scale)

  override def getResultType: InternalType =
    Decimal.inferAggAvgType(argType.precision, argType.scale)
}
