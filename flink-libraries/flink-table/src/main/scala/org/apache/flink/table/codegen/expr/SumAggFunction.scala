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
import org.apache.flink.table.expressions._
import org.apache.flink.table.dataformat.Decimal

/**
  * built-in sum aggregate function
  */
abstract class SumAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val sum = UnresolvedAggBufferReference("sum", getResultType)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(sum)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum = */ Null(sum.resultType)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    /* sum = */ IsNull(operands(0)) ? (sum, IsNull(sum) ? (operands(0), sum + operands(0)))
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    If(IsNull(sum.right), sum.left,
      If(IsNull(sum.left), sum.right, sum.left + sum.right))
  )

  override def getValueExpression: Expression = sum
}

/**
  * Built-in Int Sum aggregate function
  */
class IntSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.INT
}

/**
  * Built-in Byte Sum aggregate function
  */
class ByteSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.BYTE
}

/**
  * Built-in Short Sum aggregate function
  */
class ShortSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.SHORT
}

/**
  * Built-in Long Sum aggregate function
  */
class LongSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.LONG
}

/**
  * Built-in Float Sum aggregate function
  */
class FloatSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.FLOAT
}

/**
  * Built-in Double Sum aggregate function
  */
class DoubleSumAggFunction extends SumAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

/**
  * Built-in Decimal Sum aggregate function
  */
class DecimalSumAggFunction(argType: DecimalType)
  extends SumAggFunction {
  override def getResultType: InternalType =
    Decimal.inferAggSumType(argType.precision, argType.scale)
}
