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
  * built-in sum0 aggregate function
  */
abstract class Sum0AggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val sum0 = UnresolvedAggBufferReference("sum0", getResultType)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(sum0)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum0 = */ Literal(0L, getResultType)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    /* sum0 = */ IsNull(operands(0)) ? (sum0, sum0 + operands(0))
  )

  override def retractExpressions: Seq[Expression] = Seq(
    /* sum0 = */ IsNull(operands(0)) ? (sum0, sum0 - operands(0))
  )

  override def mergeExpressions: Seq[Expression] = Seq(sum0.right + sum0.left)

  override def getValueExpression: Expression = sum0
}

/**
  * Built-in Int Sum0 aggregate function
  */
class IntSum0AggFunction extends Sum0AggFunction {
  override def getResultType: InternalType = DataTypes.INT
}

/**
  * Built-in Byte Sum0 aggregate function
  */
class ByteSum0AggFunction extends Sum0AggFunction {
  override def getResultType: InternalType = DataTypes.BYTE
}

/**
  * Built-in Short Sum0 aggregate function
  */
class ShortSum0AggFunction extends Sum0AggFunction {
  override def getResultType: InternalType = DataTypes.SHORT
}

/**
  * Built-in Long Sum0 aggregate function
  */
class LongSum0AggFunction extends Sum0AggFunction {
  override def getResultType: InternalType = DataTypes.LONG
}

/**
  * Built-in Float Sum0 aggregate function
  */
class FloatSum0AggFunction extends Sum0AggFunction {
  override def getResultType(): InternalType = DataTypes.FLOAT

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum0 = */ Literal(0.0f, getResultType)
  )
}

/**
  * Built-in Double Sum0 aggregate function
  */
class DoubleSum0AggFunction extends Sum0AggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum0 = */ Literal(0.0d, getResultType)
  )
}

/**
  * Built-in Decimal Sum0 aggregate function
  */
class DecimalSum0AggFunction(argType: DecimalType)
  extends Sum0AggFunction {
  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* sum0 = */ Literal(new JBigDecimal(0), getResultType)
  )

  override def getResultType: InternalType =
    Decimal.inferAggSumType(argType.precision, argType.scale)
}
