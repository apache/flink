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
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.expressions.{If, _}

/**
  * Built-in max aggregate function.
  */
abstract class MaxAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val max = UnresolvedAggBufferReference("max", getResultType)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(max)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* max = */ Null(getResultType)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    If(IsNull(operands(0)), max,
      If(IsNull(max), operands(0),
        If(GreaterThan(operands(0), max), operands(0), max)))
  )

  override def retractExpressions: Seq[Expression] = Seq()

  override def mergeExpressions: Seq[Expression] = Seq(
    If(IsNull(max.right), max.left,
      If(IsNull(max.left), max.right,
        If(GreaterThan(max.right, max.left), max.right, max.left)))
  )

  override def getValueExpression: Expression = max
}

/**
  * Built-in Int Max aggregate function.
  */
class IntMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.INT
}

/**
  * Built-in Byte Max aggregate function.
  */
class ByteMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.BYTE
}

/**
  * Built-in Short Max aggregate function.
  */
class ShortMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.SHORT
}

/**
  * Built-in Long Max aggregate function.
  */
class LongMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.LONG
}

/**
  * Built-in Float Max aggregate function.
  */
class FloatMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.FLOAT
}

/**
  * Built-in Double Max aggregate function.
  */
class DoubleMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Max aggregate function.
  */
class BooleanMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.BOOLEAN
}

/**
  * Built-in Decimal Max aggregate function.
  */
class DecimalMaxAggFunction(decimalType: DecimalType)
  extends MaxAggFunction {
  override def getResultType: InternalType = decimalType
}

/**
  * Built-in String Max aggregate function.
  */
class StringMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.STRING
}

/**
  * Built-in Date Max aggregate function.
  */
class DateMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.DATE
}

/**
  * Built-in Time Max aggregate function.
  */
class TimeMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.TIME
}

/**
  * Built-in Timestamp Max aggregate function.
  */
class TimestampMaxAggFunction extends MaxAggFunction {
  override def getResultType: InternalType = DataTypes.TIMESTAMP
}
