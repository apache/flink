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
  * Built-in min aggregate function.
  */
abstract class MinAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val min = UnresolvedAggBufferReference("min", getResultType)

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(min)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    /* min = */ Null(getResultType)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    If(IsNull(operands(0)), min,
      If(IsNull(min), operands(0),
        If(LessThan(operands(0), min), operands(0), min)))
  )

  override def retractExpressions: Seq[Expression] = Seq()

  override def mergeExpressions: Seq[Expression] = Seq(
    If(IsNull(min.right), min.left,
      If(IsNull(min.left), min.right,
        If(LessThan(min.right, min.left), min.right, min.left)))
  )

  override def getValueExpression: Expression = min
}

/**
  * Built-in Int Min aggregate function.
  */
class IntMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.INT
}

/**
  * Built-in Byte Min aggregate function.
  */
class ByteMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.BYTE
}

/**
  * Built-in Short Min aggregate function.
  */
class ShortMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.SHORT
}

/**
  * Built-in Long Min aggregate function.
  */
class LongMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.LONG
}

/**
  * Built-in Float Min aggregate function.
  */
class FloatMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.FLOAT
}

/**
  * Built-in Double Min aggregate function.
  */
class DoubleMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Min aggregate function.
  */
class BooleanMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.BOOLEAN
}

/**
  * Built-in Decimal Min aggregate function.
  */
class DecimalMinAggFunction(decimalType: DecimalType)
  extends MinAggFunction {
  override def getResultType: InternalType = decimalType
}

/**
  * Built-in String Min aggregate function.
  */
class StringMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.STRING
}

/**
  * Built-in Date Min aggregate function.
  */
class DateMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.DATE
}

/**
  * Built-in Time Min aggregate function.
  */
class TimeMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.TIME
}

/**
  * Built-in Timestamp Min aggregate function.
  */
class TimestampMinAggFunction extends MinAggFunction {
  override def getResultType: InternalType = DataTypes.TIMESTAMP
}
