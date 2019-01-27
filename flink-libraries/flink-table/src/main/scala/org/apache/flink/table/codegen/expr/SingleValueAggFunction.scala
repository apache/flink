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
import org.apache.flink.table.expressions.{Literal, _}

abstract class SingleValueAggFunction extends DeclarativeAggregateFunction {

  override def inputCount: Int = 1

  private lazy val value = UnresolvedAggBufferReference("value", getResultType)
  private lazy val count = UnresolvedAggBufferReference("count", DataTypes.INT)
  private val msg = "ERROR CODE: 21000, MESSAGE: cardinality violation"

  override def aggBufferAttributes: Seq[UnresolvedAggBufferReference] = Seq(value, count)

  override def initialValuesExpressions: Seq[Expression] = Seq(
    Null(getResultType), Literal(0)
  )

  override def accumulateExpressions: Seq[Expression] = Seq(
    If(count > 0, ThrowException(s"$msg", getResultType), operands(0)),
    count + 1
  )

  override def mergeExpressions: Seq[Expression] = Seq(
    If(count.left + count.right > 1, ThrowException(s"$msg", getResultType),
      If(count.left + count.right === 0,
        // both zero or right > 0 means we need to reserve the new value here
        If(count.left === 0 || count.right > 0, value.right, Null(getResultType)), value.right)),
    count.left + count.right
  )

  override def retractExpressions: Seq[Expression] = Seq(
    If(count === 1 || count === 0, Null(getResultType), ThrowException(s"$msg", getResultType)),
    count - 1
  )

  override def getValueExpression: Expression = value
}

class ByteSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.BYTE
}

class ShortSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.SHORT
}

class IntSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.INT
}

class LongSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.LONG
}

class FloatSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.FLOAT
}

class DoubleSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

class BooleanSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.BOOLEAN
}

class DecimalSingleValueAggFunction(decimalType: DecimalType)
  extends SingleValueAggFunction {
  override def getResultType: InternalType = decimalType
}

class StringSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.STRING
}

class DateSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.DATE
}

class TimeSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.TIME
}

class TimestampSingleValueAggFunction extends SingleValueAggFunction {
  override def getResultType: InternalType = DataTypes.TIMESTAMP
}
