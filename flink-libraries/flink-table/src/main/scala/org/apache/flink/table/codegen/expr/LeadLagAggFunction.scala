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
import org.apache.flink.table.expressions._
import org.apache.flink.table.runtime.overagg.OverWindowFrame

/**
 * LEAD and LAG aggregate functions return the value of given expression evaluated at given offset.
 * The functions only are used by over window.
 *
 * LAG(input, offset, default) - Returns the value of `input` at the `offset`th row
 * before the current row in the window. The default value of `offset` is 1 and the default
 * value of `default` is null. If the value of `input` at the `offset`th row is null,
 * null is returned. If there is no such offset row (e.g., when the offset is 1, the first
 * row of the window does not have any previous row), `default` is returned.
 *
 * LEAD(input, offset, default) - Returns the value of `input` at the `offset`th row
 * after the current row in the window. The default value of `offset` is 1 and the default
 * value of `default` is null. If the value of `input` at the `offset`th row is null,
 * null is returned. If there is no such an offset row (e.g., when the offset is 1, the last
 * row of the window does not have any subsequent row), `default` is returned.
 *
 * These two aggregate functions are special, and only are used by over window. So here the
 * concrete implementation is closely related to [[OverWindowFrame]]
 */
abstract class LeadLagAggFunction(inputNum: Int) extends DeclarativeAggregateFunction {

  //If the length of function's args is 3, then the function has the default value.
  private val existDefaultValue = inputNum == 3

  override def inputCount: Int = inputNum

  private lazy val value = UnresolvedAggBufferReference("leadlag", getResultType)

  override def aggBufferAttributes = Seq(value)

  override def initialValuesExpressions = Seq(Null(getResultType))

  override def accumulateExpressions: Seq[Expression] = Seq(operands(0))

  //FIXME: use the current input reset the buffer value.
  override def retractExpressions: Seq[Expression] = {
    if (existDefaultValue) Seq(Cast(operands(2), getResultType)) else Seq(Null(getResultType))
  }

  override def mergeExpressions = Seq()

  override def getValueExpression: Expression = value
}

class IntLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.INT
}

class ByteLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.BYTE
}

class ShortLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.SHORT
}

class LongLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.LONG
}

class FloatLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.FLOAT
}

class DoubleLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.DOUBLE
}

class BooleanLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.BOOLEAN
}

class DecimalLeadLagAggFunction(inputNum: Int, decimalType: DecimalType)
    extends LeadLagAggFunction(inputNum: Int) {
  override def getResultType: InternalType = decimalType
}

class StringLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.STRING
}

class DateLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.DATE
}

class TimeLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.TIME
}

class TimestampLeadLagAggFunction(inputNum: Int) extends LeadLagAggFunction(inputNum) {
  override def getResultType: InternalType = DataTypes.TIMESTAMP
}
