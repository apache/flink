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
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.typeutils.TypeCoercion
import org.apache.flink.table.planner.typeutils.TypeInfoCheckUtils._
import org.apache.flink.table.planner.validate._
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.{fromLogicalTypeToTypeInfo, fromTypeInfoToLogicalType}

import org.apache.calcite.sql.SqlOperator

abstract class BinaryArithmetic extends BinaryExpression {
  private[flink] def sqlOperator: SqlOperator

  override private[flink] def resultType: TypeInformation[_] =
    TypeCoercion.widerTypeOf(
      fromTypeInfoToLogicalType(left.resultType),
      fromTypeInfoToLogicalType(right.resultType)) match {
      case Some(t) => fromLogicalTypeToTypeInfo(t)
      case None =>
        throw new RuntimeException("This should never happen.")
    }

  override private[flink] def validateInput(): ValidationResult = {
    if (!isNumeric(left.resultType) || !isNumeric(right.resultType)) {
      ValidationFailure(s"The arithmetic '$this' requires both operands to be numeric, but was " +
        s"'$left' : '${left.resultType}' and '$right' : '${right.resultType}'.")
    } else {
      ValidationSuccess
    }
  }
}

case class Plus(left: PlannerExpression, right: PlannerExpression) extends BinaryArithmetic {
  override def toString = s"($left + $right)"

  private[flink] val sqlOperator = FlinkSqlOperatorTable.PLUS

  override private[flink] def validateInput(): ValidationResult = {
    if (isString(left.resultType) || isString(right.resultType)) {
      ValidationSuccess
    } else if (isTimeInterval(left.resultType) && left.resultType == right.resultType) {
      ValidationSuccess
    } else if (isTimePoint(left.resultType) && isTimeInterval(right.resultType)) {
      ValidationSuccess
    } else if (isTimeInterval(left.resultType) && isTimePoint(right.resultType)) {
      ValidationSuccess
    } else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(
        s"The arithmetic '$this' requires input that is numeric, string, time intervals of the " +
        s"same type, or a time interval and a time point type, " +
        s"but was '$left' : '${left.resultType}' and '$right' : '${right.resultType}'.")
    }
  }

  override private[flink] def resultType: TypeInformation[_] = {
    ReturnTypeInference.inferPlus(this)
  }
}

case class UnaryMinus(child: PlannerExpression) extends UnaryExpression {
  override def toString = s"-($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (isNumeric(child.resultType)) {
      ValidationSuccess
    } else if (isTimeInterval(child.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"The arithmetic '$this' requires input that is numeric or a time " +
        s"interval type, but was '${child.resultType}'.")
    }
  }
}

case class Minus(left: PlannerExpression, right: PlannerExpression) extends BinaryArithmetic {
  override def toString = s"($left - $right)"

  private[flink] val sqlOperator = FlinkSqlOperatorTable.MINUS

  override private[flink] def validateInput(): ValidationResult = {
    if (isTimeInterval(left.resultType) && left.resultType == right.resultType) {
      ValidationSuccess
    } else if (isTimePoint(left.resultType) && isTimeInterval(right.resultType)) {
      ValidationSuccess
    } else if (isTimeInterval(left.resultType) && isTimePoint(right.resultType)) {
      ValidationSuccess
    } else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(
        s"The arithmetic '$this' requires inputs that are numeric, time intervals of the same " +
        s"type, or a time interval and a time point type, " +
        s"but was '$left' : '${left.resultType}' and '$right' : '${right.resultType}'.")
    }
  }

  override private[flink] def resultType: TypeInformation[_] = {
    ReturnTypeInference.inferMinus(this)
  }
}

case class Div(left: PlannerExpression, right: PlannerExpression) extends BinaryArithmetic {
  override def toString = s"($left / $right)"

  private[flink] val sqlOperator = FlinkSqlOperatorTable.DIVIDE

  override private[flink] def resultType: TypeInformation[_] = {
    ReturnTypeInference.inferDiv(this)
  }

}

case class Mul(left: PlannerExpression, right: PlannerExpression) extends BinaryArithmetic {
  override def toString = s"($left * $right)"

  private[flink] val sqlOperator = FlinkSqlOperatorTable.MULTIPLY

  override private[flink] def resultType: TypeInformation[_] = {
    ReturnTypeInference.inferMul(this)
  }
}

case class Mod(left: PlannerExpression, right: PlannerExpression) extends BinaryArithmetic {
  override def toString = s"($left % $right)"

  private[flink] val sqlOperator = FlinkSqlOperatorTable.MOD
}
