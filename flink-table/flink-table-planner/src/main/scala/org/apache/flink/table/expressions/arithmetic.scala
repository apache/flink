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
package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.typeutils.TypeCoercion
import org.apache.flink.table.validate._

import scala.collection.JavaConversions._

abstract class BinaryArithmetic extends BinaryExpression {
  private[flink] def sqlOperator: SqlOperator

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(sqlOperator, children.map(_.toRexNode))
  }

  override private[flink] def resultType: TypeInformation[_] =
    TypeCoercion.widerTypeOf(left.resultType, right.resultType) match {
      case Some(t) => t
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

case class Plus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left + $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.PLUS

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    if(isString(left.resultType)) {
      val castedRight = Cast(right, BasicTypeInfo.STRING_TYPE_INFO)
      relBuilder.call(SqlStdOperatorTable.CONCAT, left.toRexNode, castedRight.toRexNode)
    } else if(isString(right.resultType)) {
      val castedLeft = Cast(left, BasicTypeInfo.STRING_TYPE_INFO)
      relBuilder.call(SqlStdOperatorTable.CONCAT, castedLeft.toRexNode, right.toRexNode)
    } else if (isTimeInterval(left.resultType) && left.resultType == right.resultType) {
      relBuilder.call(SqlStdOperatorTable.PLUS, left.toRexNode, right.toRexNode)
    } else if (isTimeInterval(left.resultType) && isTemporal(right.resultType)) {
      // Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
      // we manually switch them here
      relBuilder.call(SqlStdOperatorTable.DATETIME_PLUS, right.toRexNode, left.toRexNode)
    } else if (isTemporal(left.resultType) && isTemporal(right.resultType)) {
      relBuilder.call(SqlStdOperatorTable.DATETIME_PLUS, left.toRexNode, right.toRexNode)
    } else {
      val castedLeft = Cast(left, resultType)
      val castedRight = Cast(right, resultType)
      relBuilder.call(SqlStdOperatorTable.PLUS, castedLeft.toRexNode, castedRight.toRexNode)
    }
  }

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
}

case class UnaryMinus(child: Expression) extends UnaryExpression {
  override def toString = s"-($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UNARY_MINUS, child.toRexNode)
  }

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

case class Minus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left - $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.MINUS

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
}

case class Div(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left / $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.DIVIDE
}

case class Mul(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left * $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.MULTIPLY
}

case class Mod(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left % $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.MOD
}
