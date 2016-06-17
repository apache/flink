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
package org.apache.flink.api.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.typeutils.TypeCheckUtils.{isNumeric, isString}
import org.apache.flink.api.table.typeutils.{TypeCheckUtils, TypeCoercion}
import org.apache.flink.api.table.validate._

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

  // TODO: tighten this rule once we implemented type coercion rules during validation
  override private[flink] def validateInput(): ExprValidationResult = {
    if (!isNumeric(left.resultType) || !isNumeric(right.resultType)) {
      ValidationFailure(s"$this requires both operands Numeric, got " +
        s"${left.resultType} and ${right.resultType}")
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
      relBuilder.call(SqlStdOperatorTable.PLUS, left.toRexNode, castedRight.toRexNode)
    } else if(isString(right.resultType)) {
      val castedLeft = Cast(left, BasicTypeInfo.STRING_TYPE_INFO)
      relBuilder.call(SqlStdOperatorTable.PLUS, castedLeft.toRexNode, right.toRexNode)
    } else {
      val castedLeft = Cast(left, resultType)
      val castedRight = Cast(right, resultType)
      relBuilder.call(SqlStdOperatorTable.PLUS, castedLeft.toRexNode, castedRight.toRexNode)
    }
  }

  // TODO: tighten this rule once we implemented type coercion rules during validation
  override private[flink] def validateInput(): ExprValidationResult = {
    if (isString(left.resultType) || isString(right.resultType)) {
      ValidationSuccess
    } else if (!isNumeric(left.resultType) || !isNumeric(right.resultType)) {
      ValidationFailure(s"$this requires Numeric or String input," +
        s" get ${left.resultType} and ${right.resultType}")
    } else {
      ValidationSuccess
    }
  }
}

case class UnaryMinus(child: Expression) extends UnaryExpression {
  override def toString = s"-($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UNARY_MINUS, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput(): ExprValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "unary minus")
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left - $right)"

  private[flink] val sqlOperator = SqlStdOperatorTable.MINUS
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
