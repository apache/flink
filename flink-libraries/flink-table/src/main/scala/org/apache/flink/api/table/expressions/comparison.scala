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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.table.typeutils.TypeCheckUtils.{isComparable, isNumeric}
import org.apache.flink.api.table.validate._

import scala.collection.JavaConversions._

abstract class BinaryComparison extends BinaryExpression {
  private[flink] def sqlOperator: SqlOperator

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(sqlOperator, children.map(_.toRexNode))
  }

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  // TODO: tighten this rule once we implemented type coercion rules during validation
  override private[flink] def validateInput(): ExprValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if isComparable(lType) && lType == rType => ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(
          s"Comparison is only supported for numeric types and " +
            s"comparable types of same type, got $lType and $rType")
    }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left === $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.EQUALS

  override private[flink] def validateInput(): ExprValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      // TODO widen this rule once we support custom objects as types (FLINK-3916)
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Equality predicate on incompatible types: $lType and $rType")
    }
}

case class NotEqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left !== $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.NOT_EQUALS

  override private[flink] def validateInput(): ExprValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      // TODO widen this rule once we support custom objects as types (FLINK-3916)
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Inequality predicate on incompatible types: $lType and $rType")
    }
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left > $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.GREATER_THAN
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left >= $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left < $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.LESS_THAN
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left <= $right"

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNull"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.isNull(child.toRexNode)
  }

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNotNull"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.isNotNull(child.toRexNode)
  }

  override private[flink] def resultType = BOOLEAN_TYPE_INFO
}
