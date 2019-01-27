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
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.validate._

abstract class BinaryPredicate extends BinaryExpression {
  override private[flink] def resultType = DataTypes.BOOLEAN

  override private[flink] def validateInput(): ValidationResult = {
    if (left.resultType == DataTypes.BOOLEAN &&
        right.resultType == DataTypes.BOOLEAN) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$this only accepts children of Boolean type, " +
        s"get $left : ${left.resultType} and $right : ${right.resultType}")
    }
  }
}

case class Not(child: Expression) extends UnaryExpression {

  override def toString = s"!($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.not(child.toRexNode)
  }

  override private[flink] def resultType = DataTypes.BOOLEAN

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.BOOLEAN) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Not operator requires a boolean expression as input, " +
        s"but $child is of type ${child.resultType}")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left && $right"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.and(left.toRexNode, right.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left || $right"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.or(left.toRexNode, right.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class If(
    condition: Expression,
    ifTrue: Expression,
    ifFalse: Expression)
  extends Expression {
  private[flink] def children = Seq(condition, ifTrue, ifFalse)

  override private[flink] def resultType = ifTrue.resultType

  override def toString = s"($condition)? $ifTrue : $ifFalse"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val c = condition.toRexNode
    val t = ifTrue.toRexNode
    val f = ifFalse.toRexNode
    relBuilder.call(SqlStdOperatorTable.CASE, c, t, f)
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (condition.resultType == DataTypes.BOOLEAN &&
        ifTrue.resultType == ifFalse.resultType) {
      ValidationSuccess
    } else {
      ValidationFailure(
        s"If should have boolean condition and same type of ifTrue and ifFalse, get " +
          s"(${condition.resultType}, ${ifTrue.resultType}, ${ifFalse.resultType})")
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
