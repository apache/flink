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
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.validate.ExprValidationResult

abstract class BinaryPredicate extends BinaryExpression {
  override def dataType = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (left.dataType == BasicTypeInfo.BOOLEAN_TYPE_INFO &&
        right.dataType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(s"$this only accept child of Boolean Type, " +
        s"get ${left.dataType} and ${right.dataType}")
    }
  }
}

case class Not(child: Expression) extends UnaryExpression {

  override def toString = s"!($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.not(child.toRexNode)
  }

  override def dataType = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override def validateInput(): ExprValidationResult = {
    if (child.dataType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(s"Not only accept child of Boolean Type, " +
        s"get ${child.dataType}")
    }
  }
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left && $right"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.and(left.toRexNode, right.toRexNode)
  }
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left || $right"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.or(left.toRexNode, right.toRexNode)
  }
}

case class Eval(
    condition: Expression,
    ifTrue: Expression,
    ifFalse: Expression)
  extends Expression {
  def children = Seq(condition, ifTrue, ifFalse)

  override def toString = s"($condition)? $ifTrue : $ifFalse"

  override val name = Expression.freshName("if-" + condition.name +
    "-then-" + ifTrue.name + "-else-" + ifFalse.name)

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val c = condition.toRexNode
    val t = ifTrue.toRexNode
    val f = ifFalse.toRexNode
    relBuilder.call(SqlStdOperatorTable.CASE, c, t, f)
  }
}
