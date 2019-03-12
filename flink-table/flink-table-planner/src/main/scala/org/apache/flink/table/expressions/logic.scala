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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.validate._

abstract class BinaryPredicate extends BinaryExpression {
  override private[flink] def resultType = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (left.resultType == BasicTypeInfo.BOOLEAN_TYPE_INFO &&
        right.resultType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"$this only accepts children of Boolean type, " +
        s"get $left : ${left.resultType} and $right : ${right.resultType}")
    }
  }
}

case class Not(child: PlannerExpression) extends UnaryExpression {

  override def toString = s"!($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.not(child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Not operator requires a boolean expression as input, " +
        s"but $child is of type ${child.resultType}")
    }
  }
}

case class And(left: PlannerExpression, right: PlannerExpression) extends BinaryPredicate {

  override def toString = s"$left && $right"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.and(left.toRexNode, right.toRexNode)
  }
}

case class Or(left: PlannerExpression, right: PlannerExpression) extends BinaryPredicate {

  override def toString = s"$left || $right"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.or(left.toRexNode, right.toRexNode)
  }
}

@deprecated(
  "Use ifThenElse(...) instead. It is available through the implicit Scala DSL.",
  "1.8.0")
case class If(
    condition: PlannerExpression,
    ifTrue: PlannerExpression,
    ifFalse: PlannerExpression)
  extends PlannerExpression {
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
    if (condition.resultType == BasicTypeInfo.BOOLEAN_TYPE_INFO &&
        ifTrue.resultType == ifFalse.resultType) {
      ValidationSuccess
    } else {
      ValidationFailure(
        s"If should have boolean condition and same type of ifTrue and ifFalse, get " +
          s"(${condition.resultType}, ${ifTrue.resultType}, ${ifFalse.resultType})")
    }
  }
}
