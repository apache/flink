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

import scala.collection.JavaConversions._

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

abstract class BinaryComparison extends BinaryExpression { self: Product =>
  def sqlOperator: SqlOperator

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(sqlOperator, children.map(_.toRexNode))
  }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left === $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.EQUALS
}

case class NotEqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left !== $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.NOT_EQUALS
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left > $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.GREATER_THAN
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left >= $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left < $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.LESS_THAN
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left <= $right"

  val sqlOperator: SqlOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNull"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.isNull(child.toRexNode)
  }
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNotNull"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.isNotNull(child.toRexNode)
  }
}
