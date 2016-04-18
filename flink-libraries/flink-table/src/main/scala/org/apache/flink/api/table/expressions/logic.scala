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
import org.apache.calcite.tools.RelBuilder

abstract class BinaryPredicate extends BinaryExpression { self: Product => }

case class Not(child: Expression) extends UnaryExpression {

  override val name = Expression.freshName("not-" + child.name)

  override def toString = s"!($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.not(child.toRexNode)
  }
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left && $right"

  override val name = Expression.freshName(left.name + "-and-" + right.name)

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.and(left.toRexNode, right.toRexNode)
  }
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {

  override def toString = s"$left || $right"

  override val name = Expression.freshName(left.name + "-or-" + right.name)

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.or(left.toRexNode, right.toRexNode)
  }
}
