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

import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

abstract class BinaryPredicate extends BinaryExpression { self: Product =>
  def typeInfo = {
    if (left.typeInfo != BasicTypeInfo.BOOLEAN_TYPE_INFO ||
      right.typeInfo != BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      throw new ExpressionException(s"Non-boolean operand types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }
}

case class Not(child: Expression) extends UnaryExpression {
  def typeInfo = {
    if (child.typeInfo != BasicTypeInfo.BOOLEAN_TYPE_INFO) {
      throw new ExpressionException(s"Non-boolean operand type ${child.typeInfo} in $this")
    }
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }

  override val name = Expression.freshName("not-" + child.name)

  override def toString = s"!($child)"
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {
  override def toString = s"$left && $right"

  override val name = Expression.freshName(left.name + "-and-" + right.name)
}

case class Or(left: Expression, right: Expression) extends BinaryPredicate {
  override def toString = s"$left || $right"

  override val name = Expression.freshName(left.name + "-or-" + right.name)

}
