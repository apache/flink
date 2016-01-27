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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, NumericTypeInfo}

abstract class BinaryComparison extends BinaryExpression { self: Product =>
  def typeInfo = {
    if (!left.typeInfo.isInstanceOf[NumericTypeInfo[_]]) {
      throw new ExpressionException(s"Non-numeric operand ${left} in $this")
    }
    if (!right.typeInfo.isInstanceOf[NumericTypeInfo[_]]) {
      throw new ExpressionException(s"Non-numeric operand ${right} in $this")
    }
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def typeInfo = {
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }

  override def toString = s"$left === $right"
}

case class NotEqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def typeInfo = {
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }

  override def toString = s"$left !== $right"
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left > $right"
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left >= $right"
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left < $right"
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left <= $right"
}

case class IsNull(child: Expression) extends UnaryExpression {
  def typeInfo = {
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }

  override def toString = s"($child).isNull"
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  def typeInfo = {
    BasicTypeInfo.BOOLEAN_TYPE_INFO
  }

  override def toString = s"($child).isNotNull"
}
