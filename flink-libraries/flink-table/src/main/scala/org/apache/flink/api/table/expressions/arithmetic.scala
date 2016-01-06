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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, IntegerTypeInfo, NumericTypeInfo, TypeInformation}

abstract class BinaryArithmetic extends BinaryExpression { self: Product =>
  def typeInfo = {
    if (!left.typeInfo.isInstanceOf[NumericTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-numeric operand ${left} of type ${left.typeInfo} in $this""")
    }
    if (!right.typeInfo.isInstanceOf[NumericTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-numeric operand "${right}" of type ${right.typeInfo} in $this""")
    }
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    left.typeInfo
  }
}

case class Plus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def typeInfo = {
    if (!left.typeInfo.isInstanceOf[NumericTypeInfo[_]] &&
      !(left.typeInfo == BasicTypeInfo.STRING_TYPE_INFO)) {
      throw new ExpressionException(s"Non-numeric operand type ${left.typeInfo} in $this")
    }
    if (!right.typeInfo.isInstanceOf[NumericTypeInfo[_]] &&
      !(right.typeInfo == BasicTypeInfo.STRING_TYPE_INFO)) {
      throw new ExpressionException(s"Non-numeric operand type ${right.typeInfo} in $this")
    }
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    left.typeInfo
  }

  override def toString = s"($left + $right)"
}

case class UnaryMinus(child: Expression) extends UnaryExpression {
  def typeInfo = {
    if (!child.typeInfo.isInstanceOf[NumericTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-numeric operand ${child} of type ${child.typeInfo} in $this""")
    }
    child.typeInfo
  }

  override def toString = s"-($child)"
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left - $right)"
}

case class Div(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left / $right)"
}

case class Mul(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left * $right)"
}

case class Mod(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left * $right)"
}

case class Abs(child: Expression) extends UnaryExpression {
  def typeInfo = child.typeInfo

  override def toString = s"abs($child)"
}

abstract class BitwiseBinaryArithmetic extends BinaryExpression { self: Product =>
  def typeInfo: TypeInformation[_] = {
    if (!left.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-integer operand ${left} of type ${left.typeInfo} in $this""")
    }
    if (!right.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-integer operand "${right}" of type ${right.typeInfo} in $this""")
    }
    if (left.typeInfo != right.typeInfo) {
      throw new ExpressionException(s"Differing operand data types ${left.typeInfo} and " +
        s"${right.typeInfo} in $this")
    }
    if (left.typeInfo == BasicTypeInfo.LONG_TYPE_INFO) {
      left.typeInfo
    } else {
      BasicTypeInfo.INT_TYPE_INFO
    }
  }
}

case class BitwiseAnd(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def toString = s"($left & $right)"
}

case class BitwiseOr(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def toString = s"($left | $right)"
}


case class BitwiseXor(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def toString = s"($left ^ $right)"
}

case class BitwiseNot(child: Expression) extends UnaryExpression {
  def typeInfo: TypeInformation[_] = {
    if (!child.typeInfo.isInstanceOf[IntegerTypeInfo[_]]) {
      throw new ExpressionException(
        s"""Non-integer operand ${child} of type ${child.typeInfo} in $this""")
    }
    if (child.typeInfo == BasicTypeInfo.LONG_TYPE_INFO) {
      child.typeInfo
    } else {
      BasicTypeInfo.INT_TYPE_INFO
    }
  }

  override def toString = s"~($child)"
}

