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
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.typeutils.TypeConverter

abstract class BinaryArithmetic extends BinaryExpression { self: Product =>
  def sqlOperator: SqlOperator

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(sqlOperator, children.map(_.toRexNode))
  }
}

case class Plus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left + $right)"

  val sqlOperator = SqlStdOperatorTable.PLUS

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val l = left.toRexNode
    val r = right.toRexNode
    if(SqlTypeName.STRING_TYPES.contains(l.getType.getSqlTypeName)) {
      val cast: RexNode = relBuilder.cast(r,
        TypeConverter.typeInfoToSqlType(BasicTypeInfo.STRING_TYPE_INFO))
      relBuilder.call(SqlStdOperatorTable.PLUS, l, cast)
    } else if(SqlTypeName.STRING_TYPES.contains(r.getType.getSqlTypeName)) {
      val cast: RexNode = relBuilder.cast(l,
        TypeConverter.typeInfoToSqlType(BasicTypeInfo.STRING_TYPE_INFO))
      relBuilder.call(SqlStdOperatorTable.PLUS, cast, r)
    } else {
      relBuilder.call(SqlStdOperatorTable.PLUS, l, r)
    }
  }
}

case class UnaryMinus(child: Expression) extends UnaryExpression {
  override def toString = s"-($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.UNARY_MINUS, child.toRexNode)
  }
}

case class Minus(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left - $right)"

  val sqlOperator = SqlStdOperatorTable.MINUS
}

case class Div(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left / $right)"

  val sqlOperator = SqlStdOperatorTable.DIVIDE
}

case class Mul(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left * $right)"

  val sqlOperator = SqlStdOperatorTable.MULTIPLY
}

case class Mod(left: Expression, right: Expression) extends BinaryArithmetic {
  override def toString = s"($left % $right)"

  val sqlOperator = SqlStdOperatorTable.MOD
}
