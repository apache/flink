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

import java.util.Date

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala.table.ImplicitExpressionOperations
import org.apache.flink.api.table.typeutils.TypeConverter

object Literal {
  def apply(l: Any): Literal = l match {
    case i: Int => Literal(i, BasicTypeInfo.INT_TYPE_INFO)
    case s: Short => Literal(s, BasicTypeInfo.SHORT_TYPE_INFO)
    case b: Byte => Literal(b, BasicTypeInfo.BYTE_TYPE_INFO)
    case l: Long => Literal(l, BasicTypeInfo.LONG_TYPE_INFO)
    case d: Double => Literal(d, BasicTypeInfo.DOUBLE_TYPE_INFO)
    case f: Float => Literal(f, BasicTypeInfo.FLOAT_TYPE_INFO)
    case str: String => Literal(str, BasicTypeInfo.STRING_TYPE_INFO)
    case bool: Boolean => Literal(bool, BasicTypeInfo.BOOLEAN_TYPE_INFO)
    case date: Date => Literal(date, BasicTypeInfo.DATE_TYPE_INFO)
  }
}

case class Literal(value: Any, tpe: TypeInformation[_])
  extends LeafExpression with ImplicitExpressionOperations {
  def expr = this
  def typeInfo = tpe

  override def toString = s"$value"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.literal(value)
  }
}

case class Null(tpe: TypeInformation[_]) extends LeafExpression {
  def expr = this
  def typeInfo = tpe

  override def toString = s"null"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.getRexBuilder.makeNullLiteral(TypeConverter.typeInfoToSqlType(tpe))
  }
}
