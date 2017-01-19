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

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

abstract sealed class GroupFunction extends Expression {

  override def toString = s"GroupFunction($children)"

  private[flink] def replaceExpression(
    relBuilder: RelBuilder,
    groupExpressions: Option[Seq[Expression]],
    children: Seq[Attribute] = Seq(),
    indicator: Boolean = false): Expression = {

    if (groupExpressions.isDefined) {
      val expressions = groupExpressions.get
      if (!indicator) {
        Cast(
          Minus(Power(Literal(2), Literal(getEffectiveArgCount(expressions))), Literal(1)),
          BasicTypeInfo.LONG_TYPE_INFO
        )
      } else {
        val operands = getOperands(expressions)
        val internalFieldsMap = getInternalFields(children)
        var shift = operands.size
        var expression: Option[Expression] = None
        operands.foreach(x => {
          shift -= 1
          expression = bitValue(relBuilder, expression, x, shift, expressions, internalFieldsMap)
        })
        Cast(expression.get, BasicTypeInfo.LONG_TYPE_INFO)
      }
    } else {
      this
    }
  }

  private def getInternalFields(children: Seq[Attribute]) = {
    val inputFields = children.map(_.name)
    inputFields.map(inputFieldName => {
      val base = "i$" + inputFieldName
      var name = base
      var i = 0
      while (inputFields.contains(name)) {
        name = base + "_" + i // if i$XXX is already a field it will be suffixed by _NUMBER
        i = i + 1
      }
      inputFieldName -> name
    }).toMap
  }

  private def bitValue(relBuilder: RelBuilder,
    expression: Option[Expression], operand: Int,
    shift: Int, expressions: Seq[Expression],
    internalFieldsMap: Map[String, String]
  ): Option[Expression] = {

    val fieldName = expressions(operand) match {
      case ne: NamedExpression => ne.name
      case _ => ""
    }

    var nextExpression: Expression =
      If(IsTrue(ResolvedFieldReference(
        internalFieldsMap(fieldName), BasicTypeInfo.BOOLEAN_TYPE_INFO)),
         Literal(1), Literal(0))

    if (shift > 0) {
      nextExpression = Mul(nextExpression, Power(Literal(2), Literal(shift)))
    }

    if (expression.isDefined) {
      nextExpression = Plus(expression.get, nextExpression)
    }

    Some(nextExpression)
  }

  protected def getEffectiveArgCount(groupExpressions: Seq[Expression]): Int

  protected def getOperands(groupExpressions: Seq[Expression]): Seq[Int] = {
    children.map(e => groupExpressions.indexOf(e))
  }
}

case class GroupId() extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = Nil

  override protected def getEffectiveArgCount(groupExpressions: Seq[Expression]): Int = {
    groupExpressions.size
  }

  override protected def getOperands(groupExpressions: Seq[Expression]): Seq[Int] =
    groupExpressions.indices
}

case class Grouping(expression: Expression) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = Seq(expression)

  override protected def getEffectiveArgCount(groupExpressions: Seq[Expression]): Int = 1
}

case class GroupingId(expressions: Expression*) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = expressions

  override protected def getEffectiveArgCount(groupExpressions: Seq[Expression]): Int = {
    expressions.size
  }
}

