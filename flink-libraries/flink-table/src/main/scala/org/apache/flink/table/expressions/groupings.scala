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

import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.TableException

import scala.collection.JavaConversions._

abstract sealed class GroupFunction extends Expression {

  override def toString = s"GroupFunction($children)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val child = relBuilder.peek()
    child match {
      case a: Aggregate =>
        val groupSet = a.getGroupSet

        val inputFields = a.getInput.getRowType.getFieldList.toList.map(_.getName)
        val outputFields = a.getRowType.getFieldList.toList.map(_.getName)

        val internalFields =
          getInternalFieldNames(inputFields)
            .filter(t => outputFields.contains(t._1))

        replaceExpression(relBuilder, groupSet, internalFields, a.indicator)
          .toRexNode(relBuilder)

      case _ =>
        throw new TableException("GROUPING functions only supported with " +
                                 "GROUP BY GROUPING SETS, CUBE or ROLLUP")
    }
  }

  private[flink] def replaceExpression(
    relBuilder: RelBuilder,
    groupSet: ImmutableBitSet,
    internalFields: Map[String, String] = Map(),
    indicator: Boolean = false): Expression = {

    if (!indicator) {
      Cast(
        Minus(Power(Literal(2), Literal(getEffectiveArgCount(groupSet))), Literal(1)),
        BasicTypeInfo.LONG_TYPE_INFO
      )
    } else {
      val operands = getOperands(internalFields)
      var shift = operands.size
      var expression: Option[Expression] = None
      operands.foreach(x => {
        shift -= 1
        expression = bitValue(relBuilder, expression, x, shift, internalFields)
      })
      Cast(expression.get, BasicTypeInfo.LONG_TYPE_INFO)
    }
  }

  private def getInternalFieldNames(inputFields: List[String]) = {
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
    shift: Int, internalFieldsMap: Map[String, String]
  ): Option[Expression] = {

    val fieldName = internalFieldsMap.values.toList.get(operand)

    var nextExpression: Expression =
      If(IsTrue(ResolvedFieldReference(fieldName, BasicTypeInfo.BOOLEAN_TYPE_INFO)),
         Literal(1), Literal(0))

    if (shift > 0) {
      nextExpression = Mul(nextExpression, Power(Literal(2), Literal(shift)))
    }

    if (expression.isDefined) {
      nextExpression = Plus(expression.get, nextExpression)
    }

    Some(nextExpression)
  }

  protected def getEffectiveArgCount(groupSet: ImmutableBitSet): Int = {
    groupSet.toList.size()
  }

  protected def getOperands(fields: Map[String, String]): Seq[Int] = {
    val keys = fields.keys.toList
    children.map(e => keys.indexOf(e.asInstanceOf[NamedExpression].name))
  }
}

case class GroupId() extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = Nil

  override protected def getOperands(fields: Map[String, String]): Seq[Int] = {
    fields.values.toList.indices
  }
}

case class Grouping(expression: Expression) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = Seq(expression)

  override protected def getEffectiveArgCount(groupSet: ImmutableBitSet): Int = 1
}

case class GroupingId(expressions: Expression*) extends GroupFunction {

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def children = expressions
}
