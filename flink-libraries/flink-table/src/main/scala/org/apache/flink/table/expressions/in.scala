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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.api.Table
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class In(expression: Expression, subquery: Seq[Expression]) extends Expression  {
  def this(expressions: Seq[Expression]) = this(expressions.head, expressions.tail)

  override def toString = s"$expression.in(${subquery.mkString(", ")})"

  override private[flink] def children: Seq[Expression] = expression +: subquery.distinct

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.IN, children.map(_.toRexNode): _*)
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (children.tail.contains(null)) {
      ValidationFailure("Operands on right side of IN operator must be not null")
    } else {
      val types = children.tail.map(_.resultType)
      if (types.distinct.length != 1) {
        ValidationFailure(
          s"Types on the right side of IN operator must be the same, got ${types.mkString(", ")}."
        )
      } else {
        (children.head.resultType, children.last.resultType) match {
          case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
          case (lType, rType) if isComparable(lType) && lType == rType => ValidationSuccess
          case (lType, rType) =>
            ValidationFailure(
              s"Types on both sides of the IN operator must be numeric" +
                s" or of the same comparable type, got $lType and $rType"
            )
        }
      }

    }
  }

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO
}


case class InSub(expression: Expression, table: Table) extends Expression {

  override def toString = s"$expression.in($table)"

  override private[flink] def children: Seq[Expression] = Seq(expression)

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO
}

