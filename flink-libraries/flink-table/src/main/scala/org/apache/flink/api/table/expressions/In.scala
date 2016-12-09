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
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.typeutils.TypeCheckUtils._
import org.apache.flink.api.table.validate.{ExprValidationResult, ValidationFailure, ValidationSuccess}

case class In(expressions: Seq[Expression]) extends Expression {
  override def toString = s"${expressions.head}.in(${expressions.tail.mkString(", ")})"

  /**
    * List of child nodes that should be considered when doing transformations. Other values
    * in the Product will not be transformed, only handed through.
    */
  override private[flink] def children: Seq[Expression] = expressions

  private[flink] val sqlOperator: SqlOperator = SqlStdOperatorTable.IN

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.IN, children.map(_.toRexNode): _*)
  }

  override private[flink] def validateInput(): ExprValidationResult = {
    if (children.tail.contains(null)) {
      ValidationFailure("Operands on right side of IN operator must be not null")
    } else {
      val types = children.tail.map(_.resultType)
      if (types.distinct.length != 1) {
        ValidationFailure(
          s"Types on the right side of IN operator must be the same, got ${types.mkString(", ")}."
        )
      } else {
        (children.head.resultType, children.tail.head.resultType) match {
          case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
          case (lType, rType) if isComparable(lType) && lType == rType => ValidationSuccess
          case (lType, rType) =>
            ValidationFailure(
              s"Types on the both side of IN operator must be the same, got $lType and $rType"
            )
        }
      }

    }
  }

  /**
    * Returns the [[TypeInformation]] for evaluating this expression.
    * It is sometimes not available until the expression is valid.
    */
  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO
}
