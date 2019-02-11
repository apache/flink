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

import com.google.common.collect.ImmutableList
import org.apache.calcite.rex.{RexNode, RexSubQuery}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class In(expression: Expression, elements: Seq[Expression]) extends Expression  {

  override def toString = s"$expression.in(${elements.mkString(", ")})"

  override private[flink] def children: Seq[Expression] = expression +: elements.distinct

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    // check if this is a sub-query expression or an element list
    elements.head match {

      case TableReference(name, table) =>
        RexSubQuery.in(table.getRelNode, ImmutableList.of(expression.toRexNode))

      case _ =>
        relBuilder.call(SqlStdOperatorTable.IN, children.map(_.toRexNode): _*)
    }
  }

  override private[flink] def validateInput(): ValidationResult = {
    // check if this is a sub-query expression or an element list
    elements.head match {

      case TableReference(name, table) =>
        if (elements.length != 1) {
          return ValidationFailure("IN operator supports only one table reference.")
        }
        val tableOutput = table.logicalPlan.output
        if (tableOutput.length > 1) {
          return ValidationFailure(
            s"The sub-query table '$name' must not have more than one column.")
        }
        (expression.resultType, tableOutput.head.resultType) match {
          case (lType, rType) if lType == rType => ValidationSuccess
          case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
          case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
            ValidationSuccess
          case (lType, rType) =>
            ValidationFailure(s"IN operator on incompatible types: $lType and $rType.")
        }

      case _ =>
        val types = children.tail.map(_.resultType)
        if (types.distinct.length != 1) {
          return ValidationFailure(
            s"Types on the right side of the IN operator must be the same, " +
              s"got ${types.mkString(", ")}.")
        }
        (children.head.resultType, children.last.resultType) match {
          case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
          case (lType, rType) if lType == rType => ValidationSuccess
          case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
            ValidationSuccess
          case (lType, rType) =>
            ValidationFailure(s"IN operator on incompatible types: $lType and $rType.")
        }
    }
  }

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO
}

