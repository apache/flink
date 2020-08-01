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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.typeutils.TypeInfoCheckUtils._
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class In(expression: PlannerExpression, elements: Seq[PlannerExpression])
  extends PlannerExpression  {

  override def toString = s"$expression.in(${elements.mkString(", ")})"

  override private[flink] def children: Seq[PlannerExpression] = expression +: elements.distinct

  override private[flink] def validateInput(): ValidationResult = {
    // check if this is a sub-query expression or an element list
    elements.head match {

      case TableReference(name, tableOperation: QueryOperation) =>
        if (elements.length != 1) {
          return ValidationFailure("IN operator supports only one table reference.")
        }
        val tableSchema = tableOperation.getTableSchema
        if (tableSchema.getFieldCount > 1) {
          return ValidationFailure(
            s"The sub-query table '$name' must not have more than one column.")
        }
        (expression.resultType, tableSchema.getFieldType(0).get()) match {
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

