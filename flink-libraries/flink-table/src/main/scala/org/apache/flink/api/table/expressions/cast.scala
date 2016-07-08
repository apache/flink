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
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.typeutils.{TypeCoercion, TypeConverter}
import org.apache.flink.api.table.validate._

case class Cast(child: Expression, resultType: TypeInformation[_]) extends UnaryExpression {

  override def toString = s"$child.cast($resultType)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.cast(child.toRexNode, TypeConverter.typeInfoToSqlType(resultType))
  }

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, resultType).asInstanceOf[this.type]
  }

  override private[flink] def validateInput(): ExprValidationResult = {
    if (TypeCoercion.canCast(child.resultType, resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"Unsupported cast from ${child.resultType} to $resultType")
    }
  }
}
