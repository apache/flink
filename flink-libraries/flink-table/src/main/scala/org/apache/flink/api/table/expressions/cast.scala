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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{NumericTypeInfo, TypeInformation}
import org.apache.flink.api.table.typeutils.TypeConverter
import org.apache.flink.api.table.validate.ExprValidationResult

case class Cast(child: Expression, dataType: TypeInformation[_]) extends UnaryExpression {

  override def toString = s"$child.cast($dataType)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.cast(child.toRexNode, TypeConverter.typeInfoToSqlType(dataType))
  }

  override def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, dataType).asInstanceOf[this.type]
  }

  override def validateInput(): ExprValidationResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(s"Unsupported cast from ${child.dataType} to $dataType")
    }
  }
}

object Cast {

  /**
    * all the supported cast type
    */
  def canCast(from: TypeInformation[_], to: TypeInformation[_]): Boolean = (from, to) match {
    case (from, to) if from == to => true

    case (_, STRING_TYPE_INFO) => true

    case (_, DATE_TYPE_INFO) => false // Date type not supported yet.
    case (_, VOID_TYPE_INFO) => false // Void type not supported
    case (_, CHAR_TYPE_INFO) => false // Character type not supported.

    case (STRING_TYPE_INFO, _: NumericTypeInfo[_]) => true
    case (STRING_TYPE_INFO, BOOLEAN_TYPE_INFO) => true

    case (BOOLEAN_TYPE_INFO, _: NumericTypeInfo[_]) => true
    case (_: NumericTypeInfo[_], BOOLEAN_TYPE_INFO) => true

    case (_: NumericTypeInfo[_], _: NumericTypeInfo[_]) => true

    case _ => false
  }
}
