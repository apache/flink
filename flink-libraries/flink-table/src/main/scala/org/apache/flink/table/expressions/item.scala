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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ObjectArrayTypeInfo}
import org.apache.flink.table.typeutils.TypeCheckUtils.isArray
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class ItemAt(container: Expression, key: Expression) extends Expression {

  override private[flink] def children: Seq[Expression] = Seq(container, key)

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder
      .getRexBuilder
      .makeCall(SqlStdOperatorTable.ITEM, container.toRexNode, key.toRexNode)
  }

  override def toString = s"($container).at($key)"

  override private[flink] def resultType = container.resultType match {
    case mti: MapTypeInfo[_, _] => mti.getValueTypeInfo
    case oati: ObjectArrayTypeInfo[_, _] => oati.getComponentInfo
    case bati: BasicArrayTypeInfo[_, _] => bati.getComponentInfo
    case pati: PrimitiveArrayTypeInfo[_] => pati.getComponentType
  }

  override private[flink] def validateInput(): ValidationResult = {
    container.resultType match {
      case ati: TypeInformation[_] if isArray(ati)  =>
        if (key.resultType == INT_TYPE_INFO) {
          // check for common user mistake
          key match {
            case Literal(value: Int, INT_TYPE_INFO) if value < 1 =>
              ValidationFailure(
                s"Array element access needs an index starting at 1 but was $value.")
            case _ => ValidationSuccess
          }
        } else {
          ValidationFailure(
            s"Array element access needs an integer index but was '${key.resultType}'.")
        }
      case mti: MapTypeInfo[_, _]  =>
        if (key.resultType == mti.getKeyTypeInfo) {
          ValidationSuccess
        } else {
          ValidationFailure(
            s"Map key-value access needs a valid key of type " +
              s"'${mti.getKeyTypeInfo}', found '${key.resultType}'.")
        }
      case other@_ => ValidationFailure(s"Array or map expected but was '$other'.")
    }
  }
}

