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

package org.apache.flink.table.legacyutils

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{DataTypes, ValidationException}
import org.apache.flink.table.expressions.{ApiExpressionUtils, Expression, FieldReferenceExpression, ResolvedFieldReference}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.sources.tsextractors.TimestampExtractor
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

/*
 * Testing utils adopted from legacy planner until the Python code is updated.
 */

@deprecated
class CustomAssigner extends PunctuatedWatermarkAssigner() {
  override def getWatermark(row: Row, timestamp: Long): Watermark =
    throw new UnsupportedOperationException()
}

@deprecated
class CustomExtractor(val field: String) extends TimestampExtractor {
  def this() = {
    this("ts")
  }

  override def getArgumentFields: Array[String] = Array(field)

  override def validateArgumentFields(argumentFieldTypes: Array[TypeInformation[_]]): Unit = {
    argumentFieldTypes(0) match {
      case Types.SQL_TIMESTAMP =>
      case _ =>
        throw new ValidationException(
          s"Field 'ts' must be of type Timestamp but is of type ${argumentFieldTypes(0)}.")
    }
  }

  override def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {
    val fieldAccess = fieldAccesses(0)
    require(fieldAccess.resultType == Types.SQL_TIMESTAMP)
    val fieldReferenceExpr = new FieldReferenceExpression(
      fieldAccess.name,
      TypeConversions.fromLegacyInfoToDataType(fieldAccess.resultType),
      0,
      fieldAccess.fieldIndex)
    ApiExpressionUtils.unresolvedCall(
      BuiltInFunctionDefinitions.CAST,
      fieldReferenceExpr,
      ApiExpressionUtils.typeLiteral(DataTypes.BIGINT()))
  }

  override def equals(other: Any): Boolean = other match {
    case that: CustomExtractor => field == that.field
    case _ => false
  }

  override def hashCode(): Int = {
    field.hashCode
  }
}
