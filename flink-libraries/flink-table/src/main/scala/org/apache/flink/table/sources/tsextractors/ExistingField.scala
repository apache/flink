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

package org.apache.flink.table.sources.tsextractors

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions._
import org.apache.flink.table.api.types._

/**
  * Converts an existing [[Long]], [[java.sql.Timestamp]], or
  * timestamp formatted [[java.lang.String]] field (e.g., "2018-05-28 12:34:56.000") into
  * a rowtime attribute.
  *
  * @param field The field to convert into a rowtime attribute.
  */
final class ExistingField(val field: String) extends TimestampExtractor {

  override def getArgumentFields: Array[String] = Array(field)

  @throws[ValidationException]
  override def validateArgumentFields(argumentFieldTypes: Array[InternalType]): Unit = {
    val fieldType = argumentFieldTypes(0)

    fieldType match {
      case DataTypes.LONG => // OK
      case DataTypes.TIMESTAMP => // OK
      case DataTypes.STRING => // OK
      case _: InternalType =>
        throw new ValidationException(
          s"Field '$field' must be of type Long or Timestamp or String but is of type $fieldType.")
    }
  }

  /**
    * Returns an [[Expression]] that casts a [[Long]], [[java.sql.Timestamp]], or
    * timestamp formatted [[java.lang.String]] field (e.g., "2018-05-28 12:34:56.000")
    * into a rowtime attribute.
    */
  override def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {
    val fieldAccess: Expression = fieldAccesses(0)

    fieldAccess.resultType match {
      case DataTypes.ROWTIME_INDICATOR =>
        // rowtime field refers the same field in physical schema.
        fieldAccess
      case DataTypes.LONG =>
        // access LONG field
        Cast(
          Div(
            fieldAccess,
            Literal(new java.math.BigDecimal(1000),
              DecimalType.SYSTEM_DEFAULT)),
          DataTypes.TIMESTAMP)
      case _: TimestampType =>
        fieldAccess
      case DataTypes.STRING =>
        Cast(fieldAccess, DataTypes.TIMESTAMP)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: ExistingField => field == that.field
    case _ => false
  }

  override def hashCode(): Int = {
    field.hashCode
  }
}
