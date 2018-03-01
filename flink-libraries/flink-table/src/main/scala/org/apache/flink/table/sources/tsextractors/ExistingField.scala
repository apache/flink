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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.expressions.{Cast, Expression, ResolvedFieldReference}

/**
  * Converts an existing [[Long]] or [[java.sql.Timestamp]] field into a rowtime attribute.
  *
  * @param field The field to convert into a rowtime attribute.
  */
final class ExistingField(val field: String) extends TimestampExtractor {

  override def getArgumentFields: Array[String] = Array(field)

  @throws[ValidationException]
  override def validateArgumentFields(physicalFieldTypes: Array[TypeInformation[_]]): Unit = {

    // get type of field to convert
    val fieldType = physicalFieldTypes(0)

    // check that the field to convert is of type Long or Timestamp
    fieldType match {
      case Types.LONG => // OK
      case Types.SQL_TIMESTAMP => // OK
      case _: TypeInformation[_] =>
        throw ValidationException(
          s"Field '$field' must be of type Long or Timestamp but is of type $fieldType.")
    }
  }

  /**
    * Returns an [[Expression]] that casts a [[Long]] or [[java.sql.Timestamp]] field into a
    * rowtime attribute.
    */
  def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {

    val fieldAccess: Expression = fieldAccesses(0)

    fieldAccess.resultType match {
      case Types.LONG =>
        // access LONG field
        fieldAccess
      case Types.SQL_TIMESTAMP =>
        // cast timestamp to long
        Cast(fieldAccess, Types.LONG)
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
