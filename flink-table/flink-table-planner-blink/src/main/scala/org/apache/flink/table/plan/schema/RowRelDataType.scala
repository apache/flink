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

package org.apache.flink.table.plan.schema

import java.util
import org.apache.calcite.rel.`type`.{RelDataTypeField, RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.flink.table.`type`.RowType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.RowRelDataType.createFieldList

import scala.collection.JavaConverters._

/**
  * Row type for encapsulating Flink's [[RowType]].
  *
  * @param rowType rowType to encapsulate
  * @param nullable flag if type can be nullable
  * @param typeFactory Flink's type factory
  */
class RowRelDataType(
    val rowType: RowType,
    val nullable: Boolean,
    typeFactory: FlinkTypeFactory)
  extends RelRecordType(
    StructKind.PEEK_FIELDS_NO_EXPAND,
    createFieldList(rowType, typeFactory)) {

  override def toString = s"ROW($rowType)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[RowRelDataType]

  override def equals(other: Any): Boolean = other match {
    case that: RowRelDataType =>
      super.equals(that) &&
        (that canEqual this) &&
          rowType == that.rowType &&
        nullable == that.nullable
    case _ => false
  }

  override def hashCode(): Int = {
    rowType.hashCode()
  }

  override def isNullable: Boolean = nullable

}

object RowRelDataType {

  /**
    * Converts the fields of a row type to list of [[RelDataTypeField]].
    */
  private def createFieldList(
      rowType: RowType,
      typeFactory: FlinkTypeFactory)
    : util.List[RelDataTypeField] = {

    rowType
      .getFieldNames
      .zipWithIndex
      .map { case (name, index) =>
        new RelDataTypeFieldImpl(
          name,
          index,
          // TODO the row type should provide the information if subtypes are nullable
          typeFactory.createTypeFromInternalType(rowType.getTypeAt(index), isNullable = true)
        ).asInstanceOf[RelDataTypeField]
      }
      .toList
      .asJava
  }
}
