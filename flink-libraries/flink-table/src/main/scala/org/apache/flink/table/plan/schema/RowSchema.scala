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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField, RelRecordType}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * Schema that describes both a logical and physical row.
  */
class RowSchema(private val logicalRowType: RelDataType) {

  private lazy val physicalRowFields: Seq[RelDataTypeField] = logicalRowType.getFieldList

  private lazy val physicalRowType: RelDataType = new RelRecordType(physicalRowFields)

  private lazy val physicalRowFieldTypes: Seq[TypeInformation[_]] = physicalRowFields map { f =>
    FlinkTypeFactory.toTypeInfo(f.getType)
  }

  private lazy val physicalRowFieldNames: Seq[String] = physicalRowFields.map(_.getName)

  private lazy val physicalRowTypeInfo: TypeInformation[Row] = new RowTypeInfo(
    physicalRowFieldTypes.toArray, physicalRowFieldNames.toArray)

  /**
    * Returns the arity of the logical record.
    */
  def logicalArity: Int = logicalRowType.getFieldCount

  /**
    * Returns the arity of the physical record.
    */
  def physicalArity: Int = physicalTypeInfo.getArity

  /**
    * Returns a logical [[RelDataType]] including logical fields (i.e. time indicators).
    */
  def logicalType: RelDataType = logicalRowType

  /**
    * Returns a physical [[RelDataType]] with no logical fields (i.e. time indicators).
    */
  def physicalType: RelDataType = physicalRowType

  /**
    * Returns a physical [[TypeInformation]] of row with no logical fields (i.e. time indicators).
    */
  def physicalTypeInfo: TypeInformation[Row] = physicalRowTypeInfo

  /**
    * Returns [[TypeInformation]] of the row's fields with no logical fields (i.e. time indicators).
    */
  def physicalFieldTypeInfo: Seq[TypeInformation[_]] = physicalRowFieldTypes

  /**
    * Returns the logical fields names including logical fields (i.e. time indicators).
    */
  def logicalFieldNames: Seq[String] = logicalRowType.getFieldNames

  /**
    * Returns the physical fields names with no logical fields (i.e. time indicators).
    */
  def physicalFieldNames: Seq[String] = physicalRowFieldNames

}
