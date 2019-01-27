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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, RowType}
import org.apache.flink.table.calcite.FlinkTypeFactory

import scala.collection.JavaConversions._

/**
  * Schema that describes both a logical and physical row.
  */
class RowSchema(private val logicalRowType: RelDataType) {

  private lazy val physicalRowFieldTypes: Seq[InternalType] =
    logicalRowType.getFieldList map {
      f => FlinkTypeFactory.toDataType(f.getType).toInternalType
    }

  private lazy val physicalRowType: InternalType = DataTypes.createRowType(
    physicalRowFieldTypes.toArray[DataType], fieldNames.toArray)

  /**
    * Returns the arity of the schema.
    */
  def arity: Int = logicalRowType.getFieldCount

  /**
    * Returns the [[RelDataType]] of the schema
    */
  def relDataType: RelDataType = logicalRowType

  /**
    * Returns the [[InternalType]] of of the schema
    */
  def dataType: InternalType = physicalRowType

  /**
    * Returns the [[TypeInformation]] of fields of the schema
    */
  def fieldTypeInfos: Seq[InternalType] = physicalRowFieldTypes

  /**
    * Returns the fields names
    */
  def fieldNames: Seq[String] = logicalRowType.getFieldNames

  /**
    * Returns a projected [[TypeInformation]] of the schema.
    */
  def projectedTypeInfo(fields: Array[Int]): RowType = {
    val projectedTypes = fields.map(fieldTypeInfos(_))
    val projectedNames = fields.map(fieldNames(_))
    DataTypes.createRowType(projectedTypes.toArray[DataType], projectedNames)
  }
}
