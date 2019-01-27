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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.types.{DataType, InternalType, RowType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.rel.`type`.RelDataType

import scala.collection.JavaConverters._

/**
  * Schema that describes both a logical row type and physical field types.
  */
class BaseRowSchema(private val logicalRowType: RelDataType) {

  private lazy val physicalRowFieldTypes: Seq[TypeInformation[_]] =
    logicalRowType.getFieldList.asScala map { f => FlinkTypeFactory.toTypeInfo(f.getType) }

  /**
    * Returns the arity of the schema.
    */
  def arity: Int = logicalRowType.getFieldCount

  /**
    * Returns the [[RelDataType]] of the schema
    */
  def relDataType: RelDataType = logicalRowType

  /**
    * Returns the [[TypeInformation]] of of the schema
    */
  def typeInfo(): BaseRowTypeInfo = {
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    new BaseRowTypeInfo(fieldTypeInfos.toArray, logicalFieldNames.toArray)
  }

  def internalType(): RowType = {
    val logicalFieldNames = logicalRowType.getFieldNames.asScala
    val types = logicalRowType.getFieldList.asScala.map {
      f => FlinkTypeFactory.toInternalType(f.getType)
    }.toArray[DataType]
    new RowType(types, logicalFieldNames.toArray)
  }

  /**
    * Returns the [[TypeInformation]] of fields of the schema
    */
  def fieldTypeInfos: Seq[TypeInformation[_]] = physicalRowFieldTypes

  def fieldTypes: Seq[InternalType] = logicalRowType.getFieldList.asScala.map {
    f => FlinkTypeFactory.toInternalType(f.getType)
  }

  /**
    * Returns the fields names
    */
  def fieldNames: Seq[String] = logicalRowType.getFieldNames.asScala

  /**
    * Returns a projected [[TypeInformation]]s of the schema.
    */
  def projectedTypes(fields: Array[Int]): Array[TypeInformation[_]] = {
    fields.map(fieldTypeInfos(_))
  }
}
