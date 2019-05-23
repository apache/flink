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
import org.apache.flink.table.`type`.{InternalType, RowType}
import org.apache.flink.table.calcite.FlinkTypeFactory

import scala.collection.JavaConversions._

/**
  * Schema that describes both a logical and physical row.
  */
class RowSchema(private val logicalRowType: RelDataType) {

  private lazy val internalFieldTypes: Seq[InternalType] =
    logicalRowType.getFieldList map { f => FlinkTypeFactory.toInternalType(f.getType) }

  private lazy val internalRowType: RowType = new RowType(
    internalFieldTypes.toArray, fieldNames.toArray)

  /**
    * Returns the arity of the schema.
    */
  def arity: Int = logicalRowType.getFieldCount

  /**
    * Returns the [[RelDataType]] of the schema
    */
  def relDataType: RelDataType = logicalRowType

  /**
    * Returns the [[RowType]] of the schema
    */
  def internalType: RowType = internalRowType

  /**
    * Returns the [[RowType]] of fields of the schema
    */
  def fieldInternalTypes: Seq[InternalType] = internalFieldTypes

  /**
    * Returns the fields names
    */
  def fieldNames: Seq[String] = logicalRowType.getFieldNames

  /**
    * Returns a projected [[TypeInformation]] of the schema.
    */
  def projectedInternalType(fields: Array[Int]): RowType = {
    val projectedTypes = fields.map(fieldInternalTypes(_))
    val projectedNames = fields.map(fieldNames(_))
    new RowType(projectedTypes, projectedNames)
  }
}
