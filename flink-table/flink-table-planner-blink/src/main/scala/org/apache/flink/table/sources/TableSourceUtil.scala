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

package org.apache.flink.table.sources

import org.apache.flink.table.`type`.TypeConverters
import org.apache.flink.table.calcite.FlinkTypeFactory

import org.apache.calcite.rel.`type`.RelDataType

/** Util class for [[TableSource]]. */
object TableSourceUtil {

  /**
    * Returns the Calcite schema of a [[TableSource]].
    *
    * @param tableSource The [[TableSource]] for which the Calcite schema is generated.
    * @param selectedFields The indices of all selected fields. None, if all fields are selected.
    * @param streaming Flag to determine whether the schema of a stream or batch table is created.
    * @param typeFactory The type factory to create the schema.
    * @return The Calcite schema for the selected fields of the given [[TableSource]].
    */
  def getRelDataType(
      tableSource: TableSource[_],
      selectedFields: Option[Array[Int]],
      streaming: Boolean,
      typeFactory: FlinkTypeFactory): RelDataType = {

    val fieldNames = tableSource.getTableSchema.getFieldNames
    val fieldTypes = tableSource.getTableSchema.getFieldTypes
      .map(TypeConverters.createInternalTypeFromTypeInfo)
    // TODO get fieldNullables from TableSchema
    val fieldNullables = fieldTypes.map(_ => true)

    // TODO supports time attributes after Expression is ready

    val (selectedFieldNames, selectedFieldTypes, selectedFieldNullables) =
      if (selectedFields.isDefined) {
        // filter field names and types by selected fields
        (
          selectedFields.get.map(fieldNames(_)),
          selectedFields.get.map(fieldTypes(_)),
          selectedFields.get.map(fieldNullables(_)))
      } else {
        (fieldNames, fieldTypes, fieldNullables)
      }
    typeFactory.buildRelDataType(selectedFieldNames, selectedFieldTypes, selectedFieldNullables)
  }

}
