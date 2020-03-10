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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.calcite.rel.`type`.RelDataType

import java.util.{Collection => JCollection, List => JList}

/**
  * The class that wraps Collection as a Calcite table.
  */
class CollectionTable(
    names: JList[String],
    rowType: RelDataType,
    val elements: JList[JList[_]],
    val dataType: DataType,
    statistic: FlinkStatistic)
  extends FlinkPreparingTableBase(null, rowType, names, statistic) {

  def this(
      names: JList[String],
      rowType: RelDataType,
      elements: JList[JList[_]],
      dataType: DataType) {
    this(names, rowType, elements, dataType, FlinkStatistic.UNKNOWN)
  }
}

object CollectionTable {

  def getRowType(
      typeFactory: FlinkTypeFactory,
      tableSchema: TableSchema): RelDataType = {
    val fieldNames = tableSchema.getFieldNames
    val fieldDataTypes = tableSchema.getFieldDataTypes
    val fieldTypes = fieldDataTypes.map(fromDataTypeToLogicalType)
    typeFactory.buildRelNodeRowType(fieldNames, fieldTypes)
  }
}
