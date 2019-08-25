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

import org.apache.flink.table.functions
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.typeutils.FieldInfoUtils

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * A Typed Function is a Table Function which the result type has already been determined.
  * The result type will be determined before constructing the class.
  *
  * @param tableFunction The Table Function instance
  * @param externalResultType The result type which has been determined
  */
class TypedFlinkTableFunction(
    val tableFunction: TableFunction[_],
    fieldNames: Array[String],
    val externalResultType: DataType)
  extends FlinkTableFunction(tableFunction) {

  override def getExternalResultType(
      tableFunction: functions.TableFunction[_],
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): DataType =
    externalResultType

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): RelDataType = {
    val fieldTypes = FieldInfoUtils.getFieldTypes(
      fromDataTypeToTypeInfo(externalResultType)).map(fromTypeInfoToLogicalType)
    if (fieldTypes.length < fieldNames.length) {
      throw new RuntimeException(s"fieldTypes: $fieldTypes, but fieldNames: $fieldNames")
    }

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = flinkTypeFactory.builder
    fieldNames
        .zip(fieldTypes.dropRight(fieldTypes.length - fieldNames.length))
        .foreach { f =>
          builder.add(f._1, flinkTypeFactory.createFieldTypeFromLogicalType(f._2))
        }
    builder.build
  }
}
