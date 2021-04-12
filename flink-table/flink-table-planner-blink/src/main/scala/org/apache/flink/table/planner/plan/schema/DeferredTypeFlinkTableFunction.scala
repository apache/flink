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
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}

/**
  * A Deferred Type is a Table Function which the result type hasn't been determined yet.
  * It will determine the result type after the arguments are passed.
  *
  * @param tableFunction The Table Function instance
  * @param implicitResultType Implicit result type.
  */
class DeferredTypeFlinkTableFunction(
    val tableFunction: TableFunction[_],
    val implicitResultType: DataType)
  extends FlinkTableFunction(tableFunction) {

  override def getExternalResultType(
      tableFunction: functions.TableFunction[_],
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): DataType = {
    // TODO
//    val resultType = tableFunction.getResultType(arguments, argTypes)
    val resultType = tableFunction.getResultType
    if (resultType != null) {
      TypeConversions.fromLegacyInfoToDataType(resultType)
    } else {
      // if user don't specific the result type, using the implicit type
      implicitResultType
    }
  }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val resultType = getExternalResultType(tableFunction, null, null)
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    UserDefinedFunctionUtils.buildRelDataType(
      typeFactory, fromDataTypeToLogicalType(resultType), fieldNames, fieldIndexes)
  }
}
