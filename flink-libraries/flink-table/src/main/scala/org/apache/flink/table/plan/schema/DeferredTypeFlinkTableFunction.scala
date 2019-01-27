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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils

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
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): DataType = {
    val resultType = tableFunction.getResultType(arguments, argTypes)
    if (resultType != null) {
      resultType
    } else {
      // if user don't specific the result type, using the implicit type
      implicitResultType
    }
  }

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      arguments: Array[AnyRef],
      argTypes: Array[Class[_]]): RelDataType = {
    val resultType = getExternalResultType(arguments, argTypes)
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    UserDefinedFunctionUtils.buildRelDataType(
      typeFactory, resultType.toInternalType, fieldNames, fieldIndexes)
  }
}
