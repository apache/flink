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

import java.lang.reflect.Method
import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{TableFunction => FlinkUDTF}

/**
  * A Deferred Type is a Table Function which the type hasn't been determined yet.
  * If will determine the result type after the arguments are passed.
  *
  * @param tableFunction The Table Function instance
  * @param evalMethod The eval() method of the [[tableFunction]]
  * @param implicitResultType Implicit result type.
  */
class DeferredTypeFlinkTableFunction(
    val tableFunction: FlinkUDTF[_],
    val evalMethod: Method,
    val implicitResultType: TypeInformation[_])
  extends FlinkTableFunction(tableFunction, evalMethod) {

  override def getResultType(arguments: util.List[AnyRef]): TypeInformation[_] = {
    determineResultType(arguments)
  }

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      arguments: util.List[AnyRef]): RelDataType = {
    val resultType = determineResultType(arguments)
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    UserDefinedFunctionUtils.buildRelDataType(typeFactory, resultType, fieldNames, fieldIndexes)
  }

  private def determineResultType(arguments: util.List[AnyRef]): TypeInformation[_] = {
    val resultType = tableFunction.getResultType(arguments)
    if (resultType != null) {
      resultType
    } else {
      // if user don't specific the result type, using the implicit type
      implicitResultType
    }
  }
}
