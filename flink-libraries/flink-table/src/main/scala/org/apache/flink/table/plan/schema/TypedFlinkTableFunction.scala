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
  * A Typed Function is a Table Function which the result type has already been determined.
  * The result type will be determined before constructing the class.
  *
  * @param tableFunction The Table Function instance
  * @param evalMethod The eval() method of the [[tableFunction]]
  * @param resultType The result type which has been determined
  */
class TypedFlinkTableFunction(
    val tableFunction: FlinkUDTF[_],
    val evalMethod: Method,
    val resultType: TypeInformation[_])
  extends FlinkTableFunction(tableFunction, evalMethod) {

  override def getResultType(arguments: util.List[AnyRef]): TypeInformation[_] = resultType

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      arguments: util.List[AnyRef]): RelDataType = {
    // we have determined the row type before, just convert it to RelDataType
    val (fieldNames, fieldIndexes, _) = UserDefinedFunctionUtils.getFieldInfo(resultType)
    UserDefinedFunctionUtils.buildRelDataType(typeFactory, resultType, fieldNames, fieldIndexes)
  }
}
