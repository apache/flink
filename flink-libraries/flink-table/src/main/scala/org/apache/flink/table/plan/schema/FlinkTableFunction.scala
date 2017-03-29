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

import java.lang.reflect.{Method, Type}
import java.util

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.TableFunction
import org.apache.calcite.schema.impl.ReflectiveFunctionBase
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.{TableFunction => FlinkUDTF}

/**
  * This is heavily inspired by Calcite's [[org.apache.calcite.schema.impl.TableFunctionImpl]].
  * We need it in order to create a [[org.apache.flink.table.functions.utils.TableSqlFunction]].
  * The main difference is that we override the [[getResultType()]] and [[getElementType()]].
  *
  * @param tableFunction The Table Function instance
  * @param evalMethod The eval() method of the [[tableFunction]]
  */
abstract class FlinkTableFunction(
    tableFunction: FlinkUDTF[_],
    evalMethod: Method)
  extends ReflectiveFunctionBase(evalMethod)
  with TableFunction {

  override def getElementType(arguments: util.List[AnyRef]): Type = classOf[Array[Object]]

  /**
    * Returns the TypeInformation for internal usage, i.e. code generation.
    */
  def getResultType(arguments: util.List[AnyRef]): TypeInformation[_]

  /**
    * Returns the record type of the table function to integrate with Calcite.
    */
  def getRowType(typeFactory: RelDataTypeFactory, arguments: util.List[AnyRef]): RelDataType

}
