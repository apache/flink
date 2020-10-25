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

package org.apache.flink.table.functions.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction

/**
  * Calcite wrapper for user-defined table functions.
  */
class TableSqlFunction(
    name: String,
    displayName: String,
    tableFunction: TableFunction[_],
    rowTypeInfo: TypeInformation[_],
    typeFactory: FlinkTypeFactory,
    functionImpl: FlinkTableFunctionImpl[_])
  extends SqlUserDefinedTableFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.CURSOR,
    createEvalOperandTypeInference(name, tableFunction, typeFactory),
    createEvalOperandMetadata(name, tableFunction),
    functionImpl) {

  /**
    * Get the user-defined table function.
    */
  def getTableFunction: TableFunction[_] = tableFunction

  /**
    * Get the type information of the table returned by the table function.
    */
  def getRowTypeInfo: TypeInformation[_] = rowTypeInfo

  /**
    * Get additional mapping information if the returned table type is a POJO
    * (POJO types have no deterministic field order).
    */
  def getPojoFieldMapping: Array[Int] = functionImpl.fieldIndexes

  override def isDeterministic: Boolean = tableFunction.isDeterministic

  override def toString: String = displayName
}
