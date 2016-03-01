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

package org.apache.flink.api.table.codegen.calls

import java.lang.reflect.Method

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.mutable

/**
  * Global registry of built-in advanced SQL scalar functions.
  */
object ScalarFunctions {

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[TypeInformation[_]]), CallGenerator] =
    mutable.Map()

  // ----------------------------------------------------------------------------------------------
  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunctionTrim(
    TRIM,
    Seq(INT_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    STRING_TYPE_INFO)

  // ----------------------------------------------------------------------------------------------

  def getCallGenerator(
      call: SqlOperator,
      operandTypes: Seq[TypeInformation[_]])
    : Option[CallGenerator] = {
    sqlFunctions.get((call, operandTypes))
  }

  // ----------------------------------------------------------------------------------------------

  private def addSqlFunctionMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      returnType: TypeInformation[_],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGenerator(returnType, method)
  }

  private def addSqlFunctionTrim(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      returnType: TypeInformation[_])
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new TrimCallGenerator(returnType)
  }

}
