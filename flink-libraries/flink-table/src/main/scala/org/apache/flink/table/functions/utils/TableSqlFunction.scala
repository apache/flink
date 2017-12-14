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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.utils.TableSqlFunction._

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
    ReturnTypes.CURSOR,
    createOperandTypeInference(name, tableFunction, typeFactory),
    createOperandTypeChecker(name, tableFunction),
    null,
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

object TableSqlFunction {

  private[flink] def createOperandTypeInference(
    name: String,
    tableFunction: TableFunction[_],
    typeFactory: FlinkTypeFactory)
  : SqlOperandTypeInference = {

    /**
      * Operand type inference based on [[TableFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getEvalMethodSignature(tableFunction, operandTypeInfo)
          .getOrElse(throw new ValidationException(
            s"Given parameters of function '$name' do not match any signature. \n" +
              s"Actual: ${signatureToString(operandTypeInfo)} \n" +
              s"Expected: ${signaturesToString(tableFunction, "eval")}"))

        val inferredTypes = tableFunction
          .getParameterTypes(foundSignature)
          .map(typeFactory.createTypeFromTypeInfo(_, isNullable = true))

        for (i <- operandTypes.indices) {
          if (i < inferredTypes.length - 1) {
            operandTypes(i) = inferredTypes(i)
          } else if (null != inferredTypes.last.getComponentType) {
            // last argument is a collection, the array type
            operandTypes(i) = inferredTypes.last.getComponentType
          } else {
            operandTypes(i) = inferredTypes.last
          }
        }
      }
    }
  }

  private[flink] def createOperandTypeChecker(
    name: String,
    tableFunction: TableFunction[_])
  : SqlOperandTypeChecker = {

    val signatures = getMethodSignatures(tableFunction, "eval")

    /**
      * Operand type checker based on [[TableFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(tableFunction, "eval")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 255
        var max = -1
        signatures.foreach( sig => {
          var len = sig.length
          if (len > 0 && sig(sig.length - 1).isArray) {
            max = 254  // according to JVM spec 4.3.3
            len = sig.length - 1
          }
          max = Math.max(len, max)
          min = Math.min(len, min)
        })
        SqlOperandCountRanges.between(min, max)
      }

      override def checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean)
      : Boolean = {
        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getEvalMethodSignature(tableFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(tableFunction, "eval")}")
          } else {
            false
          }
        } else {
          true
        }
      }

      override def isOptional(i: Int): Boolean = false

      override def getConsistency: Consistency = Consistency.NONE

    }
  }
}
