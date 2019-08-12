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

package org.apache.flink.table.planner.functions.utils

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.{getOperandType, _}
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter.getDefaultExternalClassForType
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos

import scala.collection.JavaConverters._

/**
  * Calcite wrapper for user-defined scalar functions.
  *
  * @param name           function name (used by SQL parser)
  * @param displayName    name to be displayed in operator name
  * @param scalarFunction scalar function to be called
  * @param typeFactory    type factory for converting Flink's between Calcite's types
  */
class ScalarSqlFunction(
    name: String,
    displayName: String,
    scalarFunction: ScalarFunction,
    typeFactory: FlinkTypeFactory,
    returnTypeInfer: Option[SqlReturnTypeInference] = None)
  extends SqlFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    returnTypeInfer.getOrElse(createReturnTypeInference(name, scalarFunction, typeFactory)),
    createOperandTypeInference(name, scalarFunction, typeFactory),
    createOperandTypeChecker(name, scalarFunction),
    null,
    SqlFunctionCategory.USER_DEFINED_FUNCTION) {

  def makeFunction(constants: Array[AnyRef], argTypes: Array[LogicalType]): ScalarFunction =
    scalarFunction

  override def isDeterministic: Boolean = scalarFunction.isDeterministic

  override def toString: String = displayName
}

object ScalarSqlFunction {

  private[flink] def createReturnTypeInference(
      name: String,
      scalarFunction: ScalarFunction,
      typeFactory: FlinkTypeFactory): SqlReturnTypeInference = {
    /**
      * Return type inference based on [[ScalarFunction]] given information.
      */
    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        val sqlTypes = opBinding.collectOperandTypes().asScala.toArray
        val parameters = getOperandType(opBinding).toArray

        val arguments = sqlTypes.indices.map(i =>
          if (opBinding.isOperandNull(i, false)) {
            null
          } else if (opBinding.isOperandLiteral(i, false)) {
            opBinding.getOperandLiteralValue(
              i, getDefaultExternalClassForType(parameters(i))).asInstanceOf[AnyRef]
          } else {
            null
          }
        ).toArray
        val resultType = getResultTypeOfScalarFunction(scalarFunction, arguments, parameters)
        typeFactory.createFieldTypeFromLogicalType(
          fromDataTypeToLogicalType(resultType))
      }
    }
  }

  private[flink] def createOperandTypeInference(
      name: String,
      scalarFunction: ScalarFunction,
      typeFactory: FlinkTypeFactory): SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {
        ScalarSqlFunction.inferOperandTypes(
          name, scalarFunction, typeFactory, callBinding, returnType, operandTypes)
      }
    }
  }

  def inferOperandTypes(
      name: String,
      func: ScalarFunction,
      typeFactory: FlinkTypeFactory,
      callBinding: SqlCallBinding,
      returnType: RelDataType,
      operandTypes: Array[RelDataType]): Unit = {
    val parameters = getOperandType(callBinding).toArray
    if (getEvalUserDefinedMethod(func, parameters).isEmpty) {
      throwValidationException(name, func, parameters)
    }
    func.getParameterTypes(getEvalMethodSignature(func, parameters))
        .map(fromTypeInfoToLogicalType)
        .map(typeFactory.createFieldTypeFromLogicalType)
        .zipWithIndex
        .foreach {
          case (t, i) => operandTypes(i) = t
        }
  }

  private[flink] def createOperandTypeChecker(
      name: String,
      scalarFunction: ScalarFunction): SqlOperandTypeChecker = {

    val methods = checkAndExtractMethods(scalarFunction, "eval")

    /**
      * Operand type checker based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(scalarFunction, "eval")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 254 // according to JVM spec 4.3.3
        var max = -1
        var isVarargs = false
        methods.foreach(
          m => {
            var len = m.getParameterTypes.length
            if (len > 0 && m.isVarArgs && m.getParameterTypes()(len - 1).isArray) {
              isVarargs = true
              len = len - 1
            }
            max = Math.max(len, max)
            min = Math.min(len, min)
          }
        )
        if (isVarargs) {
          // if eval method is varargs, set max to -1 to skip length check in Calcite
          max = -1
        }
        SqlOperandCountRanges.between(min, max)
      }

      override def checkOperandTypes(
          callBinding: SqlCallBinding,
          throwOnFailure: Boolean)
      : Boolean = {
        val operandTypeInfo = getOperandType(callBinding)

        val foundMethod = getEvalUserDefinedMethod(scalarFunction, operandTypeInfo)

        if (foundMethod.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                  s"Actual: ${signatureInternalToString(operandTypeInfo)} \n" +
                  s"Expected: ${signaturesToString(scalarFunction, "eval")}")
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
