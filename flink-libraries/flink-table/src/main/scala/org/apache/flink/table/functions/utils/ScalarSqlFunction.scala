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
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._

import scala.collection.JavaConverters._

/**
  * Calcite wrapper for user-defined scalar functions.
  *
  * @param name function name (used by SQL parser)
  * @param scalarFunction scalar function to be called
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class ScalarSqlFunction(
    name: String,
    scalarFunction: ScalarFunction,
    typeFactory: FlinkTypeFactory)
  extends SqlFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(name, scalarFunction, typeFactory),
    createOperandTypeInference(name, scalarFunction, typeFactory),
    createOperandTypeChecker(name, scalarFunction),
    null,
    SqlFunctionCategory.USER_DEFINED_FUNCTION) {

  def getScalarFunction = scalarFunction

  override def isDeterministic: Boolean = scalarFunction.isDeterministic
}

object ScalarSqlFunction {

  private[flink] def createReturnTypeInference(
      name: String,
      scalarFunction: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlReturnTypeInference = {
    /**
      * Return type inference based on [[ScalarFunction]] given information.
      */
    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        val parameters = opBinding
          .collectOperandTypes()
          .asScala
          .map { operandType =>
            if (operandType.getSqlTypeName == SqlTypeName.NULL) {
              null
            } else {
              FlinkTypeFactory.toTypeInfo(operandType)
            }
          }
        val foundSignature = getEvalMethodSignature(scalarFunction, parameters)
        if (foundSignature.isEmpty) {
          throw new ValidationException(
            s"Given parameters of function '$name' do not match any signature. \n" +
              s"Actual: ${signatureToString(parameters)} \n" +
              s"Expected: ${signaturesToString(scalarFunction, "eval")}")
        }
        val resultType = getResultTypeOfScalarFunction(scalarFunction, foundSignature.get)
        typeFactory.createTypeFromTypeInfo(resultType, isNullable = true)
      }
    }
  }

  private[flink] def createOperandTypeInference(
      name: String,
      scalarFunction: ScalarFunction,
      typeFactory: FlinkTypeFactory)
    : SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getEvalMethodSignature(scalarFunction, operandTypeInfo)
          .getOrElse(
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(scalarFunction, "eval")}"))

        val inferredTypes = scalarFunction
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
      scalarFunction: ScalarFunction)
    : SqlOperandTypeChecker = {

    val signatures = getMethodSignatures(scalarFunction, "eval")

    /**
      * Operand type checker based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(scalarFunction, "eval")}]"
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

        val foundSignature = getEvalMethodSignature(scalarFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
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
