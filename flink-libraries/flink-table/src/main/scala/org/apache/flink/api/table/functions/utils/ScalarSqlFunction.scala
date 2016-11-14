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

package org.apache.flink.api.table.functions.utils

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.functions.ScalarFunction
import org.apache.flink.api.table.functions.utils.ScalarSqlFunction.{createOperandTypeChecker, createOperandTypeInference, createReturnTypeInference}
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils.{getResultType, getSignature, signatureToString, signaturesToString}
import org.apache.flink.api.table.{FlinkTypeFactory, ValidationException}

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
    createOperandTypeInference(scalarFunction, typeFactory),
    createOperandTypeChecker(name, scalarFunction),
    null,
    SqlFunctionCategory.USER_DEFINED_FUNCTION) {

  def getScalarFunction = scalarFunction

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
        val foundSignature = getSignature(scalarFunction, parameters)
        if (foundSignature.isEmpty) {
          throw new ValidationException(
            s"Given parameters of function '$name' do not match any signature. \n" +
              s"Actual: ${signatureToString(parameters)} \n" +
              s"Expected: ${signaturesToString(scalarFunction)}")
        }
        val resultType = getResultType(scalarFunction, foundSignature.get)
        typeFactory.createTypeFromTypeInfo(resultType)
      }
    }
  }

  private[flink] def createOperandTypeInference(
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

        val foundSignature = getSignature(scalarFunction, operandTypeInfo)
          .getOrElse(throw new ValidationException(s"Operand types of could not be inferred."))

        val inferredTypes = scalarFunction
          .getParameterTypes(foundSignature)
          .map(typeFactory.createTypeFromTypeInfo)

        inferredTypes.zipWithIndex.foreach {
          case (inferredType, i) =>
            operandTypes(i) = inferredType
        }
      }
    }
  }

  private[flink] def createOperandTypeChecker(
      name: String,
      scalarFunction: ScalarFunction)
    : SqlOperandTypeChecker = {
    /**
      * Operand type checker based on [[ScalarFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(scalarFunction)}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        val signatureLengths = scalarFunction.getSignatures.map(_.length)
        SqlOperandCountRanges.between(signatureLengths.min, signatureLengths.max)
      }

      override def checkOperandTypes(
          callBinding: SqlCallBinding,
          throwOnFailure: Boolean)
        : Boolean = {
        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val foundSignature = getSignature(scalarFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
                s"Expected: ${signaturesToString(scalarFunction)}")
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

  private[flink] def getOperandTypeInfo(callBinding: SqlCallBinding): Seq[TypeInformation[_]] = {
    val operandTypes = for (i <- 0 until callBinding.getOperandCount)
      yield callBinding.getOperandType(i)
    operandTypes.map { operandType =>
      if (operandType.getSqlTypeName == SqlTypeName.NULL) {
        null
      } else {
        FlinkTypeFactory.toTypeInfo(operandType)
      }
    }
  }

}
