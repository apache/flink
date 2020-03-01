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

import java.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.util.Optionality
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.{AggregateFunction, FunctionRequirement, TableAggregateFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.functions.utils.AggSqlFunction.{createOperandTypeChecker, createOperandTypeInference, createReturnTypeInference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._

/**
  * Calcite wrapper for user-defined aggregate functions. Currently, the aggregate function can be
  * an [[AggregateFunction]] or a [[TableAggregateFunction]]
  *
  * @param name function name (used by SQL parser)
  * @param displayName name to be displayed in operator name
  * @param aggregateFunction user defined aggregate function to be called
  * @param returnType the type information of returned value
  * @param accType the type information of the accumulator
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class AggSqlFunction(
    name: String,
    displayName: String,
    aggregateFunction: UserDefinedAggregateFunction[_, _],
    val returnType: TypeInformation[_],
    val accType: TypeInformation[_],
    typeFactory: FlinkTypeFactory,
    requiresOver: Boolean)
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(returnType, typeFactory),
    createOperandTypeInference(aggregateFunction, typeFactory, accType),
    createOperandTypeChecker(aggregateFunction, accType),
    // Do not need to provide a calcite aggregateFunction here. Flink aggregation function
    // will be generated when translating the calcite relnode to flink runtime execution plan
    null,
    false,
    requiresOver,
    Optionality.FORBIDDEN,
    typeFactory
  ) {

  def getFunction: UserDefinedAggregateFunction[_, _] = aggregateFunction

  override def isDeterministic: Boolean = aggregateFunction.isDeterministic

  override def toString: String = displayName

  override def getParamTypes: util.List[RelDataType] = null
}

object AggSqlFunction {

  def apply(
      name: String,
      displayName: String,
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      returnType: TypeInformation[_],
      accType: TypeInformation[_],
      typeFactory: FlinkTypeFactory): AggSqlFunction = {

    val requiresOver = aggregateFunction match {
      case a: AggregateFunction[_, _] =>
        a.getRequirements.contains(FunctionRequirement.OVER_WINDOW_ONLY)
      case _ => false
    }

    new AggSqlFunction(
      name,
      displayName,
      aggregateFunction,
      returnType,
      accType,
      typeFactory,
      requiresOver)
  }

  private[flink] def createOperandTypeInference(
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      typeFactory: FlinkTypeFactory,
      accType: TypeInformation[_])
  : SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[AggregateFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val actualSignature = accType +: operandTypeInfo

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandTypeInfo)
          .getOrElse(
            throw new ValidationException(
              s"Given parameters of function do not match any signature. \n" +
                s"Actual: ${signatureToString(actualSignature)} \n" +
                s"Expected: ${signaturesToString(aggregateFunction, "accumulate")}"))

        val inferredTypes = getParameterTypes(aggregateFunction, foundSignature.drop(1))
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

  private[flink] def createReturnTypeInference(
      resultType: TypeInformation[_],
      typeFactory: FlinkTypeFactory)
  : SqlReturnTypeInference = {

    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        typeFactory.createTypeFromTypeInfo(resultType, isNullable = true)
      }
    }
  }

  private[flink] def createOperandTypeChecker(
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      accType: TypeInformation[_])
    : SqlOperandTypeChecker = {

    val methods = checkAndExtractMethods(aggregateFunction, "accumulate")

    /**
      * Operand type checker based on [[AggregateFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(aggregateFunction, "accumulate")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 253
        var max = -1
        var isVarargs = false
        methods.foreach( m => {
          // do not count accumulator as input
          val inputParams = m.getParameterTypes.drop(1)
          var len = inputParams.length
          if (len > 0 && m.isVarArgs && inputParams(len - 1).isArray) {
            isVarargs = true
            len = len - 1
          }
          max = Math.max(len, max)
          min = Math.min(len, min)
        })
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
        val operandTypeInfo = getOperandTypeInfo(callBinding)

        val actualSignature = accType +: operandTypeInfo

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function do not match any signature. \n" +
                s"Actual: ${signatureToString(actualSignature)} \n" +
                s"Expected: ${signaturesToString(aggregateFunction, "accumulate")}")
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
