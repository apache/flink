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
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.AggSqlFunction.{createOperandTypeChecker, createOperandTypeInference, createReturnTypeInference}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._

/**
  * Calcite wrapper for user-defined aggregate functions.
  *
  * @param name function name (used by SQL parser)
  * @param aggregateFunction aggregate function to be called
  * @param returnType the type information of returned value
  * @param accType the type information of the accumulator
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class AggSqlFunction(
    name: String,
    aggregateFunction: AggregateFunction[_, _],
    val returnType: TypeInformation[_],
    val accType: TypeInformation[_],
    typeFactory: FlinkTypeFactory,
    requiresOver: Boolean)
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    createReturnTypeInference(returnType, typeFactory),
    createOperandTypeInference(aggregateFunction, typeFactory),
    createOperandTypeChecker(aggregateFunction),
    // Do not need to provide a calcite aggregateFunction here. Flink aggregateion function
    // will be generated when translating the calcite relnode to flink runtime execution plan
    null,
    false,
    requiresOver
  ) {

  def getFunction: AggregateFunction[_, _] = aggregateFunction

  override def isDeterministic: Boolean = aggregateFunction.isDeterministic
}

object AggSqlFunction {

  def apply(
      name: String,
      aggregateFunction: AggregateFunction[_, _],
      returnType: TypeInformation[_],
      accType: TypeInformation[_],
      typeFactory: FlinkTypeFactory,
      requiresOver: Boolean): AggSqlFunction = {

    new AggSqlFunction(name, aggregateFunction, returnType, accType, typeFactory, requiresOver)
  }

  private[flink] def createOperandTypeInference(
      aggregateFunction: AggregateFunction[_, _],
      typeFactory: FlinkTypeFactory)
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

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandTypeInfo)
          .getOrElse(
            throw new ValidationException(
              s"Given parameters of function do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
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

  private[flink] def createOperandTypeChecker(aggregateFunction: AggregateFunction[_, _])
  : SqlOperandTypeChecker = {

    val signatures = getMethodSignatures(aggregateFunction, "accumulate")

    /**
      * Operand type checker based on [[AggregateFunction]] given information.
      */
    new SqlOperandTypeChecker {
      override def getAllowedSignatures(op: SqlOperator, opName: String): String = {
        s"$opName[${signaturesToString(aggregateFunction, "accumulate")}]"
      }

      override def getOperandCountRange: SqlOperandCountRange = {
        var min = 255
        var max = -1
        signatures.foreach(
          sig => {
            //do not count accumulator as input
            val inputSig = sig.drop(1)
            var len = inputSig.length
            if (len > 0 && inputSig.last.isArray) {
              max = 253 // according to JVM spec 4.3.3
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

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandTypeInfo)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function do not match any signature. \n" +
                s"Actual: ${signatureToString(operandTypeInfo)} \n" +
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
