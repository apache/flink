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
import org.apache.flink.table.functions.{AggregateFunction, FunctionIdentifier, TableAggregateFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.utils.AggSqlFunction.{createOperandTypeChecker, createOperandTypeInference, createReturnTypeInference}
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlOperandTypeChecker.Consistency
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
import org.apache.calcite.util.Optionality

import java.util

/**
  * Calcite wrapper for user-defined aggregate functions. Currently, the aggregate function can be
  * an [[AggregateFunction]] or a [[TableAggregateFunction]]
  *
  * @param identifier function identifier to uniquely identify this function
  * @param displayName name to be displayed in operator name
  * @param aggregateFunction aggregate function to be called
  * @param externalResultType the type information of returned value
  * @param externalAccType the type information of the accumulator
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class AggSqlFunction(
    identifier: FunctionIdentifier,
    displayName: String,
    val aggregateFunction: UserDefinedAggregateFunction[_, _],
    val externalResultType: DataType,
    val externalAccType: DataType,
    typeFactory: FlinkTypeFactory,
    requiresOver: Boolean,
    returnTypeInfer: Option[SqlReturnTypeInference] = None)
  extends SqlUserDefinedAggFunction(
    new SqlIdentifier(identifier.toList, SqlParserPos.ZERO),
    returnTypeInfer.getOrElse(createReturnTypeInference(
      fromDataTypeToLogicalType(externalResultType), typeFactory)),
    createOperandTypeInference(displayName, aggregateFunction, typeFactory, externalAccType),
    createOperandTypeChecker(displayName, aggregateFunction, externalAccType),
    // Do not need to provide a calcite aggregateFunction here. Flink aggregateion function
    // will be generated when translating the calcite relnode to flink runtime execution plan
    null,
    false,
    requiresOver,
    Optionality.FORBIDDEN,
    typeFactory
  ) {

  /**
    * This is temporary solution for hive udf and should be removed once FLIP-65 is finished,
    * please pass the non-null input arguments.
    */
  def makeFunction(
      constants: Array[AnyRef],
      argTypes: Array[LogicalType]): UserDefinedAggregateFunction[_, _] = aggregateFunction

  override def isDeterministic: Boolean = aggregateFunction.isDeterministic

  override def toString: String = displayName

  override def getParamTypes: util.List[RelDataType] = null
}

object AggSqlFunction {

  def apply(
      identifier: FunctionIdentifier,
      displayName: String,
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      externalResultType: DataType,
      externalAccType: DataType,
      typeFactory: FlinkTypeFactory,
      requiresOver: Boolean): AggSqlFunction = {

    new AggSqlFunction(
      identifier,
      displayName,
      aggregateFunction,
      externalResultType,
      externalAccType,
      typeFactory,
      requiresOver)
  }

  private[flink] def createOperandTypeInference(
      name: String,
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      typeFactory: FlinkTypeFactory,
      externalAccType: DataType): SqlOperandTypeInference = {
    /**
      * Operand type inference based on [[AggregateFunction]] given information.
      */
    new SqlOperandTypeInference {
      override def inferOperandTypes(
          callBinding: SqlCallBinding,
          returnType: RelDataType,
          operandTypes: Array[RelDataType]): Unit = {

        val operandLogicalType = getOperandType(callBinding)
        val actualSignature = externalAccType.getLogicalType +: operandLogicalType

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandLogicalType)
            .getOrElse(
              throw new ValidationException(
                s"Given parameters of function '$name' do not match any signature. \n" +
                    s"Actual: ${signatureInternalToString(actualSignature)} \n" +
                    s"Expected: ${signaturesToString(aggregateFunction, "accumulate")}"))

        val inferredTypes = getParameterTypes(aggregateFunction, foundSignature.drop(1))
            .map(typeFactory.createFieldTypeFromLogicalType)

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
      resultType: LogicalType,
      typeFactory: FlinkTypeFactory): SqlReturnTypeInference = {

    new SqlReturnTypeInference {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        typeFactory.createFieldTypeFromLogicalType(resultType)
      }
    }
  }

  private[flink] def createOperandTypeChecker(
      name: String,
      aggregateFunction: UserDefinedAggregateFunction[_, _],
      externalAccType: DataType): SqlOperandTypeChecker = {

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
          throwOnFailure: Boolean): Boolean = {

        val operandLogicalType = getOperandType(callBinding)
        val actualSignature = externalAccType.getLogicalType +: operandLogicalType

        val foundSignature = getAccumulateMethodSignature(aggregateFunction, operandLogicalType)

        if (foundSignature.isEmpty) {
          if (throwOnFailure) {
            throw new ValidationException(
              s"Given parameters of function '$name' do not match any signature. \n" +
                  s"Actual: ${signatureInternalToString(actualSignature)} \n" +
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
