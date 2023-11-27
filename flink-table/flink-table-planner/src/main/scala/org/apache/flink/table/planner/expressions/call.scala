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
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.table.expressions.CallExpression
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

/**
 * Wrapper for expressions that have been resolved already in the API with the new type inference
 * stack.
 */
case class ApiResolvedExpression(resolvedDataType: DataType) extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] =
    TypeConversions.fromDataTypeToLegacyInfo(resolvedDataType)
}

/**
 * Over call with unresolved alias for over window.
 *
 * @param agg
 *   The aggregation of the over call.
 * @param alias
 *   The alias of the referenced over window.
 */
case class UnresolvedOverCall(agg: PlannerExpression, alias: PlannerExpression)
  extends PlannerExpression {

  override private[flink] def validateInput() =
    ValidationFailure(s"Over window with alias $alias could not be resolved.")

  override private[flink] def resultType = agg.resultType

  override private[flink] def children = Seq()
}

/**
 * Expression for calling a user-defined scalar functions.
 *
 * @param scalarFunction
 *   scalar function to be called (might be overloaded)
 * @param parameters
 *   actual parameters that determine target evaluation method
 */
case class PlannerScalarFunctionCall(
    scalarFunction: ScalarFunction,
    parameters: Seq[PlannerExpression])
  extends PlannerExpression {

  private var signature: Array[LogicalType] = _

  override private[flink] def children: Seq[PlannerExpression] = parameters

  override def toString =
    s"${scalarFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override private[flink] def resultType =
    fromDataTypeToTypeInfo(getResultTypeOfScalarFunction(scalarFunction, signature))

  override private[flink] def validateInput(): ValidationResult = {
    signature = children.map(_.resultType).map(fromTypeInfoToLogicalType).toArray
    // look for a signature that matches the input types
    val foundSignature = getEvalMethodSignatureOption(scalarFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(
        s"Given parameters do not match any signature. \n" +
          s"Actual: ${signatureToString(signature.map(fromLogicalTypeToDataType))} \n" +
          s"Expected: ${signaturesToString(scalarFunction, "eval")}")
    } else {
      ValidationSuccess
    }
  }
}

/**
 * Expression for calling a user-defined table function with actual parameters.
 *
 * @param functionName
 *   function name
 * @param tableFunction
 *   user-defined table function
 * @param parameters
 *   actual parameters of function
 * @param resultType
 *   type information of returned table
 */
case class PlannerTableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[PlannerExpression],
    resultType: TypeInformation[_])
  extends PlannerExpression {

  override private[flink] def children: Seq[PlannerExpression] = parameters

  override def validateInput(): ValidationResult = {
    // look for a signature that matches the input types
    val signature = parameters.map(_.resultType).map(fromLegacyInfoToDataType)
    val foundMethod = getUserDefinedMethod(tableFunction, "eval", signature)
    if (foundMethod.isEmpty) {
      ValidationFailure(
        s"Given parameters of function '$functionName' do not match any signature. \n" +
          s"Actual: ${signatureToString(signature)} \n" +
          s"Expected: ${signaturesToString(tableFunction, "eval")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString =
    s"${tableFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"
}
