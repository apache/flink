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
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

/**
  * Over call with unresolved alias for over window.
  *
  * @param agg The aggregation of the over call.
  * @param alias The alias of the referenced over window.
  */
case class UnresolvedOverCall(agg: PlannerExpression, alias: PlannerExpression)
  extends PlannerExpression {

  override private[flink] def validateInput() =
    ValidationFailure(s"Over window with alias $alias could not be resolved.")

  override private[flink] def resultType = agg.resultType

  override private[flink] def children = Seq()
}

/**
  * Over expression for Calcite over transform.
  *
  * @param agg            over-agg expression
  * @param partitionBy    The fields by which the over window is partitioned
  * @param orderBy        The field by which the over window is sorted
  * @param preceding      The lower bound of the window
  * @param following      The upper bound of the window
  */
case class OverCall(
    agg: PlannerExpression,
    partitionBy: Seq[PlannerExpression],
    orderBy: PlannerExpression,
    preceding: PlannerExpression,
    following: PlannerExpression) extends PlannerExpression {

  override def toString: String = s"$agg OVER (" +
    s"PARTITION BY (${partitionBy.mkString(", ")}) " +
    s"ORDER BY $orderBy " +
    s"PRECEDING $preceding " +
    s"FOLLOWING $following)"

  override private[flink] def children: Seq[PlannerExpression] =
    Seq(agg) ++ Seq(orderBy) ++ partitionBy ++ Seq(preceding) ++ Seq(following)

  override private[flink] def resultType = agg.resultType

  override private[flink] def validateInput(): ValidationResult = {

    // check that agg expression is aggregation
    agg match {
      case _: Aggregation =>
        ValidationSuccess
      case _ =>
        return ValidationFailure(s"OVER can only be applied on an aggregation.")
    }

    // check partitionBy expression keys are resolved field reference
    partitionBy.foreach {
      case r: PlannerResolvedFieldReference if r.resultType.isKeyType  =>
        ValidationSuccess
      case r: PlannerResolvedFieldReference =>
        return ValidationFailure(s"Invalid PartitionBy expression: $r. " +
          s"Expression must return key type.")
      case r =>
        return ValidationFailure(s"Invalid PartitionBy expression: $r. " +
          s"Expression must be a resolved field reference.")
    }

    // check preceding is valid
    preceding match {
      case _: CurrentRow | _: CurrentRange | _: UnboundedRow | _: UnboundedRange =>
        ValidationSuccess
      case Literal(v: Long, BasicTypeInfo.LONG_TYPE_INFO) if v > 0 =>
        ValidationSuccess
      case Literal(_, BasicTypeInfo.LONG_TYPE_INFO) =>
        return ValidationFailure("Preceding row interval must be larger than 0.")
      case Literal(v: Long, _: TimeIntervalTypeInfo[_]) if v >= 0 =>
        ValidationSuccess
      case Literal(_, _: TimeIntervalTypeInfo[_]) =>
        return ValidationFailure("Preceding time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Preceding must be a row interval or time interval literal.")
    }

    // check following is valid
    following match {
      case _: CurrentRow | _: CurrentRange | _: UnboundedRow | _: UnboundedRange =>
        ValidationSuccess
      case Literal(v: Long, BasicTypeInfo.LONG_TYPE_INFO) if v > 0 =>
        ValidationSuccess
      case Literal(_, BasicTypeInfo.LONG_TYPE_INFO) =>
        return ValidationFailure("Following row interval must be larger than 0.")
      case Literal(v: Long, _: TimeIntervalTypeInfo[_]) if v >= 0 =>
        ValidationSuccess
      case Literal(_, _: TimeIntervalTypeInfo[_]) =>
        return ValidationFailure("Following time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Following must be a row interval or time interval literal.")
    }

    // check that preceding and following are of same type
    (preceding, following) match {
      case (p: PlannerExpression, f: PlannerExpression) if p.resultType == f.resultType =>
        ValidationSuccess
      case _ =>
        return ValidationFailure("Preceding and following must be of same interval type.")
    }

    // check time field
    if (!PlannerExpressionUtils.isTimeAttribute(orderBy)) {
      return ValidationFailure("Ordering must be defined on a time attribute.")
    }

    ValidationSuccess
  }
}

/**
  * Expression for calling a user-defined scalar functions.
  *
  * @param scalarFunction scalar function to be called (might be overloaded)
  * @param parameters actual parameters that determine target evaluation method
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
    fromDataTypeToTypeInfo(getResultTypeOfScalarFunction(
      scalarFunction,
      Array(),
      signature))

  override private[flink] def validateInput(): ValidationResult = {
    signature = children.map(_.resultType).map(fromTypeInfoToLogicalType).toArray
    // look for a signature that matches the input types
    val foundSignature = getEvalMethodSignatureOption(scalarFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
        s"Actual: ${signatureToString(signature.map(fromLogicalTypeToDataType))} \n" +
        s"Expected: ${signaturesToString(scalarFunction, "eval")}")
    } else {
      ValidationSuccess
    }
  }
}

/**
  *
  * Expression for calling a user-defined table function with actual parameters.
  *
  * @param functionName function name
  * @param tableFunction user-defined table function
  * @param parameters actual parameters of function
  * @param resultType type information of returned table
  */
case class PlannerTableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[PlannerExpression],
    resultType: TypeInformation[_])
  extends PlannerExpression {

  override private[flink] def children: Seq[PlannerExpression] = parameters

  override def validateInput(): ValidationResult = {
    // check if not Scala object
    UserFunctionsTypeHelper.validateNotSingleton(tableFunction.getClass)
    // check if class could be instantiated
    UserFunctionsTypeHelper.validateInstantiation(tableFunction.getClass)
    // look for a signature that matches the input types
    val signature = parameters.map(_.resultType).map(fromLegacyInfoToDataType)
    val foundMethod = getUserDefinedMethod(
      tableFunction, "eval", signature)
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

case class ThrowException(msg: PlannerExpression, tp: TypeInformation[_]) extends UnaryExpression {

  override private[flink] def resultType: TypeInformation[_] = tp

  override private[flink] def child: PlannerExpression = msg

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == Types.STRING) {
      ValidationSuccess
    } else {
      ValidationFailure(s"ThrowException operator requires String input, " +
          s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"ThrowException($msg)"
}
