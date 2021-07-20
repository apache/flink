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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.MultisetTypeInfo
import org.apache.flink.table.expressions.CallExpression
import org.apache.flink.table.functions.ImperativeAggregateFunction
import org.apache.flink.table.planner.calcite.FlinkTypeSystem
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.typeutils.TypeInfoCheckUtils
import org.apache.flink.table.planner.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.{fromLogicalTypeToTypeInfo, fromTypeInfoToLogicalType}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

abstract sealed class Aggregation extends PlannerExpression {

  override def toString = s"Aggregate"

}

/**
 * Wrapper for call expressions resolved already in the API with the new type inference stack.
 * Separate from [[ApiResolvedExpression]] because others' expressions validation logic
 * check for the [[Aggregation]] trait.
 */
case class ApiResolvedAggregateCallExpression(
    resolvedCall: CallExpression)
  extends Aggregation {

  private[flink] val children = Nil

  override private[flink] def resultType: TypeInformation[_] = TypeConversions
    .fromDataTypeToLegacyInfo(
      resolvedCall
        .getOutputDataType)
}

case class DistinctAgg(child: PlannerExpression) extends Aggregation {

  def distinct: PlannerExpression = DistinctAgg(child)

  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult = {
    super.validateInput()
    child match {
      case agg: Aggregation =>
        child.validateInput()
      case _ =>
        ValidationFailure(s"Distinct modifier cannot be applied to $child! " +
          s"It can only be applied to an aggregation expression, for example, " +
          s"'a.count.distinct which is equivalent with COUNT(DISTINCT a).")
    }
  }

  override private[flink] def children = Seq(child)
}

/**
  * Returns a multiset aggregates.
  */
case class Collect(child: PlannerExpression) extends Aggregation  {

  override private[flink] def children: Seq[PlannerExpression] = Seq(child)

  override private[flink] def resultType: TypeInformation[_] =
    MultisetTypeInfo.getInfoFor(child.resultType)

  override def toString: String = s"collect($child)"
}

/**
  * Expression for calling a user-defined (table)aggregate function.
  */
case class AggFunctionCall(
    aggregateFunction: ImperativeAggregateFunction[_, _],
    resultTypeInfo: TypeInformation[_],
    accTypeInfo: TypeInformation[_],
    args: Seq[PlannerExpression])
  extends Aggregation {

  override private[flink] def children: Seq[PlannerExpression] = args

  override def resultType: TypeInformation[_] = resultTypeInfo

  override def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    val foundSignature = getAccumulateMethodSignature(
      aggregateFunction,
      signature.map(fromTypeInfoToLogicalType))
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${
                            signatureToString(signature.map(fromLegacyInfoToDataType))} \n" +
                          s"Expected: ${
                            getMethodSignatures(aggregateFunction, "accumulate")
                              .map(_.drop(1))
                              .map(signatureToString)
                              .sorted  // make sure order to verify error messages in tests
                              .mkString(", ")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = s"${aggregateFunction.getClass.getSimpleName}($args)"
}
