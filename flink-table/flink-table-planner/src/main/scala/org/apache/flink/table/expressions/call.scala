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
package org.apache.flink.table.expressions

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.rex.RexWindowBound._
import org.apache.calcite.rex.{RexFieldCollation, RexNode, RexWindowBound}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.OrdinalReturnTypeInference
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import _root_.scala.collection.JavaConverters._

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

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {

    val rexBuilder = relBuilder.getRexBuilder

    // assemble aggregation
    val operator: SqlAggFunction = agg.asInstanceOf[Aggregation].getSqlAggFunction()
    val aggResultType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(agg.resultType, isNullable = true)

    // assemble exprs by agg children
    val aggExprs = agg.asInstanceOf[Aggregation].children.map(_.toRexNode(relBuilder)).asJava

    // assemble order by key
    val orderKey = new RexFieldCollation(orderBy.toRexNode, Set[SqlKind]().asJava)
    val orderKeys = ImmutableList.of(orderKey)

    // assemble partition by keys
    val partitionKeys = partitionBy.map(_.toRexNode).asJava

    // assemble bounds
    val isPhysical: Boolean = preceding.resultType == BasicTypeInfo.LONG_TYPE_INFO

    val lowerBound = createBound(relBuilder, preceding, SqlKind.PRECEDING)
    val upperBound = createBound(relBuilder, following, SqlKind.FOLLOWING)

    // build RexOver
    rexBuilder.makeOver(
      aggResultType,
      operator,
      aggExprs,
      partitionKeys,
      orderKeys,
      lowerBound,
      upperBound,
      isPhysical,
      true,
      false,
      false)
  }

  private def createBound(
    relBuilder: RelBuilder,
    bound: PlannerExpression,
    sqlKind: SqlKind): RexWindowBound = {

    bound match {
      case _: UnboundedRow | _: UnboundedRange =>
        val unbounded = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO)
        create(unbounded, null)
      case _: CurrentRow | _: CurrentRange =>
        val currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO)
        create(currentRow, null)
      case b: Literal =>
        val returnType = relBuilder
          .getTypeFactory.asInstanceOf[FlinkTypeFactory]
          .createTypeFromTypeInfo(Types.DECIMAL, isNullable = true)

        val sqlOperator = new SqlPostfixOperator(
          sqlKind.name,
          sqlKind,
          2,
          new OrdinalReturnTypeInference(0),
          null,
          null)

        val operands: Array[SqlNode] = new Array[SqlNode](1)
        operands(0) = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)

        val node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO)

        val expressions: util.ArrayList[RexNode] = new util.ArrayList[RexNode]()
        expressions.add(relBuilder.literal(b.value))

        val rexNode = relBuilder.getRexBuilder.makeCall(returnType, sqlOperator, expressions)

        create(node, rexNode)
    }
  }

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

  private var foundSignature: Option[Array[Class[_]]] = None

  override private[flink] def children: Seq[PlannerExpression] = parameters

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    relBuilder.call(
      createScalarSqlFunction(
        scalarFunction.functionIdentifier,
        scalarFunction.toString,
        scalarFunction,
        typeFactory),
      parameters.map(_.toRexNode): _*)
  }

  override def toString =
    s"${scalarFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override private[flink] def resultType =
    getResultTypeOfScalarFunction(
      scalarFunction,
      foundSignature.get)

  override private[flink] def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    foundSignature = getEvalMethodSignature(scalarFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
        s"Actual: ${signatureToString(signature)} \n" +
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
    val signature = parameters.map(_.resultType)
    val foundMethod = getUserDefinedMethod(tableFunction, "eval", typeInfoToClass(signature))
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
