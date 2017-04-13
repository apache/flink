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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{OverWindow, UnresolvedException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalTableFunctionCall}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.typeutils.RowIntervalTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala.{UNBOUNDED_RANGE, UNBOUNDED_ROW, CURRENT_RANGE, CURRENT_ROW}
import org.apache.flink.table.functions.{EventTimeExtractor, ProcTimeExtractor}
/**
  * General expression for unresolved function calls. The function can be a built-in
  * scalar function or a user-defined scalar function.
  */
case class Call(functionName: String, args: Seq[Expression]) extends Expression {

  override private[flink] def children: Seq[Expression] = args

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    throw UnresolvedException(s"trying to convert UnresolvedFunction $functionName to RexNode")
  }

  override def toString = s"\\$functionName(${args.mkString(", ")})"

  override private[flink] def resultType =
    throw UnresolvedException(s"calling resultType on UnresolvedFunction $functionName")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved function call: $functionName")
}

/**
  * Over expression for calcite over transform.
  *
  * @param agg             over-agg expression
  * @param overWindowAlias over window alias
  * @param overWindow      over window
  */
case class OverCall(
    agg: Aggregation,
    overWindowAlias: Expression,
    var overWindow: OverWindow = null) extends Expression {

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {

    val rexBuilder = relBuilder.getRexBuilder

    val operator: SqlAggFunction = agg.getSqlAggFunction()

    val relDataType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(agg.resultType)

    val aggExprs: util.ArrayList[RexNode] = new util.ArrayList[RexNode]()
    val aggChildName = agg.child.asInstanceOf[ResolvedFieldReference].name

    aggExprs.add(relBuilder.field(aggChildName))

    val orderKeys: ImmutableList.Builder[RexFieldCollation] =
      new ImmutableList.Builder[RexFieldCollation]()

    val sets: util.HashSet[SqlKind] = new util.HashSet[SqlKind]()
    val orderName = overWindow.orderBy.asInstanceOf[UnresolvedFieldReference].name

    val rexNode =
      if (orderName.equalsIgnoreCase("rowtime")) {
        // for stream event-time
        relBuilder.call(EventTimeExtractor)
      }
      else if (orderName.equalsIgnoreCase("proctime")) {
        // for stream proc-time
        relBuilder.call(ProcTimeExtractor)
      } else {
        // for batch event-time
        relBuilder.field(orderName)
      }

    orderKeys.add(new RexFieldCollation(rexNode, sets))

    val partitionKeys: util.ArrayList[RexNode] = new util.ArrayList[RexNode]()
    overWindow.partitionBy.foreach {
      x =>
        val partitionKey = relBuilder.field(x.asInstanceOf[UnresolvedFieldReference].name)
        if (!FlinkTypeFactory.toTypeInfo(partitionKey.getType).isKeyType) {
          throw ValidationException(
            s"expression $partitionKey cannot be used as a partition key expression " +
            "because it's not a valid key type which must be hashable and comparable")
        }
        partitionKeys.add(partitionKey)
    }

    val preceding = overWindow.preceding.asInstanceOf[Literal]
    val following = overWindow.following.asInstanceOf[Literal]

    val isPhysical: Boolean = preceding.resultType.isInstanceOf[RowIntervalTypeInfo]

    val lowerBound = createBound(relBuilder, preceding, SqlKind.PRECEDING)
    val upperBound = createBound(relBuilder, following, SqlKind.FOLLOWING)

    rexBuilder.makeOver(
      relDataType,
      operator,
      aggExprs,
      partitionKeys,
      orderKeys.build,
      lowerBound,
      upperBound,
      isPhysical,
      true,
      false)
  }

  private def createBound(
    relBuilder: RelBuilder,
    bound: Literal,
    sqlKind: SqlKind): RexWindowBound = {

    if (bound == UNBOUNDED_RANGE || bound == UNBOUNDED_ROW) {
      val unbounded = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO)
      create(unbounded, null)
    } else if (bound == CURRENT_RANGE || bound == CURRENT_ROW) {
      val currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO)
      create(currentRow, null)
    } else {
      val returnType = relBuilder
        .getTypeFactory.asInstanceOf[FlinkTypeFactory]
        .createTypeFromTypeInfo(Types.DECIMAL)

      val sqlOperator = new SqlPostfixOperator(
        sqlKind.name,
        sqlKind,
        2,
        new OrdinalReturnTypeInference(0),
        null,
        null)

      val operands: Array[SqlNode] = new Array[SqlNode](1)
      operands(0) = (SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO))

      val node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO)

      val expressions: util.ArrayList[RexNode] = new util.ArrayList[RexNode]()
      expressions.add(relBuilder.literal(bound.value))

      val rexNode = relBuilder.getRexBuilder.makeCall(returnType, sqlOperator, expressions)

      create(node, rexNode)
    }
  }

  override private[flink] def children: Seq[Expression] = Seq(agg)

  override def toString = s"${this.getClass.getCanonicalName}(${overWindowAlias.toString})"

  override private[flink] def resultType = agg.resultType

  override private[flink] def validateInput(): ValidationResult = {
    var validationResult: ValidationResult = ValidationSuccess
    val orderName = overWindow.orderBy.asInstanceOf[UnresolvedFieldReference].name
    if (!orderName.equalsIgnoreCase("rowtime")
      && !orderName.equalsIgnoreCase("proctime")) {
      validationResult = ValidationFailure(
        s"OrderBy expression must be ['rowtime] or ['proctime], but got ['${orderName}]")
    }

    if (!overWindow.preceding.asInstanceOf[Literal].resultType.getClass
         .equals(overWindow.following.asInstanceOf[Literal].resultType.getClass)) {
      validationResult = ValidationFailure(
        "Proceeding and the following must be based on same intervals type (time or row-count).")
    }

    val precedingValue = overWindow.preceding.asInstanceOf[Literal].value.asInstanceOf[Long]
    if (precedingValue <= 0) {
      validationResult = ValidationFailure(
        s"Proceeding value is [${precedingValue}], It should be bigger than 0.")
    }

    val followingValue = overWindow.following.asInstanceOf[Literal].value.asInstanceOf[Long]
    if (followingValue < -1) {
      validationResult = ValidationFailure(
        s"Following value is [${followingValue}], It should be bigger than -1.")
    }
    validationResult
  }
}

/**
  * Expression for calling a user-defined scalar functions.
  *
  * @param scalarFunction scalar function to be called (might be overloaded)
  * @param parameters actual parameters that determine target evaluation method
  */
case class ScalarFunctionCall(
    scalarFunction: ScalarFunction,
    parameters: Seq[Expression])
  extends Expression {

  private var foundSignature: Option[Array[Class[_]]] = None

  override private[flink] def children: Seq[Expression] = parameters

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    relBuilder.call(
      createScalarSqlFunction(
        scalarFunction.functionIdentifier,
        scalarFunction,
        typeFactory),
      parameters.map(_.toRexNode): _*)
  }

  override def toString =
    s"${scalarFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override private[flink] def resultType = getResultType(scalarFunction, foundSignature.get)

  override private[flink] def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    foundSignature = getSignature(scalarFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
        s"Actual: ${signatureToString(signature)} \n" +
        s"Expected: ${signaturesToString(scalarFunction)}")
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
case class TableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    resultType: TypeInformation[_])
  extends Expression {

  private var aliases: Option[Seq[String]] = None

  override private[flink] def children: Seq[Expression] = parameters

  /**
    * Assigns an alias for this table function's returned fields that the following operator
    * can refer to.
    *
    * @param aliasList alias for this table function's returned fields
    * @return this table function call
    */
  private[flink] def as(aliasList: Option[Seq[String]]): TableFunctionCall = {
    this.aliases = aliasList
    this
  }

  /**
    * Converts an API class to a logical node for planning.
    */
  private[flink] def toLogicalTableFunctionCall(child: LogicalNode): LogicalTableFunctionCall = {
    val originNames = getFieldInfo(resultType)._1

    // determine the final field names
    val fieldNames = if (aliases.isDefined) {
      val aliasList = aliases.get
      if (aliasList.length != originNames.length) {
        throw ValidationException(
          s"List of column aliases must have same degree as table; " +
            s"the returned table of function '$functionName' has ${originNames.length} " +
            s"columns (${originNames.mkString(",")}), " +
            s"whereas alias list has ${aliasList.length} columns")
      } else {
        aliasList.toArray
      }
    } else {
      originNames
    }

    LogicalTableFunctionCall(
      functionName,
      tableFunction,
      parameters,
      resultType,
      fieldNames,
      child)
  }

  override def toString =
    s"${tableFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"
}
