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
import java.math.BigDecimal

import scala.collection.mutable
import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rex.RexWindowBound._
import org.apache.calcite.rex.{RexCall, RexFieldCollation, RexNode, RexWindowBound}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.OrdinalReturnTypeInference
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api._
import org.apache.flink.table.api.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.api.scala.{CURRENT_ROW, UNBOUNDED_ROW}
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkTypeFactory}
import org.apache.flink.table.functions.sql.internal.SqlThrowExceptionFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.logical.{LogicalExprVisitor, LogicalNode, LogicalTableFunctionCall}
import org.apache.flink.table.api.types._
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

import _root_.scala.collection.JavaConverters._

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

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Over call with unresolved alias for over window.
  *
  * @param agg The aggregation of the over call.
  * @param alias The alias of the referenced over window.
  */
case class UnresolvedOverCall(agg: Expression, alias: Expression) extends Expression {

  override private[flink] def validateInput() =
    ValidationFailure(s"Over window with alias $alias could not be resolved.")

  override private[flink] def resultType = agg.resultType

  override private[flink] def children = Seq()

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
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
    agg: Expression,
    partitionBy: Seq[Expression],
    orderBy: Seq[Expression],
    var preceding: Expression,
    var following: Expression,
    tableEnv: TableEnvironment) extends Expression {

  override def toString: String = s"$agg OVER (" +
    s"PARTITION BY (${partitionBy.mkString(", ")}) " +
    s"ORDER BY (${orderBy.mkString(", ")})" +
    s"PRECEDING $preceding " +
    s"FOLLOWING $following)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {

    val rexBuilder = relBuilder.getRexBuilder

    // assemble aggregation
    val operator: SqlAggFunction = agg.asInstanceOf[Aggregation].getSqlAggFunction()
    val aggResultType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromInternalType(agg.resultType, isNullable = true)

    // assemble exprs by agg children
    val aggExprs = agg.asInstanceOf[Aggregation].children.map(_.toRexNode(relBuilder)).asJava

    // assemble order by key
    def collation(
      node: RexNode, direction: RelFieldCollation.Direction,
      nullDirection: RelFieldCollation.NullDirection, kinds: mutable.Set[SqlKind]): RexNode = {
      node.getKind match {
        case SqlKind.DESCENDING =>
          kinds.add(node.getKind)
          collation(
            node.asInstanceOf[RexCall].getOperands.get(0),
            RelFieldCollation.Direction.DESCENDING, nullDirection, kinds)
        case SqlKind.NULLS_FIRST =>
          kinds.add(node.getKind)
          collation(
            node.asInstanceOf[RexCall].getOperands.get(0), direction,
            RelFieldCollation.NullDirection.FIRST, kinds)
        case SqlKind.NULLS_LAST =>
          kinds.add(node.getKind)
          collation(
            node.asInstanceOf[RexCall].getOperands.get(0), direction,
            RelFieldCollation.NullDirection.LAST, kinds)
        case _ =>
          if (nullDirection == null) {
            // Set the null direction if not specified.
            // Consistent with HIVE/SPARK/MYSQL/BLINK-RUNTIME.
            if (FlinkPlannerImpl.defaultNullCollation.last(
              direction.equals(RelFieldCollation.Direction.DESCENDING))) {
              kinds.add(SqlKind.NULLS_LAST)
            } else {
              kinds.add(SqlKind.NULLS_FIRST)
            }
          }
          node
      }
    }

    val orderKeysBuilder = new ImmutableList.Builder[RexFieldCollation]()
    for (orderExp <- orderBy) {
      val kinds: mutable.Set[SqlKind] = mutable.Set()
      val rexNode = collation(
        orderExp.toRexNode, RelFieldCollation.Direction.ASCENDING, null, kinds)
      val orderKey = new RexFieldCollation(rexNode, kinds.asJava)
      orderKeysBuilder.add(orderKey)
    }
    val orderKeys = orderKeysBuilder.build()

    // assemble partition by keys
    val partitionKeys = partitionBy.map(_.toRexNode).asJava

    // assemble bounds
    var isPhysical: Boolean = false
    var lowerBound, upperBound: RexWindowBound = null
    agg match {
      case _: RowNumber | _: Rank | _: DenseRank =>
        isPhysical = true
        lowerBound = createBound(relBuilder, UNBOUNDED_ROW, SqlKind.PRECEDING)
        upperBound = createBound(relBuilder, CURRENT_ROW, SqlKind.FOLLOWING)
      case _ =>
        isPhysical = preceding.resultType == DataTypes.INTERVAL_ROWS
        lowerBound = createBound(relBuilder, preceding, SqlKind.PRECEDING)
        upperBound = createBound(relBuilder, following, SqlKind.FOLLOWING)
    }

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
    bound: Expression,
    sqlKind: SqlKind): RexWindowBound = {

    bound match {
      case _: UnboundedRow | _: UnboundedRange =>
        val unbounded = if (sqlKind == SqlKind.PRECEDING) {
          SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO)
        } else {
          SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO)
        }
        create(unbounded, null)
      case _: CurrentRow | _: CurrentRange =>
        val currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO)
        create(currentRow, null)
      case b: Literal =>
        val DECIMAL_PRECISION_NEEDED_FOR_LONG = 19
        val returnType = relBuilder
          .getTypeFactory.asInstanceOf[FlinkTypeFactory]
          .createTypeFromInternalType(
            DecimalType.of(DECIMAL_PRECISION_NEEDED_FOR_LONG, 0),
            isNullable = true)

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
        b.value match {
          case v: Double => expressions.add(relBuilder.literal(
            BigDecimal.valueOf(v.asInstanceOf[Number].doubleValue())))
          case _ => expressions.add(relBuilder.literal(b.value))
        }

        val rexNode = relBuilder.getRexBuilder.makeCall(returnType, sqlOperator, expressions)

        create(node, rexNode)
    }
  }

  override private[flink] def children: Seq[Expression] =
    Seq(agg) ++ orderBy ++ partitionBy ++ Seq(preceding) ++ Seq(following)

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
      case r: ResolvedFieldReference
        if TypeConverters.createExternalTypeInfoFromDataType(r.resultType).isKeyType  =>
        ValidationSuccess
      case r: ResolvedFieldReference =>
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
      case Literal(_: Long, DataTypes.INTERVAL_ROWS) =>
        ValidationSuccess
      case Literal(_: Long, DataTypes.INTERVAL_RANGE) |
           Literal(_: Double, DataTypes.INTERVAL_RANGE) =>
        ValidationSuccess
      case Literal(v: Long, b) if isTimeInterval(b) && v >= 0 =>
        ValidationSuccess
      case Literal(_, b) if isTimeInterval(b) =>
        return ValidationFailure("Preceding time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Preceding must be a row interval or time interval literal.")
    }

    // check following is valid
    following match {
      case _: CurrentRow | _: CurrentRange | _: UnboundedRow | _: UnboundedRange =>
        ValidationSuccess
      case Literal(_: Long, DataTypes.INTERVAL_ROWS) =>
        ValidationSuccess
      case Literal(_: Long, DataTypes.INTERVAL_RANGE) |
           Literal(_: Double, DataTypes.INTERVAL_RANGE) =>
        ValidationSuccess
      case Literal(v: Long, b) if isTimeInterval(b) && v >= 0 =>
        ValidationSuccess
      case Literal(_, b) if isTimeInterval(b) =>
        return ValidationFailure("Following time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Following must be a row interval or time interval literal.")
    }

    // check that preceding and following are of same type
    (preceding, following) match {
      case (p: Expression, f: Expression) if p.resultType == f.resultType =>
        ValidationSuccess
      case (_: UnboundedRange, f: Expression) if f.resultType == DataTypes.INTERVAL_MILLIS =>
        ValidationSuccess
      case (_: CurrentRange, f: Expression) if f.resultType == DataTypes.INTERVAL_MILLIS =>
        ValidationSuccess
      case (p: Expression, _: UnboundedRange) if p.resultType == DataTypes.INTERVAL_MILLIS =>
        ValidationSuccess
      case (p: Expression, _: CurrentRange) if p.resultType == DataTypes.INTERVAL_MILLIS =>
        ValidationSuccess
      case _ =>
        return ValidationFailure("Preceding and following must be of same interval type.")
    }

    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      validateInputForStream()
    } else {
      ValidationSuccess
    }
  }

  private[flink] def validateInputForStream(): ValidationResult = {
    // check preceding/following range
    preceding match {
      case Literal(_, DataTypes.INTERVAL_RANGE) =>
        return ValidationFailure("Stream table API does not support value range.")
      case Literal(v: Long, DataTypes.INTERVAL_ROWS) if v < 0 =>
        return ValidationFailure("Stream table API does not support negative preceding.")
      case _ =>
        ValidationSuccess
    }
    following match {
      case _: UnboundedRow | _: UnboundedRange =>
        return ValidationFailure("Stream table API does not support unbounded following.")
      case Literal(_, DataTypes.INTERVAL_RANGE) =>
        return ValidationFailure("Stream table API does not support value range.")
      case Literal(v: Long, DataTypes.INTERVAL_ROWS) if v > 0 =>
        return ValidationFailure("Stream table API does not support positive following.")
      case _ =>
        ValidationSuccess
    }

    // check lead/lag offset
    agg match {
      case a: Lead =>
        if (a.getOffsetValue > 0) {
          return ValidationFailure("Stream table API does not support positive offset for lead.")
        }
      case a: Lag =>
        if (a.getOffsetValue < 0) {
          return ValidationFailure("Stream table API does not support negative offset for lag.")
        }
      case _ =>
        ValidationSuccess
    }

    ValidationSuccess
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

object ScalarFunctionCall {
  def apply(
      scalarFunction: ScalarFunction,
      parameters: Array[Expression]): ScalarFunctionCall =
    new ScalarFunctionCall(scalarFunction, parameters)
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

  override private[flink] def children: Seq[Expression] = parameters

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
    getResultTypeOfCTDFunction(
      scalarFunction,
      parameters.toArray,
      () => {extractTypeFromScalarFunc(
        scalarFunction, parameters.map(_.resultType).toArray)}).toInternalType

  override private[flink] def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    if (getEvalUserDefinedMethod(scalarFunction, signature).isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
        s"Actual: ${signatureToString(signature)} \n" +
        s"Expected: ${signaturesToString(scalarFunction, "eval")}")
    } else {
      ValidationSuccess
    }
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  *
  * Expression for calling a user-defined table function with actual parameters.
  *
  * @param functionName function name
  * @param tableFunction user-defined table function
  * @param parameters actual parameters of function
  * @param externalResultType type information of returned table
  */
case class TableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    externalResultType: DataType)
  extends Expression {

  private var aliases: Option[Seq[String]] = None

  override private[flink] def children: Seq[Expression] = parameters

  override private[flink] def resultType: InternalType = externalResultType.toInternalType

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
        throw new ValidationException(
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
      externalResultType,
      fieldNames,
      child)
  }

  override def toString =
    s"${tableFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ThrowException(msg: Expression, tp: InternalType) extends UnaryExpression {

  override private[flink] def validateInput(): ValidationResult = {
    if (child.resultType == DataTypes.STRING) {
      ValidationSuccess
    } else {
      ValidationFailure(s"ThrowException operator requires String input, " +
          s"but $child is of type ${child.resultType}")
    }
  }

  override def toString: String = s"ThrowException($msg)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(
      new SqlThrowExceptionFunction(
        tp,
        relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]),
      msg.toRexNode)
  }

  override private[flink] def child = msg

  override private[flink] def resultType = tp

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
