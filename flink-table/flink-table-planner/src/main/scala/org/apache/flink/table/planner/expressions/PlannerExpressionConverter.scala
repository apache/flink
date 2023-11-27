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

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions._
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.types.logical.LogicalTypeRoot.{CHAR, DECIMAL, SYMBOL}
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{hasLength, hasPrecision, hasScale}

import _root_.scala.collection.JavaConverters._

/** Visitor implementation for converting [[Expression]]s to [[PlannerExpression]]s. */
class PlannerExpressionConverter private extends ApiExpressionVisitor[PlannerExpression] {

  override def visit(call: CallExpression): PlannerExpression = {
    val definition = call.getFunctionDefinition
    translateCall(
      definition,
      call.getChildren.asScala,
      () =>
        definition match {
          case ROW | ARRAY | MAP => ApiResolvedExpression(call.getOutputDataType)
          case _ =>
            if (
              definition.getKind == FunctionKind.AGGREGATE ||
              definition.getKind == FunctionKind.TABLE_AGGREGATE
            ) {
              ApiResolvedAggregateCallExpression(call)
            } else {
              ApiResolvedExpression(call.getOutputDataType)
            }
        }
    )
  }

  override def visit(unresolvedCall: UnresolvedCallExpression): PlannerExpression = {
    val definition = unresolvedCall.getFunctionDefinition
    translateCall(
      definition,
      unresolvedCall.getChildren.asScala,
      () => throw new TableException(s"Unsupported function definition: $definition"))
  }

  private def translateCall(
      func: FunctionDefinition,
      children: Seq[Expression],
      unknownFunctionHandler: () => PlannerExpression): PlannerExpression = {

    val args = children.map(_.accept(this))

    func match {
      case sfd: ScalarFunctionDefinition =>
        val call = PlannerScalarFunctionCall(sfd.getScalarFunction, args)
        // it configures underlying state
        call.validateInput()
        call

      case tfd: TableFunctionDefinition =>
        PlannerTableFunctionCall(tfd.toString, tfd.getTableFunction, args, tfd.getResultType)

      case afd: AggregateFunctionDefinition =>
        AggFunctionCall(
          afd.getAggregateFunction,
          afd.getResultTypeInfo,
          afd.getAccumulatorTypeInfo,
          args)

      case tafd: TableAggregateFunctionDefinition =>
        AggFunctionCall(
          tafd.getTableAggregateFunction,
          tafd.getResultTypeInfo,
          tafd.getAccumulatorTypeInfo,
          args)

      case _: FunctionDefinition =>
        unknownFunctionHandler()
    }
  }

  override def visit(literal: ValueLiteralExpression): PlannerExpression = {
    if (literal.getOutputDataType.getLogicalType.is(SYMBOL)) {
      val plannerSymbol = getSymbol(literal.getValueAs(classOf[TableSymbol]).get())
      return SymbolPlannerExpression(plannerSymbol)
    }

    val typeInfo = getLiteralTypeInfo(literal)
    if (literal.isNull) {
      Null(typeInfo)
    } else {
      Literal(literal.getValueAs(typeInfo.getTypeClass).get(), typeInfo)
    }
  }

  /** This method makes the planner more lenient for new data types defined for literals. */
  private def getLiteralTypeInfo(literal: ValueLiteralExpression): TypeInformation[_] = {
    val logicalType = literal.getOutputDataType.getLogicalType

    if (logicalType.is(DECIMAL)) {
      if (literal.isNull) {
        return Types.BIG_DEC
      }
      val value = literal.getValueAs(classOf[java.math.BigDecimal]).get()
      if (hasPrecision(logicalType, value.precision()) && hasScale(logicalType, value.scale())) {
        return Types.BIG_DEC
      }
    } else if (logicalType.is(CHAR)) {
      if (literal.isNull) {
        return Types.STRING
      }
      val value = literal.getValueAs(classOf[java.lang.String]).get()
      if (hasLength(logicalType, value.length)) {
        return Types.STRING
      }
    }

    fromDataTypeToTypeInfo(literal.getOutputDataType)
  }

  private def getSymbol(symbol: TableSymbol): PlannerSymbol = symbol match {
    case TimeIntervalUnit.MILLENNIUM => PlannerTimeIntervalUnit.MILLENNIUM
    case TimeIntervalUnit.CENTURY => PlannerTimeIntervalUnit.CENTURY
    case TimeIntervalUnit.DECADE => PlannerTimeIntervalUnit.DECADE
    case TimeIntervalUnit.YEAR => PlannerTimeIntervalUnit.YEAR
    case TimeIntervalUnit.YEAR_TO_MONTH => PlannerTimeIntervalUnit.YEAR_TO_MONTH
    case TimeIntervalUnit.QUARTER => PlannerTimeIntervalUnit.QUARTER
    case TimeIntervalUnit.MONTH => PlannerTimeIntervalUnit.MONTH
    case TimeIntervalUnit.WEEK => PlannerTimeIntervalUnit.WEEK
    case TimeIntervalUnit.DAY => PlannerTimeIntervalUnit.DAY
    case TimeIntervalUnit.DAY_TO_HOUR => PlannerTimeIntervalUnit.DAY_TO_HOUR
    case TimeIntervalUnit.DAY_TO_MINUTE => PlannerTimeIntervalUnit.DAY_TO_MINUTE
    case TimeIntervalUnit.DAY_TO_SECOND => PlannerTimeIntervalUnit.DAY_TO_SECOND
    case TimeIntervalUnit.HOUR => PlannerTimeIntervalUnit.HOUR
    case TimeIntervalUnit.SECOND => PlannerTimeIntervalUnit.SECOND
    case TimeIntervalUnit.HOUR_TO_MINUTE => PlannerTimeIntervalUnit.HOUR_TO_MINUTE
    case TimeIntervalUnit.HOUR_TO_SECOND => PlannerTimeIntervalUnit.HOUR_TO_SECOND
    case TimeIntervalUnit.MINUTE => PlannerTimeIntervalUnit.MINUTE
    case TimeIntervalUnit.MINUTE_TO_SECOND => PlannerTimeIntervalUnit.MINUTE_TO_SECOND
    case TimeIntervalUnit.MILLISECOND => PlannerTimeIntervalUnit.MILLISECOND
    case TimeIntervalUnit.MICROSECOND => PlannerTimeIntervalUnit.MICROSECOND
    case TimeIntervalUnit.NANOSECOND => PlannerTimeIntervalUnit.NANOSECOND
    case TimeIntervalUnit.EPOCH => PlannerTimeIntervalUnit.EPOCH
    case TimePointUnit.YEAR => PlannerTimePointUnit.YEAR
    case TimePointUnit.MONTH => PlannerTimePointUnit.MONTH
    case TimePointUnit.DAY => PlannerTimePointUnit.DAY
    case TimePointUnit.HOUR => PlannerTimePointUnit.HOUR
    case TimePointUnit.MINUTE => PlannerTimePointUnit.MINUTE
    case TimePointUnit.SECOND => PlannerTimePointUnit.SECOND
    case TimePointUnit.QUARTER => PlannerTimePointUnit.QUARTER
    case TimePointUnit.WEEK => PlannerTimePointUnit.WEEK
    case TimePointUnit.MILLISECOND => PlannerTimePointUnit.MILLISECOND
    case TimePointUnit.MICROSECOND => PlannerTimePointUnit.MICROSECOND

    case _ =>
      throw new TableException("Unsupported symbol: " + symbol)
  }

  override def visit(fieldReference: FieldReferenceExpression): PlannerExpression = {
    PlannerResolvedFieldReference(
      fieldReference.getName,
      fromDataTypeToTypeInfo(fieldReference.getOutputDataType))
  }

  override def visit(fieldReference: UnresolvedReferenceExpression): PlannerExpression = {
    UnresolvedFieldReference(fieldReference.getName)
  }

  override def visit(typeLiteral: TypeLiteralExpression): PlannerExpression = {
    ApiResolvedExpression(typeLiteral.getOutputDataType)
  }

  override def visit(tableRef: TableReferenceExpression): PlannerExpression = {
    TableReference(
      tableRef.asInstanceOf[TableReferenceExpression].getName,
      tableRef.asInstanceOf[TableReferenceExpression].getQueryOperation
    )
  }

  override def visit(local: LocalReferenceExpression): PlannerExpression =
    PlannerLocalReference(local.getName, fromDataTypeToTypeInfo(local.getOutputDataType))

  override def visit(lookupCall: LookupCallExpression): PlannerExpression =
    throw new TableException("Unsupported function call: " + lookupCall)

  override def visit(sqlCall: SqlCallExpression): PlannerExpression =
    throw new TableException("Unsupported function call: " + sqlCall)

  override def visit(other: ResolvedExpression): PlannerExpression = visitNonApiExpression(other)

  override def visitNonApiExpression(other: Expression): PlannerExpression = {
    other match {
      // already converted planner expressions will pass this visitor without modification
      case plannerExpression: PlannerExpression => plannerExpression
      case expr: RexNodeExpression => RexPlannerExpression(expr.getRexNode)
      case _ =>
        throw new TableException("Unrecognized expression: " + other)
    }
  }
}

object PlannerExpressionConverter {
  val INSTANCE: PlannerExpressionConverter = new PlannerExpressionConverter
}
