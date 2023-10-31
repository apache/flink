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
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions._
import org.apache.flink.table.functions.BuiltInFunctionDefinitions._
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions.THROW_EXCEPTION
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

    // special case: requires individual handling of child expressions
    func match {

      case WINDOW_START =>
        assert(children.size == 1)
        val windowReference = translateWindowReference(children.head)
        return WindowStart(windowReference)

      case WINDOW_END =>
        assert(children.size == 1)
        val windowReference = translateWindowReference(children.head)
        return WindowEnd(windowReference)

      case PROCTIME =>
        assert(children.size == 1)
        val windowReference = translateWindowReference(children.head)
        return ProctimeAttribute(windowReference)

      case ROWTIME =>
        assert(children.size == 1)
        val windowReference = translateWindowReference(children.head)
        return RowtimeAttribute(windowReference)

      case THROW_EXCEPTION =>
        assert(children.size == 2)
        return ThrowException(
          children.head.accept(this),
          fromDataTypeToTypeInfo(children(1).asInstanceOf[TypeLiteralExpression].getOutputDataType))

      case _ =>
    }

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

      case fd: FunctionDefinition =>
        fd match {

          case IN =>
            assert(args.size > 1)
            In(args.head, args.drop(1))

          case DISTINCT =>
            assert(args.size == 1)
            DistinctAgg(args.head)

          case COLLECT =>
            assert(args.size == 1)
            Collect(args.head)

          case AT =>
            assert(args.size == 2)
            ItemAt(args.head, args.last)

          case CARDINALITY =>
            assert(args.size == 1)
            Cardinality(args.head)

          case ARRAY_ELEMENT =>
            assert(args.size == 1)
            ArrayElement(args.head)

          case ORDER_ASC =>
            assert(args.size == 1)
            Asc(args.head)

          case ORDER_DESC =>
            assert(args.size == 1)
            Desc(args.head)

          case OVER =>
            assert(args.size >= 4)
            OverCall(
              args.head,
              args.slice(4, args.size),
              args(1),
              args(2),
              args(3)
            )

          case UNBOUNDED_RANGE =>
            assert(args.isEmpty)
            UnboundedRange()

          case UNBOUNDED_ROW =>
            assert(args.isEmpty)
            UnboundedRow()

          case CURRENT_RANGE =>
            assert(args.isEmpty)
            CurrentRange()

          case CURRENT_ROW =>
            assert(args.isEmpty)
            CurrentRow()

          case STREAM_RECORD_TIMESTAMP =>
            assert(args.isEmpty)
            StreamRecordTimestamp()

          case _ =>
            unknownFunctionHandler()
        }
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

  private def getValue[T](literal: PlannerExpression): T = {
    literal.asInstanceOf[Literal].value.asInstanceOf[T]
  }

  private def assert(condition: Boolean): Unit = {
    if (!condition) {
      throw new ValidationException("Invalid number of arguments for function.")
    }
  }

  private def translateWindowReference(reference: Expression): PlannerExpression = reference match {
    case expr: LocalReferenceExpression =>
      WindowReference(expr.getName, Some(fromDataTypeToTypeInfo(expr.getOutputDataType)))
    // just because how the datastream is converted to table
    case expr: UnresolvedReferenceExpression =>
      UnresolvedFieldReference(expr.getName)
    case _ =>
      throw new ValidationException(s"Expected LocalReferenceExpression. Got: $reference")
  }
}

object PlannerExpressionConverter {
  val INSTANCE: PlannerExpressionConverter = new PlannerExpressionConverter
}
