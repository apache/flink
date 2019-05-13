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

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions._
import org.apache.flink.table.expressions.{E => PlannerE, UUID => PlannerUUID}

import _root_.scala.collection.JavaConverters._

/**
  * Visitor implementation for converting [[Expression]]s to [[PlannerExpression]]s.
  */
class PlannerExpressionConverter private extends ApiExpressionVisitor[PlannerExpression] {

  override def visitCall(call: CallExpression): PlannerExpression = {
    val func = call.getFunctionDefinition

    // special case: casting requires a type literal
    if (func.equals(CAST)) {
      assert(call.getChildren.size() == 2)
      return Cast(
        call.getChildren.get(0).accept(this),
        call.getChildren.get(1).asInstanceOf[TypeLiteralExpression].getType)
    }

    val args = call.getChildren.asScala.map(_.accept(this))

    func match {
      case sfd: ScalarFunctionDefinition =>
        PlannerScalarFunctionCall(
          sfd.getScalarFunction,
          args)

      case tfd: TableFunctionDefinition =>
        PlannerTableFunctionCall(
          tfd.getName,
          tfd.getTableFunction,
          args,
          tfd.getResultType)

      case afd: AggregateFunctionDefinition =>
        AggFunctionCall(
          afd.getAggregateFunction,
          afd.getResultTypeInfo,
          afd.getAccumulatorTypeInfo,
          args)

      case fd: FunctionDefinition =>
        fd match {

          case AS =>
            assert(args.size >= 2)
            val name = getValue[String](args(1))
            val extraNames = args
              .drop(2)
              .map(e => getValue[String](e))
            Alias(args.head, name, extraNames)

          case FLATTEN =>
            assert(args.size == 1)
            Flattening(args.head)

          case GET =>
            assert(args.size == 2)
            GetCompositeField(args.head, getValue(args.last))

          case AND =>
            assert(args.size == 2)
            And(args.head, args.last)

          case OR =>
            assert(args.size == 2)
            Or(args.head, args.last)

          case NOT =>
            assert(args.size == 1)
            Not(args.head)

          case EQUALS =>
            assert(args.size == 2)
            EqualTo(args.head, args.last)

          case GREATER_THAN =>
            assert(args.size == 2)
            GreaterThan(args.head, args.last)

          case GREATER_THAN_OR_EQUAL =>
            assert(args.size == 2)
            GreaterThanOrEqual(args.head, args.last)

          case LESS_THAN =>
            assert(args.size == 2)
            LessThan(args.head, args.last)

          case LESS_THAN_OR_EQUAL =>
            assert(args.size == 2)
            LessThanOrEqual(args.head, args.last)

          case NOT_EQUALS =>
            assert(args.size == 2)
            NotEqualTo(args.head, args.last)

          case IN =>
            assert(args.size > 1)
            In(args.head, args.drop(1))

          case IS_NULL =>
            assert(args.size == 1)
            IsNull(args.head)

          case IS_NOT_NULL =>
            assert(args.size == 1)
            IsNotNull(args.head)

          case IS_TRUE =>
            assert(args.size == 1)
            IsTrue(args.head)

          case IS_FALSE =>
            assert(args.size == 1)
            IsFalse(args.head)

          case IS_NOT_TRUE =>
            assert(args.size == 1)
            IsNotTrue(args.head)

          case IS_NOT_FALSE =>
            assert(args.size == 1)
            IsNotFalse(args.head)

          case IF =>
            assert(args.size == 3)
            If(args.head, args(1), args.last)

          case BETWEEN =>
            assert(args.size == 3)
            Between(args.head, args(1), args.last)

          case NOT_BETWEEN =>
            assert(args.size == 3)
            NotBetween(args.head, args(1), args.last)

          case DISTINCT =>
            assert(args.size == 1)
            DistinctAgg(args.head)

          case AVG =>
            assert(args.size == 1)
            Avg(args.head)

          case COUNT =>
            assert(args.size == 1)
            Count(args.head)

          case MAX =>
            assert(args.size == 1)
            Max(args.head)

          case MIN =>
            assert(args.size == 1)
            Min(args.head)

          case SUM =>
            assert(args.size == 1)
            Sum(args.head)

          case SUM0 =>
            assert(args.size == 1)
            Sum0(args.head)

          case STDDEV_POP =>
            assert(args.size == 1)
            StddevPop(args.head)

          case STDDEV_SAMP =>
            assert(args.size == 1)
            StddevSamp(args.head)

          case VAR_POP =>
            assert(args.size == 1)
            VarPop(args.head)

          case VAR_SAMP =>
            assert(args.size == 1)
            VarSamp(args.head)

          case COLLECT =>
            assert(args.size == 1)
            Collect(args.head)

          case CHAR_LENGTH =>
            assert(args.size == 1)
            CharLength(args.head)

          case INIT_CAP =>
            assert(args.size == 1)
            InitCap(args.head)

          case LIKE =>
            assert(args.size == 2)
            Like(args.head, args.last)

          case LOWER =>
            assert(args.size == 1)
            Lower(args.head)

          case SIMILAR =>
            assert(args.size == 2)
            Similar(args.head, args.last)

          case SUBSTRING =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Substring(args.head, args.last)
            } else {
              Substring(args.head, args(1), args.last)
            }

          case REPLACE =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Replace(args.head, args.last)
            } else {
              Replace(args.head, args(1), args.last)
            }

          case TRIM =>
            assert(args.size == 4)
            val removeLeading = getValue[Boolean](args.head)
            val removeTrailing = getValue[Boolean](args(1))

            val trimMode = if (removeLeading && removeTrailing) {
              PlannerTrimMode.BOTH
            } else if (removeLeading) {
              PlannerTrimMode.LEADING
            } else if (removeTrailing) {
              PlannerTrimMode.TRAILING
            } else {
              throw new TableException("Unsupported trim mode.")
            }
            Trim(trimMode, args(2), args(3))

          case UPPER =>
            assert(args.size == 1)
            Upper(args.head)

          case POSITION =>
            assert(args.size == 2)
            Position(args.head, args.last)

          case OVERLAY =>
            assert(args.size == 3 || args.size == 4)
            if (args.size == 3) {
              new Overlay(args.head, args(1), args.last)
            } else {
              Overlay(
                args.head,
                args(1),
                args(2),
                args.last)
            }

          case CONCAT =>
            Concat(args)

          case CONCAT_WS =>
            assert(args.nonEmpty)
            ConcatWs(args.head, args.tail)

          case LPAD =>
            assert(args.size == 3)
            Lpad(args.head, args(1), args.last)

          case RPAD =>
            assert(args.size == 3)
            Rpad(args.head, args(1), args.last)

          case REGEXP_EXTRACT =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              RegexpExtract(args.head, args.last)
            } else {
              RegexpExtract(args.head, args(1), args.last)
            }

          case FROM_BASE64 =>
            assert(args.size == 1)
            FromBase64(args.head)

          case TO_BASE64 =>
            assert(args.size == 1)
            ToBase64(args.head)

          case BuiltInFunctionDefinitions.UUID =>
            assert(args.isEmpty)
            PlannerUUID()

          case LTRIM =>
            assert(args.size == 1)
            LTrim(args.head)

          case RTRIM =>
            assert(args.size == 1)
            RTrim(args.head)

          case REPEAT =>
            assert(args.size == 2)
            Repeat(args.head, args.last)

          case REGEXP_REPLACE =>
            assert(args.size == 3)
            RegexpReplace(args.head, args(1), args.last)

          case PLUS =>
            assert(args.size == 2)
            Plus(args.head, args.last)

          case MINUS =>
            assert(args.size == 2)
            Minus(args.head, args.last)

          case DIVIDE =>
            assert(args.size == 2)
            Div(args.head, args.last)

          case TIMES =>
            assert(args.size == 2)
            Mul(args.head, args.last)

          case ABS =>
            assert(args.size == 1)
            Abs(args.head)

          case CEIL =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              Ceil(args.head)
            } else {
              TemporalCeil(args.head, args.last)
            }

          case EXP =>
            assert(args.size == 1)
            Exp(args.head)

          case FLOOR =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              Floor(args.head)
            } else {
              TemporalFloor(args.head, args.last)
            }

          case LOG10 =>
            assert(args.size == 1)
            Log10(args.head)

          case LOG2 =>
            assert(args.size == 1)
            Log2(args.head)

          case LN =>
            assert(args.size == 1)
            Ln(args.head)

          case LOG =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              Log(args.head)
            } else {
              Log(args.head, args.last)
            }

          case POWER =>
            assert(args.size == 2)
            Power(args.head, args.last)

          case MOD =>
            assert(args.size == 2)
            Mod(args.head, args.last)

          case SQRT =>
            assert(args.size == 1)
            Sqrt(args.head)

          case MINUS_PREFIX =>
            assert(args.size == 1)
            UnaryMinus(args.head)

          case SIN =>
            assert(args.size == 1)
            Sin(args.head)

          case COS =>
            assert(args.size == 1)
            Cos(args.head)

          case SINH =>
            assert(args.size == 1)
            Sinh(args.head)

          case TAN =>
            assert(args.size == 1)
            Tan(args.head)

          case TANH =>
            assert(args.size == 1)
            Tanh(args.head)

          case COT =>
            assert(args.size == 1)
            Cot(args.head)

          case ASIN =>
            assert(args.size == 1)
            Asin(args.head)

          case ACOS =>
            assert(args.size == 1)
            Acos(args.head)

          case ATAN =>
            assert(args.size == 1)
            Atan(args.head)

          case ATAN2 =>
            assert(args.size == 2)
            Atan2(args.head, args.last)

          case COSH =>
            assert(args.size == 1)
            Cosh(args.head)

          case DEGREES =>
            assert(args.size == 1)
            Degrees(args.head)

          case RADIANS =>
            assert(args.size == 1)
            Radians(args.head)

          case SIGN =>
            assert(args.size == 1)
            Sign(args.head)

          case ROUND =>
            assert(args.size == 2)
            Round(args.head, args.last)

          case PI =>
            assert(args.isEmpty)
            Pi()

          case BuiltInFunctionDefinitions.E =>
            assert(args.isEmpty)
            PlannerE()

          case RAND =>
            assert(args.isEmpty || args.size == 1)
            if (args.isEmpty) {
              new Rand()
            } else {
              Rand(args.head)
            }

          case RAND_INTEGER =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new RandInteger(args.head)
            } else {
              RandInteger(args.head, args.last)
            }

          case BIN =>
            assert(args.size == 1)
            Bin(args.head)

          case HEX =>
            assert(args.size == 1)
            Hex(args.head)

          case TRUNCATE =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new Truncate(args.head)
            } else {
              Truncate(args.head, args.last)
            }

          case EXTRACT =>
            assert(args.size == 2)
            Extract(args.head, args.last)

          case CURRENT_DATE =>
            assert(args.isEmpty)
            CurrentDate()

          case CURRENT_TIME =>
            assert(args.isEmpty)
            CurrentTime()

          case CURRENT_TIMESTAMP =>
            assert(args.isEmpty)
            CurrentTimestamp()

          case LOCAL_TIME =>
            assert(args.isEmpty)
            LocalTime()

          case LOCAL_TIMESTAMP =>
            assert(args.isEmpty)
            LocalTimestamp()

          case TEMPORAL_OVERLAPS =>
            assert(args.size == 4)
            TemporalOverlaps(
              args.head,
              args(1),
              args(2),
              args.last)

          case DATE_TIME_PLUS =>
            assert(args.size == 2)
            Plus(args.head, args.last)

          case DATE_FORMAT =>
            assert(args.size == 2)
            DateFormat(args.head, args.last)

          case TIMESTAMP_DIFF =>
            assert(args.size == 3)
            TimestampDiff(args.head, args(1), args.last)

          case AT =>
            assert(args.size == 2)
            ItemAt(args.head, args.last)

          case CARDINALITY =>
            assert(args.size == 1)
            Cardinality(args.head)

          case ARRAY =>
            ArrayConstructor(args)

          case ARRAY_ELEMENT =>
            assert(args.size == 1)
            ArrayElement(args.head)

          case MAP =>
            MapConstructor(args)

          case ROW =>
            RowConstructor(args)

          case WINDOW_START =>
            assert(args.size == 1)
            WindowStart(args.head)

          case WINDOW_END =>
            assert(args.size == 1)
            WindowEnd(args.head)

          case ORDER_ASC =>
            assert(args.size == 1)
            Asc(args.head)

          case ORDER_DESC =>
            assert(args.size == 1)
            Desc(args.head)

          case MD5 =>
            assert(args.size == 1)
            Md5(args.head)

          case SHA1 =>
            assert(args.size == 1)
            Sha1(args.head)

          case SHA224 =>
            assert(args.size == 1)
            Sha224(args.head)

          case SHA256 =>
            assert(args.size == 1)
            Sha256(args.head)

          case SHA384 =>
            assert(args.size == 1)
            Sha384(args.head)

          case SHA512 =>
            assert(args.size == 1)
            Sha512(args.head)

          case SHA2 =>
            assert(args.size == 2)
            Sha2(args.head, args.last)

          case PROCTIME =>
            assert(args.size == 1)
            ProctimeAttribute(args.head)

          case ROWTIME =>
            assert(args.size == 1)
            RowtimeAttribute(args.head)

          case OVER =>
            assert(args.size == 2)
            UnresolvedOverCall(
              args.head,
              args.last
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

          case _ =>
            throw new TableException(s"Unsupported function definition: $fd")
        }
    }
  }

  override def visitSymbol(symbolExpression: SymbolExpression): PlannerExpression = {
    val plannerSymbol = symbolExpression.getSymbol match {
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
        throw new TableException("Unsupported symbol: " + symbolExpression.getSymbol)
    }

    SymbolPlannerExpression(plannerSymbol)
  }

  override def visitValueLiteral(literal: ValueLiteralExpression): PlannerExpression = {
    if (literal.getValue == null) {
      Null(literal.getType)
    } else {
      Literal(literal.getValue, literal.getType)
    }
  }

  override def visitFieldReference(fieldReference: FieldReferenceExpression): PlannerExpression = {
    ResolvedFieldReference(
      fieldReference.getName,
      fieldReference.getResultType)
  }

  override def visitUnresolvedReference(fieldReference: UnresolvedReferenceExpression)
    : PlannerExpression = {
    UnresolvedFieldReference(fieldReference.getName)
  }

  override def visitTypeLiteral(typeLiteral: TypeLiteralExpression): PlannerExpression = {
    throw new TableException("Unsupported type literal expression: " + typeLiteral)
  }

  override def visitTableReference(tableRef: TableReferenceExpression): PlannerExpression = {
    TableReference(
      tableRef.asInstanceOf[TableReferenceExpression].getName,
      tableRef.asInstanceOf[TableReferenceExpression].getTable
    )
  }

  override def visitLookupCall(lookupCall: LookupCallExpression): PlannerExpression =
    throw new TableException("Unsupported function call: " + lookupCall)

  override def visitNonApiExpression(other: Expression): PlannerExpression = {
    other match {
      // already converted planner expressions will pass this visitor without modification
      case plannerExpression: PlannerExpression => plannerExpression

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
}

object PlannerExpressionConverter {
  val INSTANCE: PlannerExpressionConverter = new PlannerExpressionConverter
}
