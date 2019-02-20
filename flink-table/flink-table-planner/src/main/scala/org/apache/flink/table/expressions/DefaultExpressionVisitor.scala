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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.FunctionDefinitions._
import org.apache.flink.table.expressions.{UUID => PlannerUUID, E => PlannerE, Call => PlannerCall, Literal => PlannerLiteral, TableFunctionCall => PlannerTableFunctionCall, TableReference => PlannerTableReference}
import _root_.scala.collection.JavaConverters._

/**
  * Default implementation of expression visitor.
  */
class DefaultExpressionVisitor private extends ExpressionVisitor[PlannerExpression] {

  override def visitCall(call: CallExpression): PlannerExpression = {

    val func = call.getFunctionDefinition
    val args = call.getChildren.asScala

    func match {
      case e: ScalarFunctionDefinition =>
        ScalarFunctionCall(e.getScalarFunction, args.map(_.accept(this)))

      case e: TableFunctionDefinition =>
        PlannerTableFunctionCall(
          e.getName, e.getTableFunction, args.map(_.accept(this)), e.getResultType)

      case aggFuncDef: AggregateFunctionDefinition =>
        AggFunctionCall(
          aggFuncDef.getAggregateFunction,
          aggFuncDef.getResultTypeInfo,
          aggFuncDef.getAccumulatorTypeInfo,
          args.map(_.accept(this)))

      case e: FunctionDefinition =>
        e match {
          case CAST =>
            assert(args.size == 2)
            Cast(args.head.accept(this), args.last.asInstanceOf[TypeLiteralExpression].getType)

          case AS =>
            assert(args.size >= 2)
            val child = args(0)
            val name = args(1).asInstanceOf[ValueLiteralExpression].getValue.asInstanceOf[String]
            val extraNames =
              args.drop(1).drop(1)
                .map(e => e.asInstanceOf[ValueLiteralExpression].getValue.asInstanceOf[String])
            val plannerExpression = child.accept(this)
            plannerExpression match {
              case tfc: PlannerTableFunctionCall =>
                tfc.setAliases(name +: extraNames)
              case _ =>
                Alias(plannerExpression, name, extraNames)
            }

          case FLATTEN =>
            assert(args.size == 1)
            Flattening(args.head.accept(this))

          case GET_COMPOSITE_FIELD =>
            assert(args.size == 2)
            GetCompositeField(args.head.accept(this),
              args.last.asInstanceOf[ValueLiteralExpression].getValue)

          case AND =>
            assert(args.size == 2)
            And(args.head.accept(this), args.last.accept(this))

          case OR =>
            assert(args.size == 2)
            Or(args.head.accept(this), args.last.accept(this))

          case NOT =>
            assert(args.size == 1)
            Not(args.head.accept(this))

          case EQUALS =>
            assert(args.size == 2)
            EqualTo(args.head.accept(this), args.last.accept(this))

          case GREATER_THAN =>
            assert(args.size == 2)
            GreaterThan(args.head.accept(this), args.last.accept(this))

          case GREATER_THAN_OR_EQUAL =>
            assert(args.size == 2)
            GreaterThanOrEqual(args.head.accept(this), args.last.accept(this))

          case LESS_THAN =>
            assert(args.size == 2)
            LessThan(args.head.accept(this), args.last.accept(this))

          case LESS_THAN_OR_EQUAL =>
            assert(args.size == 2)
            LessThanOrEqual(args.head.accept(this), args.last.accept(this))

          case NOT_EQUALS =>
            assert(args.size == 2)
            NotEqualTo(args.head.accept(this), args.last.accept(this))

          case IN =>
            In(args.head.accept(this), args.slice(1, args.size).map(_.accept(this)))

          case IS_NULL =>
            assert(args.size == 1)
            IsNull(args.head.accept(this))

          case IS_NOT_NULL =>
            assert(args.size == 1)
            IsNotNull(args.head.accept(this))

          case IS_TRUE =>
            assert(args.size == 1)
            IsTrue(args.head.accept(this))

          case IS_FALSE =>
            assert(args.size == 1)
            IsFalse(args.head.accept(this))

          case IS_NOT_TRUE =>
            assert(args.size == 1)
            IsNotTrue(args.head.accept(this))

          case IS_NOT_FALSE =>
            assert(args.size == 1)
            IsNotFalse(args.head.accept(this))

          case IF =>
            assert(args.size == 3)
            If(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case BETWEEN =>
            assert(args.size == 3)
            Between(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case NOT_BETWEEN =>
            assert(args.size == 3)
            NotBetween(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case DISTINCT =>
            assert(args.size == 1)
            DistinctAgg(args.head.accept(this))

          case AVG =>
            assert(args.size == 1)
            Avg(args.head.accept(this))

          case COUNT =>
            assert(args.size == 1)
            Count(args.head.accept(this))

          case MAX =>
            assert(args.size == 1)
            Max(args.head.accept(this))

          case MIN =>
            assert(args.size == 1)
            Min(args.head.accept(this))

          case SUM =>
            assert(args.size == 1)
            Sum(args.head.accept(this))

          case SUM0 =>
            assert(args.size == 1)
            Sum0(args.head.accept(this))

          case STDDEV_POP =>
            assert(args.size == 1)
            StddevPop(args.head.accept(this))

          case STDDEV_SAMP =>
            assert(args.size == 1)
            StddevSamp(args.head.accept(this))

          case VAR_POP =>
            assert(args.size == 1)
            VarPop(args.head.accept(this))

          case VAR_SAMP =>
            assert(args.size == 1)
            VarSamp(args.head.accept(this))

          case COLLECT =>
            assert(args.size == 1)
            Collect(args.head.accept(this))

          case CHAR_LENGTH =>
            assert(args.size == 1)
            CharLength(args.head.accept(this))

          case INIT_CAP =>
            assert(args.size == 1)
            InitCap(args.head.accept(this))

          case LIKE =>
            assert(args.size == 2)
            Like(args.head.accept(this), args.last.accept(this))

          case LOWER =>
            assert(args.size == 1)
            Lower(args.head.accept(this))

          case SIMILAR =>
            assert(args.size == 2)
            Similar(args.head.accept(this), args.last.accept(this))

          case SUBSTRING =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Substring(args.head.accept(this), args.last.accept(this))
            } else {
              Substring(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case REPLACE =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Replace(args.head.accept(this), args.last.accept(this))
            } else {
              Replace(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case TRIM =>
            assert(args.size == 3)
            Trim(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case UPPER =>
            assert(args.size == 1)
            Upper(args.head.accept(this))

          case POSITION =>
            assert(args.size == 2)
            Position(args.head.accept(this), args.last.accept(this))

          case OVERLAY =>
            assert(args.size == 3 || args.size == 4)
            if (args.size == 3) {
              new Overlay(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            } else {
              Overlay(
                args.head.accept(this),
                args(1).accept(this),
                args(2).accept(this),
                args.last.accept(this))
            }

          case CONCAT =>
            Concat(args.map(_.accept(this)))

          case CONCAT_WS =>
            assert(args.nonEmpty)
            ConcatWs(args.head.accept(this), args.tail.map(_.accept(this)))

          case LPAD =>
            assert(args.size == 3)
            Lpad(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case RPAD =>
            assert(args.size == 3)
            Rpad(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case REGEXP_EXTRACT =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              RegexpExtract(args.head.accept(this), args.last.accept(this))
            } else {
              RegexpExtract(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case FROM_BASE64 =>
            assert(args.size == 1)
            FromBase64(args.head.accept(this))

          case TO_BASE64 =>
            assert(args.size == 1)
            ToBase64(args.head.accept(this))

          case FunctionDefinitions.UUID =>
            assert(args.isEmpty)
            PlannerUUID()

          case LTRIM =>
            assert(args.size == 1)
            LTrim(args.head.accept(this))

          case RTRIM =>
            assert(args.size == 1)
            RTrim(args.head.accept(this))

          case REPEAT =>
            assert(args.size == 2)
            Repeat(args.head.accept(this), args.last.accept(this))

          case REGEXP_REPLACE =>
            assert(args.size == 3)
            RegexpReplace(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case PLUS =>
            assert(args.size == 2)
            Plus(args.head.accept(this), args.last.accept(this))

          case MINUS =>
            assert(args.size == 2)
            Minus(args.head.accept(this), args.last.accept(this))

          case DIVIDE =>
            assert(args.size == 2)
            Div(args.head.accept(this), args.last.accept(this))

          case TIMES =>
            assert(args.size == 2)
            Mul(args.head.accept(this), args.last.accept(this))

          case ABS =>
            assert(args.size == 1)
            Abs(args.head.accept(this))

          case CEIL =>
            assert(args.size == 1)
            Ceil(args.head.accept(this))

          case EXP =>
            assert(args.size == 1)
            Exp(args.head.accept(this))

          case FLOOR =>
            assert(args.size == 1)
            Floor(args.head.accept(this))

          case LOG10 =>
            assert(args.size == 1)
            Log10(args.head.accept(this))

          case LOG2 =>
            assert(args.size == 1)
            Log2(args.head.accept(this))

          case LN =>
            assert(args.size == 1)
            Ln(args.head.accept(this))

          case LOG =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              Log(args.head.accept(this))
            } else {
              Log(args.head.accept(this), args.last.accept(this))
            }

          case POWER =>
            assert(args.size == 2)
            Power(args.head.accept(this), args.last.accept(this))

          case MOD =>
            assert(args.size == 2)
            Mod(args.head.accept(this), args.last.accept(this))

          case SQRT =>
            assert(args.size == 1)
            Sqrt(args.head.accept(this))

          case MINUS_PREFIX =>
            assert(args.size == 1)
            UnaryMinus(args.head.accept(this))

          case SIN =>
            assert(args.size == 1)
            Sin(args.head.accept(this))

          case COS =>
            assert(args.size == 1)
            Cos(args.head.accept(this))

          case SINH =>
            assert(args.size == 1)
            Sinh(args.head.accept(this))

          case TAN =>
            assert(args.size == 1)
            Tan(args.head.accept(this))

          case TANH =>
            assert(args.size == 1)
            Tanh(args.head.accept(this))

          case COT =>
            assert(args.size == 1)
            Cot(args.head.accept(this))

          case ASIN =>
            assert(args.size == 1)
            Asin(args.head.accept(this))

          case ACOS =>
            assert(args.size == 1)
            Acos(args.head.accept(this))

          case ATAN =>
            assert(args.size == 1)
            Atan(args.head.accept(this))

          case ATAN2 =>
            assert(args.size == 2)
            Atan2(args.head.accept(this), args.last.accept(this))

          case COSH =>
            assert(args.size == 1)
            Cosh(args.head.accept(this))

          case DEGREES =>
            assert(args.size == 1)
            Degrees(args.head.accept(this))

          case RADIANS =>
            assert(args.size == 1)
            Radians(args.head.accept(this))

          case SIGN =>
            assert(args.size == 1)
            Sign(args.head.accept(this))

          case ROUND =>
            assert(args.size == 2)
            Round(args.head.accept(this), args.last.accept(this))

          case PI =>
            assert(args.isEmpty)
            Pi()

          case FunctionDefinitions.E =>
            assert(args.isEmpty)
            PlannerE()

          case RAND =>
            assert(args.isEmpty || args.size == 1)
            if (args.isEmpty) {
              new Rand()
            } else {
              Rand(args.head.accept(this))
            }

          case RAND_INTEGER =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new RandInteger(args.head.accept(this))
            } else {
              RandInteger(args.head.accept(this), args.last.accept(this))
            }

          case BIN =>
            assert(args.size == 1)
            Bin(args.head.accept(this))

          case HEX =>
            assert(args.size == 1)
            Hex(args.head.accept(this))

          case TRUNCATE =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new Truncate(args.head.accept(this))
            } else {
              Truncate(args.head.accept(this), args.last.accept(this))
            }

          case EXTRACT =>
            assert(args.size == 2)
            Extract(args.head.accept(this), args.last.accept(this))

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

          case QUARTER =>
            assert(args.size == 1)
            Quarter(args.head.accept(this))

          case TEMPORAL_OVERLAPS =>
            assert(args.size == 4)
            TemporalOverlaps(
              args.head.accept(this),
              args(1).accept(this),
              args(2).accept(this),
              args.last.accept(this))

          case DATE_TIME_PLUS =>
            assert(args.size == 2)
            Plus(args.head.accept(this), args.last.accept(this))

          case DATE_FORMAT =>
            assert(args.size == 2)
            DateFormat(args.head.accept(this), args.last.accept(this))

          case TIMESTAMP_DIFF =>
            assert(args.size == 3)
            TimestampDiff(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case TEMPORAL_FLOOR =>
            assert(args.size == 2)
            TemporalFloor(args.head.accept(this), args.last.accept(this))

          case TEMPORAL_CEIL =>
            assert(args.size == 2)
            TemporalCeil(args.head.accept(this), args.last.accept(this))

          case AT =>
            assert(args.size == 2)
            ItemAt(args.head.accept(this), args.last.accept(this))

          case CARDINALITY =>
            assert(args.size == 1)
            Cardinality(args.head.accept(this))

          case ARRAY =>
            ArrayConstructor(args.map(_.accept(this)))

          case ARRAY_ELEMENT =>
            assert(args.size == 1)
            ArrayElement(args.head.accept(this))

          case MAP =>
            MapConstructor(args.map(_.accept(this)))

          case ROW =>
            RowConstructor(args.map(_.accept(this)))

          case WIN_START =>
            assert(args.size == 1)
            WindowStart(args.head.accept(this))

          case WIN_END =>
            assert(args.size == 1)
            WindowEnd(args.head.accept(this))

          case ASC =>
            assert(args.size == 1)
            Asc(args.head.accept(this))

          case DESC =>
            assert(args.size == 1)
            Desc(args.head.accept(this))

          case MD5 =>
            assert(args.size == 1)
            Md5(args.head.accept(this))

          case SHA1 =>
            assert(args.size == 1)
            Sha1(args.head.accept(this))

          case SHA224 =>
            assert(args.size == 1)
            Sha224(args.head.accept(this))

          case SHA256 =>
            assert(args.size == 1)
            Sha256(args.head.accept(this))

          case SHA384 =>
            assert(args.size == 1)
            Sha384(args.head.accept(this))

          case SHA512 =>
            assert(args.size == 1)
            Sha512(args.head.accept(this))

          case SHA2 =>
            assert(args.size == 2)
            Sha2(args.head.accept(this), args.last.accept(this))

          case PROC_TIME =>
            ProctimeAttribute(args.head.accept(this))

          case ROW_TIME =>
            RowtimeAttribute(args.head.accept(this))

          case OVER_CALL =>
            UnresolvedOverCall(
              args.head.accept(this),
              args.last.accept(this)
            )

          case UNBOUNDED_RANGE =>
            UnboundedRange()

          case UNBOUNDED_ROW =>
            UnboundedRow()

          case CURRENT_RANGE =>
            CurrentRange()

          case CURRENT_ROW =>
            CurrentRow()

          case _ => PlannerCall(e.getName, args.map(_.accept(this)))
        }
    }
  }

  override def visitSymbol(symbolExpression: SymbolExpression): PlannerExpression = {
    SymbolPlannerExpression(symbolExpression.getSymbol)
  }

  override def visitValueLiteral(literal: ValueLiteralExpression): PlannerExpression = {
    if (!literal.getType.isPresent) {
      PlannerLiteral(literal.getValue)
    } else if (literal.getValue == null) {
      Null(literal.getType.get())
    } else {
      PlannerLiteral(literal.getValue, literal.getType.get())
    }
  }

  override def visit(other: Expression): PlannerExpression = {
    other match {
      case e: TableReferenceExpression => PlannerTableReference(
        e.asInstanceOf[TableReferenceExpression].getName,
        e.asInstanceOf[TableReferenceExpression].getTable)
      case plannerExpression: PlannerExpression => plannerExpression
      case _ =>
        throw new TableException("Unrecognized expression [" + other + "]")
    }
  }

  override def visitFieldReference(fieldReference: FieldReferenceExpression): PlannerExpression = {
    if (fieldReference.getResultType.isPresent) {
      ResolvedFieldReference(
        fieldReference.getName,
        fieldReference.getResultType.get())
    } else {
      UnresolvedFieldReference(fieldReference.getName)
    }
  }
}

object DefaultExpressionVisitor {
  val INSTANCE: DefaultExpressionVisitor = new DefaultExpressionVisitor
}
