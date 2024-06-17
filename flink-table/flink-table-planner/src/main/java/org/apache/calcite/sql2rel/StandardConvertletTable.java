/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql2rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.fun.SqlJsonQueryFunction;
import org.apache.calcite.sql.fun.SqlJsonValueFunction;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlLiteralChainOperator;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlMultisetQueryConstructor;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.fun.SqlOverlapsOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSequenceValueOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;

/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 *
 * <p>Lines 691-736 implement supporting RETURNING clause in JSON_QUERY (CALCITE-6365).
 */
public class StandardConvertletTable extends ReflectiveConvertletTable {

    /** Singleton instance. */
    public static final StandardConvertletTable INSTANCE = new StandardConvertletTable();

    // ~ Constructors -----------------------------------------------------------

    private StandardConvertletTable() {
        super();

        // Register aliases (operators which have a different name but
        // identical behavior to other operators).
        addAlias(SqlStdOperatorTable.CHARACTER_LENGTH, SqlStdOperatorTable.CHAR_LENGTH);
        addAlias(SqlStdOperatorTable.IS_UNKNOWN, SqlStdOperatorTable.IS_NULL);
        addAlias(SqlStdOperatorTable.IS_NOT_UNKNOWN, SqlStdOperatorTable.IS_NOT_NULL);
        addAlias(SqlLibraryOperators.NULL_SAFE_EQUAL, SqlStdOperatorTable.IS_NOT_DISTINCT_FROM);
        addAlias(SqlStdOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD);

        // Register convertlets for specific objects.
        registerOp(SqlStdOperatorTable.CAST, this::convertCast);
        registerOp(SqlLibraryOperators.INFIX_CAST, this::convertCast);
        registerOp(
                SqlStdOperatorTable.IS_DISTINCT_FROM,
                (cx, call) -> convertIsDistinctFrom(cx, call, false));
        registerOp(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                (cx, call) -> convertIsDistinctFrom(cx, call, true));

        registerOp(SqlStdOperatorTable.PLUS, this::convertPlus);

        registerOp(
                SqlStdOperatorTable.MINUS,
                (cx, call) -> {
                    final RexCall e = (RexCall) StandardConvertletTable.this.convertCall(cx, call);
                    switch (e.getOperands().get(0).getType().getSqlTypeName()) {
                        case DATE:
                        case TIME:
                        case TIMESTAMP:
                            return convertDatetimeMinus(cx, SqlStdOperatorTable.MINUS_DATE, call);
                        default:
                            return e;
                    }
                });

        registerOp(SqlLibraryOperators.LTRIM, new TrimConvertlet(SqlTrimFunction.Flag.LEADING));
        registerOp(SqlLibraryOperators.RTRIM, new TrimConvertlet(SqlTrimFunction.Flag.TRAILING));

        registerOp(SqlLibraryOperators.GREATEST, new GreatestConvertlet());
        registerOp(SqlLibraryOperators.LEAST, new GreatestConvertlet());
        registerOp(
                SqlLibraryOperators.SUBSTR_BIG_QUERY, new SubstrConvertlet(SqlLibrary.BIG_QUERY));
        registerOp(SqlLibraryOperators.SUBSTR_MYSQL, new SubstrConvertlet(SqlLibrary.MYSQL));
        registerOp(SqlLibraryOperators.SUBSTR_ORACLE, new SubstrConvertlet(SqlLibrary.ORACLE));
        registerOp(
                SqlLibraryOperators.SUBSTR_POSTGRESQL, new SubstrConvertlet(SqlLibrary.POSTGRESQL));

        registerOp(SqlLibraryOperators.NVL, StandardConvertletTable::convertNvl);
        registerOp(SqlLibraryOperators.DECODE, StandardConvertletTable::convertDecode);
        registerOp(SqlLibraryOperators.IF, StandardConvertletTable::convertIf);

        // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
        registerOp(
                SqlStdOperatorTable.NOT_LIKE,
                (cx, call) ->
                        cx.convertExpression(
                                SqlStdOperatorTable.NOT.createCall(
                                        SqlParserPos.ZERO,
                                        SqlStdOperatorTable.LIKE.createCall(
                                                SqlParserPos.ZERO, call.getOperandList()))));

        // Expand "x NOT ILIKE y" into "NOT (x ILIKE y)"
        registerOp(
                SqlLibraryOperators.NOT_ILIKE,
                (cx, call) ->
                        cx.convertExpression(
                                SqlStdOperatorTable.NOT.createCall(
                                        SqlParserPos.ZERO,
                                        SqlLibraryOperators.ILIKE.createCall(
                                                SqlParserPos.ZERO, call.getOperandList()))));

        // Expand "x NOT RLIKE y" into "NOT (x RLIKE y)"
        registerOp(
                SqlLibraryOperators.NOT_RLIKE,
                (cx, call) ->
                        cx.convertExpression(
                                SqlStdOperatorTable.NOT.createCall(
                                        SqlParserPos.ZERO,
                                        SqlLibraryOperators.RLIKE.createCall(
                                                SqlParserPos.ZERO, call.getOperandList()))));

        // Expand "x NOT SIMILAR y" into "NOT (x SIMILAR y)"
        registerOp(
                SqlStdOperatorTable.NOT_SIMILAR_TO,
                (cx, call) ->
                        cx.convertExpression(
                                SqlStdOperatorTable.NOT.createCall(
                                        SqlParserPos.ZERO,
                                        SqlStdOperatorTable.SIMILAR_TO.createCall(
                                                SqlParserPos.ZERO, call.getOperandList()))));

        // Unary "+" has no effect, so expand "+ x" into "x".
        registerOp(
                SqlStdOperatorTable.UNARY_PLUS,
                (cx, call) -> cx.convertExpression(call.operand(0)));

        // "DOT"
        registerOp(
                SqlStdOperatorTable.DOT,
                (cx, call) ->
                        cx.getRexBuilder()
                                .makeFieldAccess(
                                        cx.convertExpression(call.operand(0)),
                                        call.operand(1).toString(),
                                        false));
        // "ITEM"
        registerOp(SqlStdOperatorTable.ITEM, this::convertItem);
        // "AS" has no effect, so expand "x AS id" into "x".
        registerOp(SqlStdOperatorTable.AS, (cx, call) -> cx.convertExpression(call.operand(0)));
        // "SQRT(x)" is equivalent to "POWER(x, .5)"
        registerOp(
                SqlStdOperatorTable.SQRT,
                (cx, call) ->
                        cx.convertExpression(
                                SqlStdOperatorTable.POWER.createCall(
                                        SqlParserPos.ZERO,
                                        call.operand(0),
                                        SqlLiteral.createExactNumeric("0.5", SqlParserPos.ZERO))));

        // REVIEW jvs 24-Apr-2006: This only seems to be working from within a
        // windowed agg.  I have added an optimizer rule
        // org.apache.calcite.rel.rules.AggregateReduceFunctionsRule which handles
        // other cases post-translation.  The reason I did that was to defer the
        // implementation decision; e.g. we may want to push it down to a foreign
        // server directly rather than decomposed; decomposition is easier than
        // recognition.

        // Convert "avg(<expr>)" to "cast(sum(<expr>) / count(<expr>) as
        // <type>)". We don't need to handle the empty set specially, because
        // the SUM is already supposed to come out as NULL in cases where the
        // COUNT is zero, so the null check should take place first and prevent
        // division by zero. We need the cast because SUM and COUNT may use
        // different types, say BIGINT.
        //
        // Similarly STDDEV_POP and STDDEV_SAMP, VAR_POP and VAR_SAMP.
        registerOp(SqlStdOperatorTable.AVG, new AvgVarianceConvertlet(SqlKind.AVG));
        registerOp(SqlStdOperatorTable.STDDEV_POP, new AvgVarianceConvertlet(SqlKind.STDDEV_POP));
        registerOp(SqlStdOperatorTable.STDDEV_SAMP, new AvgVarianceConvertlet(SqlKind.STDDEV_SAMP));
        registerOp(SqlStdOperatorTable.STDDEV, new AvgVarianceConvertlet(SqlKind.STDDEV_SAMP));
        registerOp(SqlStdOperatorTable.VAR_POP, new AvgVarianceConvertlet(SqlKind.VAR_POP));
        registerOp(SqlStdOperatorTable.VAR_SAMP, new AvgVarianceConvertlet(SqlKind.VAR_SAMP));
        registerOp(SqlStdOperatorTable.VARIANCE, new AvgVarianceConvertlet(SqlKind.VAR_SAMP));
        registerOp(SqlStdOperatorTable.COVAR_POP, new RegrCovarianceConvertlet(SqlKind.COVAR_POP));
        registerOp(
                SqlStdOperatorTable.COVAR_SAMP, new RegrCovarianceConvertlet(SqlKind.COVAR_SAMP));
        registerOp(SqlStdOperatorTable.REGR_SXX, new RegrCovarianceConvertlet(SqlKind.REGR_SXX));
        registerOp(SqlStdOperatorTable.REGR_SYY, new RegrCovarianceConvertlet(SqlKind.REGR_SYY));

        final SqlRexConvertlet floorCeilConvertlet = new FloorCeilConvertlet();
        registerOp(SqlStdOperatorTable.FLOOR, floorCeilConvertlet);
        registerOp(SqlStdOperatorTable.CEIL, floorCeilConvertlet);

        registerOp(SqlStdOperatorTable.TIMESTAMP_ADD, new TimestampAddConvertlet());
        registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF, new TimestampDiffConvertlet());

        registerOp(SqlStdOperatorTable.INTERVAL, StandardConvertletTable::convertInterval);

        // Convert "element(<expr>)" to "$element_slice(<expr>)", if the
        // expression is a multiset of scalars.
        if (false) {
            registerOp(
                    SqlStdOperatorTable.ELEMENT,
                    (cx, call) -> {
                        assert call.operandCount() == 1;
                        final SqlNode operand = call.operand(0);
                        final RelDataType type = cx.getValidator().getValidatedNodeType(operand);
                        if (!getComponentTypeOrThrow(type).isStruct()) {
                            return cx.convertExpression(
                                    SqlStdOperatorTable.ELEMENT_SLICE.createCall(
                                            SqlParserPos.ZERO, operand));
                        }

                        // fallback on default behavior
                        return StandardConvertletTable.this.convertCall(cx, call);
                    });
        }

        // Convert "$element_slice(<expr>)" to "element(<expr>).field#0"
        if (false) {
            registerOp(
                    SqlStdOperatorTable.ELEMENT_SLICE,
                    (cx, call) -> {
                        assert call.operandCount() == 1;
                        final SqlNode operand = call.operand(0);
                        final RexNode expr =
                                cx.convertExpression(
                                        SqlStdOperatorTable.ELEMENT.createCall(
                                                SqlParserPos.ZERO, operand));
                        return cx.getRexBuilder().makeFieldAccess(expr, 0);
                    });
        }
    }

    /** Converts a call to the NVL function. */
    private static RexNode convertNvl(SqlRexContext cx, SqlCall call) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final RexNode operand0 = cx.convertExpression(call.getOperandList().get(0));
        final RexNode operand1 = cx.convertExpression(call.getOperandList().get(1));
        final RelDataType type = cx.getValidator().getValidatedNodeType(call);
        // Preserve Operand Nullability
        return rexBuilder.makeCall(
                type,
                SqlStdOperatorTable.CASE,
                ImmutableList.of(
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand0),
                        rexBuilder.makeCast(
                                cx.getTypeFactory()
                                        .createTypeWithNullability(
                                                type, operand0.getType().isNullable()),
                                operand0),
                        rexBuilder.makeCast(
                                cx.getTypeFactory()
                                        .createTypeWithNullability(
                                                type, operand1.getType().isNullable()),
                                operand1)));
    }

    /** Converts a call to the DECODE function. */
    private static RexNode convertDecode(SqlRexContext cx, SqlCall call) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> operands =
                convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
        final RelDataType type = cx.getValidator().getValidatedNodeType(call);
        final List<RexNode> exprs = new ArrayList<>();
        for (int i = 1; i < operands.size() - 1; i += 2) {
            exprs.add(
                    RelOptUtil.isDistinctFrom(rexBuilder, operands.get(0), operands.get(i), true));
            exprs.add(operands.get(i + 1));
        }
        if (operands.size() % 2 == 0) {
            exprs.add(Util.last(operands));
        } else {
            exprs.add(rexBuilder.makeNullLiteral(type));
        }
        return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprs);
    }

    /**
     * Converts a call to the IF function.
     *
     * <p>{@code IF(b, x, y)} &rarr; {@code CASE WHEN b THEN x ELSE y END}.
     */
    private static RexNode convertIf(SqlRexContext cx, SqlCall call) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> operands =
                convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
        final RelDataType type = cx.getValidator().getValidatedNodeType(call);
        return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, operands);
    }

    /** Converts an interval expression to a numeric multiplied by an interval literal. */
    private static RexNode convertInterval(SqlRexContext cx, SqlCall call) {
        // "INTERVAL n HOUR" becomes "n * INTERVAL '1' HOUR"
        final SqlNode n = call.operand(0);
        final SqlIntervalQualifier intervalQualifier = call.operand(1);
        final SqlIntervalLiteral literal =
                SqlLiteral.createInterval(1, "1", intervalQualifier, call.getParserPosition());
        final SqlCall multiply =
                SqlStdOperatorTable.MULTIPLY.createCall(call.getParserPosition(), n, literal);
        return cx.convertExpression(multiply);
    }

    // ~ Methods ----------------------------------------------------------------

    private static RexNode or(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, a0, a1);
    }

    private static RexNode eq(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, a0, a1);
    }

    private static RexNode ge(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, a0, a1);
    }

    private static RexNode le(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, a0, a1);
    }

    private static RexNode and(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, a0, a1);
    }

    private static RexNode divideInt(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, a0, a1);
    }

    private static RexNode plus(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, a0, a1);
    }

    private static RexNode minus(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a0, a1);
    }

    private static RexNode multiply(RexBuilder rexBuilder, RexNode a0, RexNode a1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, a0, a1);
    }

    private static RexNode case_(RexBuilder rexBuilder, RexNode... args) {
        return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args);
    }

    // SqlNode helpers

    private static SqlCall plus(SqlParserPos pos, SqlNode a0, SqlNode a1) {
        return SqlStdOperatorTable.PLUS.createCall(pos, a0, a1);
    }

    /** Converts a CASE expression. */
    public RexNode convertCase(SqlRexContext cx, SqlCase call) {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        assert whenList.size() == thenList.size();

        RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> exprList = new ArrayList<>();
        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        final RexLiteral unknownLiteral =
                rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
        final RexLiteral nullLiteral =
                rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        for (int i = 0; i < whenList.size(); i++) {
            if (SqlUtil.isNullLiteral(whenList.get(i), false)) {
                exprList.add(unknownLiteral);
            } else {
                exprList.add(cx.convertExpression(whenList.get(i)));
            }
            if (SqlUtil.isNullLiteral(thenList.get(i), false)) {
                exprList.add(nullLiteral);
            } else {
                exprList.add(cx.convertExpression(thenList.get(i)));
            }
        }
        SqlNode elseOperand = call.getElseOperand();
        if (SqlUtil.isNullLiteral(elseOperand, false)) {
            exprList.add(nullLiteral);
        } else {
            exprList.add(cx.convertExpression(requireNonNull(elseOperand, "elseOperand")));
        }

        RelDataType type = rexBuilder.deriveReturnType(call.getOperator(), exprList);
        for (int i : elseArgs(exprList.size())) {
            exprList.set(i, rexBuilder.ensureType(type, exprList.get(i), false));
        }
        return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprList);
    }

    public RexNode convertMultiset(SqlRexContext cx, SqlMultisetValueConstructor op, SqlCall call) {
        final RelDataType originalType = cx.getValidator().getValidatedNodeType(call);
        RexRangeRef rr = cx.getSubQueryExpr(call);
        assert rr != null;
        RelDataType msType = rr.getType().getFieldList().get(0).getType();
        RexNode expr = cx.getRexBuilder().makeInputRef(msType, rr.getOffset());
        assert msType.getComponentType() != null && msType.getComponentType().isStruct()
                : "componentType of " + msType + " must be struct";
        assert originalType.getComponentType() != null
                : "componentType of " + originalType + " must be struct";
        if (!originalType.getComponentType().isStruct()) {
            // If the type is not a struct, the multiset operator will have
            // wrapped the type as a record. Add a call to the $SLICE operator
            // to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
            // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr =
                    cx.getRexBuilder()
                            .makeCall(
                                    originalType,
                                    SqlStdOperatorTable.SLICE,
                                    ImmutableList.of(expr));
        }
        return expr;
    }

    public RexNode convertArray(SqlRexContext cx, SqlArrayValueConstructor op, SqlCall call) {
        return convertCall(cx, call);
    }

    public RexNode convertMap(SqlRexContext cx, SqlMapValueConstructor op, SqlCall call) {
        return convertCall(cx, call);
    }

    public RexNode convertMultisetQuery(
            SqlRexContext cx, SqlMultisetQueryConstructor op, SqlCall call) {
        final RelDataType originalType = cx.getValidator().getValidatedNodeType(call);
        RexRangeRef rr = cx.getSubQueryExpr(call);
        assert rr != null;
        RelDataType msType = rr.getType().getFieldList().get(0).getType();
        RexNode expr = cx.getRexBuilder().makeInputRef(msType, rr.getOffset());
        assert msType.getComponentType() != null
                : "componentType of " + msType + " must not be null";
        assert originalType.getComponentType() != null
                : "componentType of " + originalType + " must not be null";
        return expr;
    }

    public RexNode convertJdbc(SqlRexContext cx, SqlJdbcFunctionCall op, SqlCall call) {
        // Yuck!! The function definition contains arguments!
        // TODO: adopt a more conventional definition/instance structure
        final SqlCall convertedCall = op.getLookupCall();
        return cx.convertExpression(convertedCall);
    }

    protected RexNode convertCast(SqlRexContext cx, final SqlCall call) {
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        assert call.getKind() == SqlKind.CAST;
        final SqlNode left = call.operand(0);
        final SqlNode right = call.operand(1);
        if (right instanceof SqlIntervalQualifier) {
            final SqlIntervalQualifier intervalQualifier = (SqlIntervalQualifier) right;
            if (left instanceof SqlIntervalLiteral) {
                RexLiteral sourceInterval = (RexLiteral) cx.convertExpression(left);
                BigDecimal sourceValue = (BigDecimal) sourceInterval.getValue();
                RexLiteral castedInterval =
                        cx.getRexBuilder().makeIntervalLiteral(sourceValue, intervalQualifier);
                return castToValidatedType(cx, call, castedInterval);
            } else if (left instanceof SqlNumericLiteral) {
                RexLiteral sourceInterval = (RexLiteral) cx.convertExpression(left);
                BigDecimal sourceValue = (BigDecimal) sourceInterval.getValue();
                final BigDecimal multiplier = intervalQualifier.getUnit().multiplier;
                sourceValue = SqlFunctions.multiply(sourceValue, multiplier);
                RexLiteral castedInterval =
                        cx.getRexBuilder().makeIntervalLiteral(sourceValue, intervalQualifier);
                return castToValidatedType(cx, call, castedInterval);
            }
            return castToValidatedType(cx, call, cx.convertExpression(left));
        }
        SqlDataTypeSpec dataType = (SqlDataTypeSpec) right;
        RelDataType type = dataType.deriveType(cx.getValidator());
        if (type == null) {
            type = cx.getValidator().getValidatedNodeType(dataType.getTypeName());
        }
        RexNode arg = cx.convertExpression(left);
        if (arg.getType().isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true);
        }
        if (SqlUtil.isNullLiteral(left, false)) {
            final SqlValidatorImpl validator = (SqlValidatorImpl) cx.getValidator();
            validator.setValidatedNodeType(left, type);
            return cx.convertExpression(left);
        }
        if (null != dataType.getCollectionsTypeName()) {
            final RelDataType argComponentType =
                    requireNonNull(
                            arg.getType().getComponentType(), () -> "componentType of " + arg);

            RelDataType typeFinal = type;
            final RelDataType componentType =
                    requireNonNull(type.getComponentType(), () -> "componentType of " + typeFinal);
            if (argComponentType.isStruct() && !componentType.isStruct()) {
                RelDataType tt =
                        typeFactory
                                .builder()
                                .add(
                                        argComponentType.getFieldList().get(0).getName(),
                                        componentType)
                                .build();
                tt = typeFactory.createTypeWithNullability(tt, componentType.isNullable());
                boolean isn = type.isNullable();
                type = typeFactory.createMultisetType(tt, -1);
                type = typeFactory.createTypeWithNullability(type, isn);
            }
        }
        return cx.getRexBuilder().makeCast(type, arg);
    }

    protected RexNode convertFloorCeil(SqlRexContext cx, SqlCall call) {
        final boolean floor = call.getKind() == SqlKind.FLOOR;
        // Rewrite floor, ceil of interval
        if (call.operandCount() == 1 && call.operand(0) instanceof SqlIntervalLiteral) {
            final SqlIntervalLiteral literal = call.operand(0);
            SqlIntervalLiteral.IntervalValue interval =
                    literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
            BigDecimal val = interval.getIntervalQualifier().getStartUnit().multiplier;
            RexNode rexInterval = cx.convertExpression(literal);

            final RexBuilder rexBuilder = cx.getRexBuilder();
            RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0));
            RexNode cond = ge(rexBuilder, rexInterval, zero);

            RexNode pad = rexBuilder.makeExactLiteral(val.subtract(BigDecimal.ONE));
            RexNode cast =
                    rexBuilder.makeReinterpretCast(
                            rexInterval.getType(), pad, rexBuilder.makeLiteral(false));
            RexNode sum =
                    floor
                            ? minus(rexBuilder, rexInterval, cast)
                            : plus(rexBuilder, rexInterval, cast);

            RexNode kase =
                    floor
                            ? case_(rexBuilder, rexInterval, cond, sum)
                            : case_(rexBuilder, sum, cond, rexInterval);

            RexNode factor = rexBuilder.makeExactLiteral(val);
            RexNode div = divideInt(rexBuilder, kase, factor);
            return multiply(rexBuilder, div, factor);
        }

        // normal floor, ceil function
        return convertFunction(cx, (SqlFunction) call.getOperator(), call);
    }

    /**
     * Converts a call to the {@code EXTRACT} function.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertExtract(SqlRexContext cx, SqlExtractFunction op, SqlCall call) {
        return convertFunction(cx, (SqlFunction) call.getOperator(), call);
    }

    @SuppressWarnings("unused")
    private static RexNode mod(
            RexBuilder rexBuilder, RelDataType resType, RexNode res, BigDecimal val) {
        if (val.equals(BigDecimal.ONE)) {
            return res;
        }
        return rexBuilder.makeCall(
                SqlStdOperatorTable.MOD, res, rexBuilder.makeExactLiteral(val, resType));
    }

    private static RexNode divide(RexBuilder rexBuilder, RexNode res, BigDecimal val) {
        if (val.equals(BigDecimal.ONE)) {
            return res;
        }
        // If val is between 0 and 1, rather than divide by val, multiply by its
        // reciprocal. For example, rather than divide by 0.001 multiply by 1000.
        if (val.compareTo(BigDecimal.ONE) < 0 && val.signum() == 1) {
            try {
                final BigDecimal reciprocal = BigDecimal.ONE.divide(val, RoundingMode.UNNECESSARY);
                return multiply(rexBuilder, res, rexBuilder.makeExactLiteral(reciprocal));
            } catch (ArithmeticException e) {
                // ignore - reciprocal is not an integer
            }
        }
        return divideInt(rexBuilder, res, rexBuilder.makeExactLiteral(val));
    }

    public RexNode convertDatetimeMinus(
            SqlRexContext cx, SqlDatetimeSubtractionOperator op, SqlCall call) {
        // Rewrite datetime minus
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> exprs =
                convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);

        final RelDataType resType = cx.getValidator().getValidatedNodeType(call);
        return rexBuilder.makeCall(resType, op, exprs.subList(0, 2));
    }

    public RexNode convertFunction(SqlRexContext cx, SqlFunction fun, SqlCall call) {
        final List<RexNode> exprs =
                convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
        if (fun.getFunctionType() == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
            return makeConstructorCall(cx, fun, exprs);
        }
        RelDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown(call);
        if (returnType == null) {
            returnType = cx.getRexBuilder().deriveReturnType(fun, exprs);
        }
        return cx.getRexBuilder().makeCall(returnType, fun, exprs);
    }

    public RexNode convertWindowFunction(
            SqlRexContext cx, SqlWindowTableFunction fun, SqlCall call) {
        // The first operand of window function is actually a query, skip that.
        final List<SqlNode> operands = Util.skip(call.getOperandList());
        final List<RexNode> exprs =
                convertOperands(cx, call, operands, SqlOperandTypeChecker.Consistency.NONE);
        RelDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown(call);
        if (returnType == null) {
            returnType = cx.getRexBuilder().deriveReturnType(fun, exprs);
        }
        return cx.getRexBuilder().makeCall(returnType, fun, exprs);
    }

    // BEGIN FLINK MODIFICATION
    // Reason: this method is changed to extract return type from RETURNING clause of JSON_QUERY
    // Whole class should be removed after CALCITE-6365 is fixed
    public RexNode convertJsonValueFunction(
            SqlRexContext cx, SqlJsonValueFunction fun, SqlCall call) {
        return convertJsonReturningFunction(
                cx,
                fun,
                call,
                SqlJsonValueFunction::hasExplicitTypeSpec,
                SqlJsonValueFunction::removeTypeSpecOperands);
    }

    public RexNode convertJsonQueryFunction(
            SqlRexContext cx, SqlJsonQueryFunction fun, SqlCall call) {
        return convertJsonReturningFunction(
                cx,
                fun,
                call,
                SqlJsonQueryFunction::hasExplicitTypeSpec,
                SqlJsonQueryFunction::removeTypeSpecOperands);
    }

    public RexNode convertJsonReturningFunction(
            SqlRexContext cx,
            SqlFunction fun,
            SqlCall call,
            Predicate<SqlNode[]> hasExplicitTypeSpec,
            Function<SqlCall, List<SqlNode>> removeTypeSpecOperands) {
        // For Expression with explicit return type:
        // i.e. json_query('{"foo":"bar"}', 'lax $.foo', returning varchar(2000))
        // use the specified type as the return type.
        List<SqlNode> operands = call.getOperandList();
        @SuppressWarnings("all")
        boolean hasExplicitReturningType =
                hasExplicitTypeSpec.test(operands.toArray(SqlNode.EMPTY_ARRAY));
        if (hasExplicitReturningType) {
            operands = removeTypeSpecOperands.apply(call);
        }
        final List<RexNode> exprs =
                convertOperands(cx, call, operands, SqlOperandTypeChecker.Consistency.NONE);
        RelDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown(call);
        requireNonNull(returnType, () -> "Unable to get type of " + call);
        return cx.getRexBuilder().makeCall(returnType, fun, exprs);
    }
    // END FLINK MODIFICATION

    public RexNode convertSequenceValue(
            SqlRexContext cx, SqlSequenceValueOperator fun, SqlCall call) {
        final List<SqlNode> operands = call.getOperandList();
        assert operands.size() == 1;
        assert operands.get(0) instanceof SqlIdentifier;
        final SqlIdentifier id = (SqlIdentifier) operands.get(0);
        final String key = Util.listToString(id.names);
        RelDataType returnType = cx.getValidator().getValidatedNodeType(call);
        return cx.getRexBuilder()
                .makeCall(returnType, fun, ImmutableList.of(cx.getRexBuilder().makeLiteral(key)));
    }

    public RexNode convertAggregateFunction(SqlRexContext cx, SqlAggFunction fun, SqlCall call) {
        final List<RexNode> exprs;
        if (call.isCountStar()) {
            exprs = ImmutableList.of();
        } else {
            exprs = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
        }
        RelDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown(call);
        final int groupCount = cx.getGroupCount();
        if (returnType == null) {
            RexCallBinding binding =
                    new RexCallBinding(cx.getTypeFactory(), fun, exprs, ImmutableList.of()) {
                        @Override
                        public int getGroupCount() {
                            return groupCount;
                        }
                    };
            returnType = fun.inferReturnType(binding);
        }
        return cx.getRexBuilder().makeCall(returnType, fun, exprs);
    }

    private static RexNode makeConstructorCall(
            SqlRexContext cx, SqlFunction constructor, List<RexNode> exprs) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        RelDataType type = rexBuilder.deriveReturnType(constructor, exprs);

        int n = type.getFieldCount();
        ImmutableList.Builder<RexNode> initializationExprs = ImmutableList.builder();
        final InitializerContext initializerContext =
                new InitializerContext() {
                    @Override
                    public RexBuilder getRexBuilder() {
                        return rexBuilder;
                    }

                    @Override
                    public SqlNode validateExpression(RelDataType rowType, SqlNode expr) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public RexNode convertExpression(SqlNode e) {
                        throw new UnsupportedOperationException();
                    }
                };
        for (int i = 0; i < n; ++i) {
            initializationExprs.add(
                    cx.getInitializerExpressionFactory()
                            .newAttributeInitializer(
                                    type, constructor, i, exprs, initializerContext));
        }

        List<RexNode> defaultCasts =
                RexUtil.generateCastExpressions(rexBuilder, type, initializationExprs.build());

        return rexBuilder.makeNewInvocation(type, defaultCasts);
    }

    private RexNode convertItem(SqlRexContext cx, SqlCall call) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final SqlOperator op = call.getOperator();
        SqlOperandTypeChecker operandTypeChecker = op.getOperandTypeChecker();
        final SqlOperandTypeChecker.Consistency consistency =
                operandTypeChecker == null
                        ? SqlOperandTypeChecker.Consistency.NONE
                        : operandTypeChecker.getConsistency();
        final List<RexNode> exprs = convertOperands(cx, call, consistency);

        final RelDataType collectionType = exprs.get(0).getType();
        final boolean isRowTypeField = SqlTypeUtil.isRow(collectionType);
        final boolean isNumericIndex = SqlTypeUtil.isIntType(exprs.get(1).getType());

        if (isRowTypeField && isNumericIndex) {
            final SqlOperatorBinding opBinding =
                    new RexCallBinding(cx.getTypeFactory(), op, exprs, ImmutableList.of());
            final RelDataType operandType = opBinding.getOperandType(0);

            final Integer index = opBinding.getOperandLiteralValue(1, Integer.class);
            if (index == null || index < 1 || index > operandType.getFieldCount()) {
                throw new AssertionError(
                        "Cannot access field at position "
                                + index
                                + " within ROW type: "
                                + operandType);
            } else {
                RelDataTypeField relDataTypeField = collectionType.getFieldList().get(index - 1);
                return rexBuilder.makeFieldAccess(exprs.get(0), relDataTypeField.getName(), false);
            }
        }
        RelDataType type = rexBuilder.deriveReturnType(op, exprs);
        return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));
    }

    /**
     * Converts a call to an operator into a {@link RexCall} to the same operator.
     *
     * <p>Called automatically via reflection.
     *
     * @param cx Context
     * @param call Call
     * @return Rex call
     */
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
        final SqlOperator op = call.getOperator();
        final RexBuilder rexBuilder = cx.getRexBuilder();
        SqlOperandTypeChecker operandTypeChecker = op.getOperandTypeChecker();
        final SqlOperandTypeChecker.Consistency consistency =
                operandTypeChecker == null
                        ? SqlOperandTypeChecker.Consistency.NONE
                        : operandTypeChecker.getConsistency();
        final List<RexNode> exprs = convertOperands(cx, call, consistency);
        RelDataType type = rexBuilder.deriveReturnType(op, exprs);

        // Expand 'ROW (x0, x1, ...) = ROW (y0, y1, ...)'
        // to 'x0 = y0 AND x1 = y1 AND ...'
        if (op.kind == SqlKind.EQUALS) {
            final RexNode expr0 = RexUtil.removeCast(exprs.get(0));
            final RexNode expr1 = RexUtil.removeCast(exprs.get(1));
            if (expr0.getKind() == SqlKind.ROW && expr1.getKind() == SqlKind.ROW) {
                final RexCall call0 = (RexCall) expr0;
                final RexCall call1 = (RexCall) expr1;
                final List<RexNode> eqList = new ArrayList<>();
                Pair.forEach(
                        call0.getOperands(),
                        call1.getOperands(),
                        (x, y) -> eqList.add(rexBuilder.makeCall(op, x, y)));
                return RexUtil.composeConjunction(rexBuilder, eqList);
            }
        }
        return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));
    }

    private static List<Integer> elseArgs(int count) {
        // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
        // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
        final List<Integer> list = new ArrayList<>();
        for (int i = count % 2; ; ) {
            list.add(i);
            i += 2;
            if (i >= count) {
                list.add(i - 1);
                break;
            }
        }
        return list;
    }

    private static List<RexNode> convertOperands(
            SqlRexContext cx, SqlCall call, SqlOperandTypeChecker.Consistency consistency) {
        List<SqlNode> operandList;
        if (call.getOperator() instanceof SqlTableFunction) {
            // skip set semantic table node of table function
            operandList =
                    call.getOperandList().stream()
                            .filter(operand -> operand.getKind() != SqlKind.SET_SEMANTICS_TABLE)
                            .collect(Collectors.toList());
        } else {
            operandList = call.getOperandList();
        }
        return convertOperands(cx, call, operandList, consistency);
    }

    private static List<RexNode> convertOperands(
            SqlRexContext cx,
            SqlCall call,
            List<SqlNode> nodes,
            SqlOperandTypeChecker.Consistency consistency) {
        final List<RexNode> exprs = new ArrayList<>();
        for (SqlNode node : nodes) {
            exprs.add(cx.convertExpression(node));
        }
        final List<RelDataType> operandTypes = cx.getValidator().getValidatedOperandTypes(call);
        if (operandTypes != null) {
            final List<RexNode> oldExprs = new ArrayList<>(exprs);
            exprs.clear();
            Pair.forEach(
                    oldExprs,
                    operandTypes,
                    (expr, type) -> exprs.add(cx.getRexBuilder().ensureType(type, expr, true)));
        }
        if (exprs.size() > 1) {
            final RelDataType type = consistentType(cx, consistency, RexUtil.types(exprs));
            if (type != null) {
                final List<RexNode> oldExprs = new ArrayList<>(exprs);
                exprs.clear();
                for (RexNode expr : oldExprs) {
                    exprs.add(cx.getRexBuilder().ensureType(type, expr, true));
                }
            }
        }
        return exprs;
    }

    private static @Nullable RelDataType consistentType(
            SqlRexContext cx,
            SqlOperandTypeChecker.Consistency consistency,
            List<RelDataType> types) {
        switch (consistency) {
            case COMPARE:
                if (SqlTypeUtil.areSameFamily(types)) {
                    // All arguments are of same family. No need for explicit casts.
                    return null;
                }
                final List<RelDataType> nonCharacterTypes = new ArrayList<>();
                for (RelDataType type : types) {
                    if (type.getFamily() != SqlTypeFamily.CHARACTER) {
                        nonCharacterTypes.add(type);
                    }
                }
                if (!nonCharacterTypes.isEmpty()) {
                    final int typeCount = types.size();
                    types = nonCharacterTypes;
                    if (nonCharacterTypes.size() < typeCount) {
                        final RelDataTypeFamily family = nonCharacterTypes.get(0).getFamily();
                        if (family instanceof SqlTypeFamily) {
                            // The character arguments might be larger than the numeric
                            // argument. Give ourselves some headroom.
                            switch ((SqlTypeFamily) family) {
                                case INTEGER:
                                case NUMERIC:
                                    nonCharacterTypes.add(
                                            cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
                // fall through
            case LEAST_RESTRICTIVE:
                return cx.getTypeFactory().leastRestrictive(types);
            default:
                return null;
        }
    }

    private RexNode convertPlus(SqlRexContext cx, SqlCall call) {
        final RexNode rex = convertCall(cx, call);
        switch (rex.getType().getSqlTypeName()) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                // Use special "+" operator for datetime + interval.
                // Re-order operands, if necessary, so that interval is second.
                final RexBuilder rexBuilder = cx.getRexBuilder();
                List<RexNode> operands = ((RexCall) rex).getOperands();
                if (operands.size() == 2) {
                    final SqlTypeName sqlTypeName = operands.get(0).getType().getSqlTypeName();
                    switch (sqlTypeName) {
                        case INTERVAL_YEAR:
                        case INTERVAL_YEAR_MONTH:
                        case INTERVAL_MONTH:
                        case INTERVAL_DAY:
                        case INTERVAL_DAY_HOUR:
                        case INTERVAL_DAY_MINUTE:
                        case INTERVAL_DAY_SECOND:
                        case INTERVAL_HOUR:
                        case INTERVAL_HOUR_MINUTE:
                        case INTERVAL_HOUR_SECOND:
                        case INTERVAL_MINUTE:
                        case INTERVAL_MINUTE_SECOND:
                        case INTERVAL_SECOND:
                            operands = ImmutableList.of(operands.get(1), operands.get(0));
                            break;
                        default:
                            break;
                    }
                }
                return rexBuilder.makeCall(
                        rex.getType(), SqlStdOperatorTable.DATETIME_PLUS, operands);
            default:
                return rex;
        }
    }

    private RexNode convertIsDistinctFrom(SqlRexContext cx, SqlCall call, boolean neg) {
        RexNode op0 = cx.convertExpression(call.operand(0));
        RexNode op1 = cx.convertExpression(call.operand(1));
        return RelOptUtil.isDistinctFrom(cx.getRexBuilder(), op0, op1, neg);
    }

    /**
     * Converts a BETWEEN expression.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertBetween(SqlRexContext cx, SqlBetweenOperator op, SqlCall call) {
        SqlOperandTypeChecker operandTypeChecker = op.getOperandTypeChecker();
        final SqlOperandTypeChecker.Consistency consistency =
                operandTypeChecker == null
                        ? SqlOperandTypeChecker.Consistency.NONE
                        : operandTypeChecker.getConsistency();
        final List<RexNode> list = convertOperands(cx, call, consistency);
        final RexNode x = list.get(SqlBetweenOperator.VALUE_OPERAND);
        final RexNode y = list.get(SqlBetweenOperator.LOWER_OPERAND);
        final RexNode z = list.get(SqlBetweenOperator.UPPER_OPERAND);

        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode ge1 = ge(rexBuilder, x, y);
        RexNode le1 = le(rexBuilder, x, z);
        RexNode and1 = and(rexBuilder, ge1, le1);

        RexNode res;
        final SqlBetweenOperator.Flag symmetric = op.flag;
        switch (symmetric) {
            case ASYMMETRIC:
                res = and1;
                break;
            case SYMMETRIC:
                RexNode ge2 = ge(rexBuilder, x, z);
                RexNode le2 = le(rexBuilder, x, y);
                RexNode and2 = and(rexBuilder, ge2, le2);
                res = or(rexBuilder, and1, and2);
                break;
            default:
                throw Util.unexpected(symmetric);
        }
        final SqlBetweenOperator betweenOp = (SqlBetweenOperator) call.getOperator();
        if (betweenOp.isNegated()) {
            res = rexBuilder.makeCall(SqlStdOperatorTable.NOT, res);
        }
        return res;
    }

    /**
     * Converts a SUBSTRING expression.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertSubstring(SqlRexContext cx, SqlSubstringFunction op, SqlCall call) {
        final SqlLibrary library = cx.getValidator().config().conformance().semantics();
        final SqlBasicCall basicCall = (SqlBasicCall) call;
        switch (library) {
            case BIG_QUERY:
                return toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_BIG_QUERY);
            case MYSQL:
                return toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_MYSQL);
            case ORACLE:
                return toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_ORACLE);
            case POSTGRESQL:
            default:
                return convertFunction(cx, op, call);
        }
    }

    private RexNode toRex(SqlRexContext cx, SqlBasicCall call, SqlFunction f) {
        final SqlCall call2 = new SqlBasicCall(f, call.getOperandList(), call.getParserPosition());
        final SqlRexConvertlet convertlet = requireNonNull(get(call2));
        return convertlet.convertCall(cx, call2);
    }

    /**
     * Converts a LiteralChain expression: that is, concatenates the operands immediately, to
     * produce a single literal string.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertLiteralChain(SqlRexContext cx, SqlLiteralChainOperator op, SqlCall call) {
        Util.discard(cx);

        SqlLiteral sum = SqlLiteralChainOperator.concatenateOperands(call);
        return cx.convertLiteral(sum);
    }

    /**
     * Converts a ROW.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertRow(SqlRexContext cx, SqlRowOperator op, SqlCall call) {
        if (cx.getValidator().getValidatedNodeType(call).getSqlTypeName()
                != SqlTypeName.COLUMN_LIST) {
            return convertCall(cx, call);
        }
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> columns = new ArrayList<>();
        for (String operand : SqlIdentifier.simpleNames(call.getOperandList())) {
            columns.add(rexBuilder.makeLiteral(operand));
        }
        final RelDataType type =
                rexBuilder.deriveReturnType(SqlStdOperatorTable.COLUMN_LIST, columns);
        return rexBuilder.makeCall(type, SqlStdOperatorTable.COLUMN_LIST, columns);
    }

    /**
     * Converts a call to OVERLAPS.
     *
     * <p>Called automatically via reflection.
     */
    public RexNode convertOverlaps(SqlRexContext cx, SqlOverlapsOperator op, SqlCall call) {
        // for intervals [t0, t1] overlaps [t2, t3], we can find if the
        // intervals overlaps by: ~(t1 < t2 or t3 < t0)
        assert call.getOperandList().size() == 2;

        final Pair<RexNode, RexNode> left =
                convertOverlapsOperand(cx, call.getParserPosition(), call.operand(0));
        final RexNode r0 = left.left;
        final RexNode r1 = left.right;
        final Pair<RexNode, RexNode> right =
                convertOverlapsOperand(cx, call.getParserPosition(), call.operand(1));
        final RexNode r2 = right.left;
        final RexNode r3 = right.right;

        // Sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode leftSwap = le(rexBuilder, r0, r1);
        final RexNode s0 = case_(rexBuilder, leftSwap, r0, r1);
        final RexNode e0 = case_(rexBuilder, leftSwap, r1, r0);
        RexNode rightSwap = le(rexBuilder, r2, r3);
        final RexNode s1 = case_(rexBuilder, rightSwap, r2, r3);
        final RexNode e1 = case_(rexBuilder, rightSwap, r3, r2);
        // (e0 >= s1) AND (e1 >= s0)
        switch (op.kind) {
            case OVERLAPS:
                return and(rexBuilder, ge(rexBuilder, e0, s1), ge(rexBuilder, e1, s0));
            case CONTAINS:
                return and(rexBuilder, le(rexBuilder, s0, s1), ge(rexBuilder, e0, e1));
            case PERIOD_EQUALS:
                return and(rexBuilder, eq(rexBuilder, s0, s1), eq(rexBuilder, e0, e1));
            case PRECEDES:
                return le(rexBuilder, e0, s1);
            case IMMEDIATELY_PRECEDES:
                return eq(rexBuilder, e0, s1);
            case SUCCEEDS:
                return ge(rexBuilder, s0, e1);
            case IMMEDIATELY_SUCCEEDS:
                return eq(rexBuilder, s0, e1);
            default:
                throw new AssertionError(op);
        }
    }

    private static Pair<RexNode, RexNode> convertOverlapsOperand(
            SqlRexContext cx, SqlParserPos pos, SqlNode operand) {
        final SqlNode a0;
        final SqlNode a1;
        switch (operand.getKind()) {
            case ROW:
                a0 = ((SqlCall) operand).operand(0);
                final SqlNode a10 = ((SqlCall) operand).operand(1);
                final RelDataType t1 = cx.getValidator().getValidatedNodeType(a10);
                if (SqlTypeUtil.isInterval(t1)) {
                    // make t1 = t0 + t1 when t1 is an interval.
                    a1 = plus(pos, a0, a10);
                } else {
                    a1 = a10;
                }
                break;
            default:
                a0 = operand;
                a1 = operand;
        }

        final RexNode r0 = cx.convertExpression(a0);
        final RexNode r1 = cx.convertExpression(a1);
        return Pair.of(r0, r1);
    }

    /**
     * Casts a RexNode value to the validated type of a SqlCall. If the value was already of the
     * validated type, then the value is returned without an additional cast.
     */
    public RexNode castToValidatedType(SqlRexContext cx, SqlCall call, RexNode value) {
        return castToValidatedType(call, value, cx.getValidator(), cx.getRexBuilder());
    }

    /**
     * Casts a RexNode value to the validated type of a SqlCall. If the value was already of the
     * validated type, then the value is returned without an additional cast.
     */
    public static RexNode castToValidatedType(
            SqlNode node, RexNode e, SqlValidator validator, RexBuilder rexBuilder) {
        final RelDataType type = validator.getValidatedNodeType(node);
        if (e.getType() == type) {
            return e;
        }
        return rexBuilder.makeCast(type, e);
    }

    /**
     * Convertlet that handles {@code COVAR_POP}, {@code COVAR_SAMP}, {@code REGR_SXX}, {@code
     * REGR_SYY} windowed aggregate functions.
     */
    private static class RegrCovarianceConvertlet implements SqlRexConvertlet {
        private final SqlKind kind;

        RegrCovarianceConvertlet(SqlKind kind) {
            this.kind = kind;
        }

        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            assert call.operandCount() == 2;
            final SqlNode arg1 = call.operand(0);
            final SqlNode arg2 = call.operand(1);
            final SqlNode expr;
            final RelDataType type = cx.getValidator().getValidatedNodeType(call);
            switch (kind) {
                case COVAR_POP:
                    expr = expandCovariance(arg1, arg2, null, type, cx, true);
                    break;
                case COVAR_SAMP:
                    expr = expandCovariance(arg1, arg2, null, type, cx, false);
                    break;
                case REGR_SXX:
                    expr = expandRegrSzz(arg2, arg1, type, cx, true);
                    break;
                case REGR_SYY:
                    expr = expandRegrSzz(arg1, arg2, type, cx, true);
                    break;
                default:
                    throw Util.unexpected(kind);
            }
            RexNode rex = cx.convertExpression(expr);
            return cx.getRexBuilder().ensureType(type, rex, true);
        }

        private static SqlNode expandRegrSzz(
                final SqlNode arg1,
                final SqlNode arg2,
                final RelDataType avgType,
                final SqlRexContext cx,
                boolean variance) {
            final SqlParserPos pos = SqlParserPos.ZERO;
            final SqlNode count = SqlStdOperatorTable.REGR_COUNT.createCall(pos, arg1, arg2);
            final SqlNode varPop =
                    expandCovariance(arg1, variance ? arg1 : arg2, arg2, avgType, cx, true);
            final RexNode varPopRex = cx.convertExpression(varPop);
            final SqlNode varPopCast;
            varPopCast = getCastedSqlNode(varPop, avgType, pos, varPopRex);
            return SqlStdOperatorTable.MULTIPLY.createCall(pos, varPopCast, count);
        }

        private static SqlNode expandCovariance(
                final SqlNode arg0Input,
                final SqlNode arg1Input,
                final @Nullable SqlNode dependent,
                final RelDataType varType,
                final SqlRexContext cx,
                boolean biased) {
            // covar_pop(x1, x2) ==>
            //     (sum(x1 * x2) - sum(x2) * sum(x1) / count(x1, x2))
            //     / count(x1, x2)
            //
            // covar_samp(x1, x2) ==>
            //     (sum(x1 * x2) - sum(x1) * sum(x2) / count(x1, x2))
            //     / (count(x1, x2) - 1)
            final SqlParserPos pos = SqlParserPos.ZERO;
            final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);

            final RexNode arg0Rex = cx.convertExpression(arg0Input);
            final RexNode arg1Rex = cx.convertExpression(arg1Input);

            final SqlNode arg0 = getCastedSqlNode(arg0Input, varType, pos, arg0Rex);
            final SqlNode arg1 = getCastedSqlNode(arg1Input, varType, pos, arg1Rex);
            final SqlNode argSquared = SqlStdOperatorTable.MULTIPLY.createCall(pos, arg0, arg1);
            final SqlNode sumArgSquared;
            final SqlNode sum0;
            final SqlNode sum1;
            final SqlNode count;
            if (dependent == null) {
                sumArgSquared = SqlStdOperatorTable.SUM.createCall(pos, argSquared);
                sum0 = SqlStdOperatorTable.SUM.createCall(pos, arg0, arg1);
                sum1 = SqlStdOperatorTable.SUM.createCall(pos, arg1, arg0);
                count = SqlStdOperatorTable.REGR_COUNT.createCall(pos, arg0, arg1);
            } else {
                sumArgSquared = SqlStdOperatorTable.SUM.createCall(pos, argSquared, dependent);
                sum0 =
                        SqlStdOperatorTable.SUM.createCall(
                                pos, arg0, Objects.equals(dependent, arg0Input) ? arg1 : dependent);
                sum1 =
                        SqlStdOperatorTable.SUM.createCall(
                                pos, arg1, Objects.equals(dependent, arg1Input) ? arg0 : dependent);
                count =
                        SqlStdOperatorTable.REGR_COUNT.createCall(
                                pos, arg0, Objects.equals(dependent, arg0Input) ? arg1 : dependent);
            }

            final SqlNode sumSquared = SqlStdOperatorTable.MULTIPLY.createCall(pos, sum0, sum1);
            final SqlNode countCasted =
                    getCastedSqlNode(count, varType, pos, cx.convertExpression(count));

            final SqlNode avgSumSquared =
                    SqlStdOperatorTable.DIVIDE.createCall(pos, sumSquared, countCasted);
            final SqlNode diff =
                    SqlStdOperatorTable.MINUS.createCall(pos, sumArgSquared, avgSumSquared);
            SqlNode denominator;
            if (biased) {
                denominator = countCasted;
            } else {
                final SqlNumericLiteral one = SqlLiteral.createExactNumeric("1", pos);
                denominator =
                        new SqlCase(
                                SqlParserPos.ZERO,
                                countCasted,
                                SqlNodeList.of(
                                        SqlStdOperatorTable.EQUALS.createCall(
                                                pos, countCasted, one)),
                                SqlNodeList.of(getCastedSqlNode(nullLiteral, varType, pos, null)),
                                SqlStdOperatorTable.MINUS.createCall(pos, countCasted, one));
            }

            return SqlStdOperatorTable.DIVIDE.createCall(pos, diff, denominator);
        }

        private static SqlNode getCastedSqlNode(
                SqlNode argInput, RelDataType varType, SqlParserPos pos, @Nullable RexNode argRex) {
            SqlNode arg;
            if (argRex != null && !argRex.getType().equals(varType)) {
                arg =
                        SqlStdOperatorTable.CAST.createCall(
                                pos, argInput, SqlTypeUtil.convertTypeToSpec(varType));
            } else {
                arg = argInput;
            }
            return arg;
        }
    }

    /** Convertlet that handles {@code AVG} and {@code VARIANCE} windowed aggregate functions. */
    private static class AvgVarianceConvertlet implements SqlRexConvertlet {
        private final SqlKind kind;

        AvgVarianceConvertlet(SqlKind kind) {
            this.kind = kind;
        }

        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            assert call.operandCount() == 1;
            final SqlNode arg = call.operand(0);
            final SqlNode expr;
            final RelDataType type = cx.getValidator().getValidatedNodeType(call);
            switch (kind) {
                case AVG:
                    expr = expandAvg(arg, type, cx);
                    break;
                case STDDEV_POP:
                    expr = expandVariance(arg, type, cx, true, true);
                    break;
                case STDDEV_SAMP:
                    expr = expandVariance(arg, type, cx, false, true);
                    break;
                case VAR_POP:
                    expr = expandVariance(arg, type, cx, true, false);
                    break;
                case VAR_SAMP:
                    expr = expandVariance(arg, type, cx, false, false);
                    break;
                default:
                    throw Util.unexpected(kind);
            }
            RexNode rex = cx.convertExpression(expr);
            return cx.getRexBuilder().ensureType(type, rex, true);
        }

        private static SqlNode expandAvg(
                final SqlNode arg, final RelDataType avgType, final SqlRexContext cx) {
            final SqlParserPos pos = SqlParserPos.ZERO;
            final SqlNode sum = SqlStdOperatorTable.SUM.createCall(pos, arg);
            final RexNode sumRex = cx.convertExpression(sum);
            final SqlNode sumCast;
            sumCast = getCastedSqlNode(sum, avgType, pos, sumRex);
            final SqlNode count = SqlStdOperatorTable.COUNT.createCall(pos, arg);
            return SqlStdOperatorTable.DIVIDE.createCall(pos, sumCast, count);
        }

        private static SqlNode expandVariance(
                final SqlNode argInput,
                final RelDataType varType,
                final SqlRexContext cx,
                boolean biased,
                boolean sqrt) {
            // stddev_pop(x) ==>
            //   power(
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / count(x),
            //     .5)
            //
            // stddev_samp(x) ==>
            //   power(
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / (count(x) - 1),
            //     .5)
            //
            // var_pop(x) ==>
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / count(x)
            //
            // var_samp(x) ==>
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / (count(x) - 1)
            final SqlParserPos pos = SqlParserPos.ZERO;

            final SqlNode arg =
                    getCastedSqlNode(argInput, varType, pos, cx.convertExpression(argInput));

            final SqlNode argSquared = SqlStdOperatorTable.MULTIPLY.createCall(pos, arg, arg);
            final SqlNode argSquaredCasted =
                    getCastedSqlNode(argSquared, varType, pos, cx.convertExpression(argSquared));
            final SqlNode sumArgSquared = SqlStdOperatorTable.SUM.createCall(pos, argSquaredCasted);
            final SqlNode sumArgSquaredCasted =
                    getCastedSqlNode(
                            sumArgSquared, varType, pos, cx.convertExpression(sumArgSquared));
            final SqlNode sum = SqlStdOperatorTable.SUM.createCall(pos, arg);
            final SqlNode sumCasted =
                    getCastedSqlNode(sum, varType, pos, cx.convertExpression(sum));
            final SqlNode sumSquared =
                    SqlStdOperatorTable.MULTIPLY.createCall(pos, sumCasted, sumCasted);
            final SqlNode sumSquaredCasted =
                    getCastedSqlNode(sumSquared, varType, pos, cx.convertExpression(sumSquared));
            final SqlNode count = SqlStdOperatorTable.COUNT.createCall(pos, arg);
            final SqlNode countCasted =
                    getCastedSqlNode(count, varType, pos, cx.convertExpression(count));
            final SqlNode avgSumSquared =
                    SqlStdOperatorTable.DIVIDE.createCall(pos, sumSquaredCasted, countCasted);
            final SqlNode avgSumSquaredCasted =
                    getCastedSqlNode(
                            avgSumSquared, varType, pos, cx.convertExpression(avgSumSquared));
            final SqlNode diff =
                    SqlStdOperatorTable.MINUS.createCall(
                            pos, sumArgSquaredCasted, avgSumSquaredCasted);
            final SqlNode diffCasted =
                    getCastedSqlNode(diff, varType, pos, cx.convertExpression(diff));
            final SqlNode denominator;
            if (biased) {
                denominator = countCasted;
            } else {
                final SqlNumericLiteral one = SqlLiteral.createExactNumeric("1", pos);
                final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
                denominator =
                        new SqlCase(
                                SqlParserPos.ZERO,
                                count,
                                SqlNodeList.of(
                                        SqlStdOperatorTable.EQUALS.createCall(pos, count, one)),
                                SqlNodeList.of(getCastedSqlNode(nullLiteral, varType, pos, null)),
                                SqlStdOperatorTable.MINUS.createCall(pos, count, one));
            }
            final SqlNode div = SqlStdOperatorTable.DIVIDE.createCall(pos, diffCasted, denominator);
            final SqlNode divCasted =
                    getCastedSqlNode(div, varType, pos, cx.convertExpression(div));

            SqlNode result = div;
            if (sqrt) {
                final SqlNumericLiteral half = SqlLiteral.createExactNumeric("0.5", pos);
                result = SqlStdOperatorTable.POWER.createCall(pos, divCasted, half);
            }
            return result;
        }

        private static SqlNode getCastedSqlNode(
                SqlNode argInput, RelDataType varType, SqlParserPos pos, @Nullable RexNode argRex) {
            SqlNode arg;
            if (argRex != null && !argRex.getType().equals(varType)) {
                arg =
                        SqlStdOperatorTable.CAST.createCall(
                                pos, argInput, SqlTypeUtil.convertTypeToSpec(varType));
            } else {
                arg = argInput;
            }
            return arg;
        }
    }

    /** Convertlet that converts {@code LTRIM} and {@code RTRIM} to {@code TRIM}. */
    private static class TrimConvertlet implements SqlRexConvertlet {
        private final SqlTrimFunction.Flag flag;

        TrimConvertlet(SqlTrimFunction.Flag flag) {
            this.flag = flag;
        }

        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final RexNode operand = cx.convertExpression(call.getOperandList().get(0));
            return rexBuilder.makeCall(
                    SqlStdOperatorTable.TRIM,
                    rexBuilder.makeFlag(flag),
                    rexBuilder.makeLiteral(" "),
                    operand);
        }
    }

    /** Convertlet that converts {@code GREATEST} and {@code LEAST}. */
    private static class GreatestConvertlet implements SqlRexConvertlet {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // Translate
            //   GREATEST(a, b, c, d)
            // to
            //   CASE
            //   WHEN a IS NULL OR b IS NULL OR c IS NULL OR d IS NULL
            //   THEN NULL
            //   WHEN a > b AND a > c AND a > d
            //   THEN a
            //   WHEN b > c AND b > d
            //   THEN b
            //   WHEN c > d
            //   THEN c
            //   ELSE d
            //   END
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final RelDataType type = cx.getValidator().getValidatedNodeType(call);
            final SqlBinaryOperator op;
            switch (call.getKind()) {
                case GREATEST:
                    op = SqlStdOperatorTable.GREATER_THAN;
                    break;
                case LEAST:
                    op = SqlStdOperatorTable.LESS_THAN;
                    break;
                default:
                    throw new AssertionError();
            }
            final List<RexNode> exprs =
                    convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
            final List<RexNode> list = new ArrayList<>();
            final List<RexNode> orList = new ArrayList<>();
            for (RexNode expr : exprs) {
                orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, expr));
            }
            list.add(RexUtil.composeDisjunction(rexBuilder, orList));
            list.add(rexBuilder.makeNullLiteral(type));
            for (int i = 0; i < exprs.size() - 1; i++) {
                RexNode expr = exprs.get(i);
                final List<RexNode> andList = new ArrayList<>();
                for (int j = i + 1; j < exprs.size(); j++) {
                    final RexNode expr2 = exprs.get(j);
                    andList.add(rexBuilder.makeCall(op, expr, expr2));
                }
                list.add(RexUtil.composeConjunction(rexBuilder, andList));
                list.add(expr);
            }
            list.add(exprs.get(exprs.size() - 1));
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, list);
        }
    }

    /** Convertlet that handles {@code FLOOR} and {@code CEIL} functions. */
    private class FloorCeilConvertlet implements SqlRexConvertlet {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            return convertFloorCeil(cx, call);
        }
    }

    /**
     * Convertlet that handles the {@code SUBSTR} function; various dialects have slightly different
     * specifications. PostgreSQL seems to comply with the ISO standard for the {@code SUBSTRING}
     * function, and therefore Calcite's default behavior matches PostgreSQL.
     */
    private static class SubstrConvertlet implements SqlRexConvertlet {
        private final SqlLibrary library;

        SubstrConvertlet(SqlLibrary library) {
            this.library = library;
            Preconditions.checkArgument(
                    library == SqlLibrary.ORACLE
                            || library == SqlLibrary.MYSQL
                            || library == SqlLibrary.BIG_QUERY
                            || library == SqlLibrary.POSTGRESQL);
        }

        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // Translate
            //   SUBSTR(value, start, length)
            //
            // to the following if we want PostgreSQL semantics:
            //   SUBSTRING(value, start, length)
            //
            // to the following if we want Oracle semantics:
            //   SUBSTRING(
            //     value
            //     FROM CASE
            //          WHEN start = 0
            //          THEN 1
            //          WHEN start + (length(value) + 1) < 1
            //          THEN length(value) + 1
            //          WHEN start < 0
            //          THEN start + (length(value) + 1)
            //          ELSE start)
            //     FOR CASE WHEN length < 0 THEN 0 ELSE length END)
            //
            // to the following in MySQL:
            //   SUBSTRING(
            //     value
            //     FROM CASE
            //          WHEN start = 0
            //          THEN length(value) + 1    -- different from Oracle
            //          WHEN start + (length(value) + 1) < 1
            //          THEN length(value) + 1
            //          WHEN start < 0
            //          THEN start + length(value) + 1
            //          ELSE start)
            //     FOR CASE WHEN length < 0 THEN 0 ELSE length END)
            //
            // to the following if we want BigQuery semantics:
            //   CASE
            //   WHEN start + (length(value) + 1) < 1
            //   THEN value
            //   ELSE SUBSTRING(
            //       value
            //       FROM CASE
            //            WHEN start = 0
            //            THEN 1
            //            WHEN start < 0
            //            THEN start + length(value) + 1
            //            ELSE start)
            //       FOR CASE WHEN length < 0 THEN 0 ELSE length END)

            final RexBuilder rexBuilder = cx.getRexBuilder();
            final List<RexNode> exprs =
                    convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE);
            final RexNode value = exprs.get(0);
            final RexNode start = exprs.get(1);
            final RelDataType startType = start.getType();
            final RexLiteral zeroLiteral = rexBuilder.makeLiteral(0, startType);
            final RexLiteral oneLiteral = rexBuilder.makeLiteral(1, startType);
            final RexNode valueLength =
                    SqlTypeUtil.isBinary(value.getType())
                            ? rexBuilder.makeCall(SqlStdOperatorTable.OCTET_LENGTH, value)
                            : rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH, value);
            final RexNode valueLengthPlusOne =
                    rexBuilder.makeCall(SqlStdOperatorTable.PLUS, valueLength, oneLiteral);

            final RexNode newStart;
            switch (library) {
                case POSTGRESQL:
                    if (call.operandCount() == 2) {
                        newStart =
                                rexBuilder.makeCall(
                                        SqlStdOperatorTable.CASE,
                                        rexBuilder.makeCall(
                                                SqlStdOperatorTable.LESS_THAN, start, oneLiteral),
                                        oneLiteral,
                                        start);
                    } else {
                        newStart = start;
                    }
                    break;
                case BIG_QUERY:
                    newStart =
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.CASE,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.EQUALS, start, zeroLiteral),
                                    oneLiteral,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.LESS_THAN, start, zeroLiteral),
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.PLUS, start, valueLengthPlusOne),
                                    start);
                    break;
                default:
                    newStart =
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.CASE,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.EQUALS, start, zeroLiteral),
                                    library == SqlLibrary.MYSQL ? valueLengthPlusOne : oneLiteral,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.LESS_THAN,
                                            rexBuilder.makeCall(
                                                    SqlStdOperatorTable.PLUS,
                                                    start,
                                                    valueLengthPlusOne),
                                            oneLiteral),
                                    valueLengthPlusOne,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.LESS_THAN, start, zeroLiteral),
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.PLUS, start, valueLengthPlusOne),
                                    start);
                    break;
            }

            if (call.operandCount() == 2) {
                return rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, value, newStart);
            }

            assert call.operandCount() == 3;
            final RexNode length = exprs.get(2);
            final RexNode newLength;
            switch (library) {
                case POSTGRESQL:
                    newLength = length;
                    break;
                default:
                    newLength =
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.CASE,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.LESS_THAN, length, zeroLiteral),
                                    zeroLiteral,
                                    length);
            }
            final RexNode substringCall =
                    rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, value, newStart, newLength);
            switch (library) {
                case BIG_QUERY:
                    return rexBuilder.makeCall(
                            SqlStdOperatorTable.CASE,
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.LESS_THAN,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.PLUS, start, valueLengthPlusOne),
                                    oneLiteral),
                            value,
                            substringCall);
                default:
                    return substringCall;
            }
        }
    }

    /** Convertlet that handles the {@code TIMESTAMPADD} function. */
    private static class TimestampAddConvertlet implements SqlRexConvertlet {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // TIMESTAMPADD(unit, count, timestamp)
            //  => timestamp + count * INTERVAL '1' UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlLiteral unitLiteral = call.operand(0);
            final TimeUnit unit = unitLiteral.getValueAs(TimeUnit.class);
            RexNode interval2Add;
            SqlIntervalQualifier qualifier =
                    new SqlIntervalQualifier(unit, null, unitLiteral.getParserPosition());
            RexNode op1 = cx.convertExpression(call.operand(1));
            switch (unit) {
                case MICROSECOND:
                case NANOSECOND:
                    interval2Add =
                            divide(
                                    rexBuilder,
                                    multiply(
                                            rexBuilder,
                                            rexBuilder.makeIntervalLiteral(
                                                    BigDecimal.ONE, qualifier),
                                            op1),
                                    BigDecimal.ONE.divide(
                                            unit.multiplier, RoundingMode.UNNECESSARY));
                    break;
                default:
                    interval2Add =
                            multiply(
                                    rexBuilder,
                                    rexBuilder.makeIntervalLiteral(unit.multiplier, qualifier),
                                    op1);
            }

            return rexBuilder.makeCall(
                    SqlStdOperatorTable.DATETIME_PLUS,
                    cx.convertExpression(call.operand(2)),
                    interval2Add);
        }
    }

    /** Convertlet that handles the {@code TIMESTAMPDIFF} function. */
    private static class TimestampDiffConvertlet implements SqlRexConvertlet {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlLiteral unitLiteral = call.operand(0);
            TimeUnit unit = unitLiteral.getValueAs(TimeUnit.class);
            BigDecimal multiplier = BigDecimal.ONE;
            BigDecimal divider = BigDecimal.ONE;
            SqlTypeName sqlTypeName =
                    unit == TimeUnit.NANOSECOND ? SqlTypeName.BIGINT : SqlTypeName.INTEGER;
            switch (unit) {
                case MICROSECOND:
                case MILLISECOND:
                case NANOSECOND:
                case WEEK:
                    multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND);
                    divider = unit.multiplier;
                    unit = TimeUnit.SECOND;
                    break;
                case QUARTER:
                    divider = unit.multiplier;
                    unit = TimeUnit.MONTH;
                    break;
                default:
                    break;
            }
            final SqlIntervalQualifier qualifier =
                    new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO);
            final RexNode op2 = cx.convertExpression(call.operand(2));
            final RexNode op1 = cx.convertExpression(call.operand(1));
            final RelDataType intervalType =
                    cx.getTypeFactory()
                            .createTypeWithNullability(
                                    cx.getTypeFactory().createSqlIntervalType(qualifier),
                                    op1.getType().isNullable() || op2.getType().isNullable());
            final RexCall rexCall =
                    (RexCall)
                            rexBuilder.makeCall(
                                    intervalType,
                                    SqlStdOperatorTable.MINUS_DATE,
                                    ImmutableList.of(op2, op1));
            final RelDataType intType =
                    cx.getTypeFactory()
                            .createTypeWithNullability(
                                    cx.getTypeFactory().createSqlType(sqlTypeName),
                                    SqlTypeUtil.containsNullable(rexCall.getType()));
            RexNode e = rexBuilder.makeCast(intType, rexCall);
            return rexBuilder.multiplyDivide(e, multiplier, divider);
        }
    }
}
