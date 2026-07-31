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
package org.apache.calcite.rex;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.rex.RexUnknownAs.FALSE;
import static org.apache.calcite.rex.RexUnknownAs.TRUE;
import static org.apache.calcite.rex.RexUnknownAs.UNKNOWN;

/**
 * Context required to simplify a row-expression.
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>CALCITE-7588, CALCITE-7578: Lines 574 ~ 588, 598 ~ 600, 624 ~ 636
 * </ol>
 */
public class RexSimplify {
    private final boolean paranoid;
    public final RexBuilder rexBuilder;
    private final RelOptPredicateList predicates;

    /**
     * How to treat UNKNOWN values, if one of the deprecated {@code simplify} methods without an
     * {@code unknownAs} argument is called.
     */
    final RexUnknownAs defaultUnknownAs;

    final boolean predicateElimination;
    private final RexExecutor executor;

    private static final Strong STRONG = new Strong();

    /**
     * Creates a RexSimplify.
     *
     * @param rexBuilder Rex builder
     * @param predicates Predicates known to hold on input fields
     * @param executor Executor for constant reduction, not null
     */
    public RexSimplify(
            RexBuilder rexBuilder, RelOptPredicateList predicates, RexExecutor executor) {
        this(rexBuilder, predicates, UNKNOWN, true, false, executor);
    }

    /** Internal constructor. */
    private RexSimplify(
            RexBuilder rexBuilder,
            RelOptPredicateList predicates,
            RexUnknownAs defaultUnknownAs,
            boolean predicateElimination,
            boolean paranoid,
            RexExecutor executor) {
        this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder");
        this.predicates = requireNonNull(predicates, "predicates");
        this.defaultUnknownAs = requireNonNull(defaultUnknownAs, "defaultUnknownAs");
        this.predicateElimination = predicateElimination;
        this.paranoid = paranoid;
        this.executor = requireNonNull(executor, "executor");
    }

    @Deprecated // to be removed before 2.0
    public RexSimplify(RexBuilder rexBuilder, boolean unknownAsFalse, RexExecutor executor) {
        this(
                rexBuilder,
                RelOptPredicateList.EMPTY,
                RexUnknownAs.falseIf(unknownAsFalse),
                true,
                false,
                executor);
    }

    @Deprecated // to be removed before 2.0
    public RexSimplify(
            RexBuilder rexBuilder,
            RelOptPredicateList predicates,
            boolean unknownAsFalse,
            RexExecutor executor) {
        this(rexBuilder, predicates, RexUnknownAs.falseIf(unknownAsFalse), true, false, executor);
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Returns a RexSimplify the same as this but with a specified {@link #defaultUnknownAs} value.
     *
     * @deprecated Use methods with a {@link RexUnknownAs} argument, such as {@link
     *     #simplify(RexNode, RexUnknownAs)}.
     */
    @Deprecated // to be removed before 2.0
    public RexSimplify withUnknownAsFalse(boolean unknownAsFalse) {
        final RexUnknownAs defaultUnknownAs = RexUnknownAs.falseIf(unknownAsFalse);
        return defaultUnknownAs == this.defaultUnknownAs
                ? this
                : new RexSimplify(
                        rexBuilder,
                        predicates,
                        defaultUnknownAs,
                        predicateElimination,
                        paranoid,
                        executor);
    }

    /** Returns a RexSimplify the same as this but with a specified {@link #predicates} value. */
    public RexSimplify withPredicates(RelOptPredicateList predicates) {
        return predicates == this.predicates
                ? this
                : new RexSimplify(
                        rexBuilder,
                        predicates,
                        defaultUnknownAs,
                        predicateElimination,
                        paranoid,
                        executor);
    }

    /**
     * Returns a RexSimplify the same as this but which verifies that the expression before and
     * after simplification are equivalent.
     *
     * @see #verify
     */
    public RexSimplify withParanoid(boolean paranoid) {
        return paranoid == this.paranoid
                ? this
                : new RexSimplify(
                        rexBuilder,
                        predicates,
                        defaultUnknownAs,
                        predicateElimination,
                        paranoid,
                        executor);
    }

    /**
     * Returns a RexSimplify the same as this but with a specified {@link #predicateElimination}
     * value.
     *
     * <p>This is introduced temporarily, until {@link Bug#CALCITE_2401_FIXED [CALCITE-2401] is
     * fixed}.
     */
    private RexSimplify withPredicateElimination(boolean predicateElimination) {
        return predicateElimination == this.predicateElimination
                ? this
                : new RexSimplify(
                        rexBuilder,
                        predicates,
                        defaultUnknownAs,
                        predicateElimination,
                        paranoid,
                        executor);
    }

    /**
     * Simplifies a boolean expression, always preserving its type and its nullability.
     *
     * <p>This is useful if you are simplifying expressions in a {@link Project}.
     */
    public RexNode simplifyPreservingType(RexNode e) {
        return simplifyPreservingType(e, defaultUnknownAs, true);
    }

    public RexNode simplifyPreservingType(
            RexNode e, RexUnknownAs unknownAs, boolean matchNullability) {
        final RexNode e2 = simplifyUnknownAs(e, unknownAs);
        if (e2.getType() == e.getType()) {
            return e2;
        }
        if (!matchNullability
                && SqlTypeUtil.equalSansNullability(
                        rexBuilder.typeFactory, e2.getType(), e.getType())) {
            return e2;
        }
        final RexNode e3 = rexBuilder.makeCast(e.getType(), e2, matchNullability, false);
        if (e3.equals(e)) {
            return e;
        }
        return e3;
    }

    /**
     * Simplifies a boolean expression.
     *
     * <p>In particular:
     *
     * <ul>
     *   <li>{@code simplify(x = 1 OR NOT x = 1 OR x IS NULL)} returns {@code TRUE}
     *   <li>{@code simplify(x = 1 AND FALSE)} returns {@code FALSE}
     * </ul>
     *
     * <p>Handles UNKNOWN values using the policy specified when you created this {@code
     * RexSimplify}. Unless you used a deprecated constructor, that policy is {@link
     * RexUnknownAs#UNKNOWN}.
     *
     * <p>If the expression is a predicate in a WHERE clause, consider instead using {@link
     * #simplifyUnknownAsFalse(RexNode)}.
     *
     * @param e Expression to simplify
     */
    public RexNode simplify(RexNode e) {
        return simplifyUnknownAs(e, defaultUnknownAs);
    }

    /**
     * As {@link #simplify(RexNode)}, but for a boolean expression for which a result of UNKNOWN
     * will be treated as FALSE.
     *
     * <p>Use this form for expressions on a WHERE, ON, HAVING or FILTER(WHERE) clause.
     *
     * <p>This may allow certain additional simplifications. A result of UNKNOWN may yield FALSE,
     * however it may still yield UNKNOWN. (If the simplified expression has type BOOLEAN NOT NULL,
     * then of course it can only return FALSE.)
     */
    public final RexNode simplifyUnknownAsFalse(RexNode e) {
        return simplifyUnknownAs(e, FALSE);
    }

    /**
     * As {@link #simplify(RexNode)}, but specifying how UNKNOWN values are to be treated.
     *
     * <p>If UNKNOWN is treated as FALSE, this may allow certain additional simplifications. A
     * result of UNKNOWN may yield FALSE, however it may still yield UNKNOWN. (If the simplified
     * expression has type BOOLEAN NOT NULL, then of course it can only return FALSE.)
     */
    public RexNode simplifyUnknownAs(RexNode e, RexUnknownAs unknownAs) {
        final RexNode simplified = withParanoid(false).simplify(e, unknownAs);
        if (paranoid) {
            verify(e, simplified, unknownAs);
        }
        return simplified;
    }

    /**
     * Internal method to simplify an expression.
     *
     * <p>Unlike the public {@link #simplify(RexNode)} and {@link #simplifyUnknownAsFalse(RexNode)}
     * methods, never calls {@link #verify(RexNode, RexNode, RexUnknownAs)}. Verify adds an overhead
     * that is only acceptable for a top-level call.
     */
    RexNode simplify(RexNode e, RexUnknownAs unknownAs) {
        if (isSafeExpression(e) && STRONG.isNull(e)) {
            // Only boolean NULL (aka UNKNOWN) can be converted to FALSE. Even in
            // unknownAs=FALSE mode, we must not convert a NULL integer (say) to FALSE
            if (e.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                switch (unknownAs) {
                    case FALSE:
                    case TRUE:
                        return rexBuilder.makeLiteral(unknownAs.toBoolean());
                    default:
                        break;
                }
            }
            return rexBuilder.makeNullLiteral(e.getType());
        }
        switch (e.getKind()) {
            case AND:
                return simplifyAnd((RexCall) e, unknownAs);
            case OR:
                return simplifyOr((RexCall) e, unknownAs);
            case NOT:
                return simplifyNot((RexCall) e, unknownAs);
            case CASE:
                return simplifyCase((RexCall) e, unknownAs);
            case COALESCE:
                return simplifyCoalesce((RexCall) e);
            case CAST:
            case SAFE_CAST:
                return simplifyCast((RexCall) e);
            case CEIL:
            case FLOOR:
                return simplifyCeilFloor((RexCall) e);
            case TRIM:
                return simplifyTrim((RexCall) e);
            case IS_NULL:
            case IS_NOT_NULL:
            case IS_TRUE:
            case IS_NOT_TRUE:
            case IS_FALSE:
            case IS_NOT_FALSE:
                assert e instanceof RexCall;
                return simplifyIs((RexCall) e, unknownAs);
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case NOT_EQUALS:
                return simplifyComparison((RexCall) e, unknownAs);
            case SEARCH:
                return simplifySearch((RexCall) e, unknownAs);
            case LIKE:
                return simplifyLike((RexCall) e, unknownAs);
            case MINUS_PREFIX:
            case CHECKED_MINUS_PREFIX:
                return simplifyUnaryMinus((RexCall) e, unknownAs);
            case PLUS_PREFIX:
                return simplifyUnaryPlus((RexCall) e, unknownAs);
            case PLUS:
            case MINUS:
            case TIMES:
            case DIVIDE:
            case CHECKED_PLUS:
            case CHECKED_MINUS:
            case CHECKED_TIMES:
            case CHECKED_DIVIDE:
                return simplifyArithmetic((RexCall) e);
            case M2V:
                return simplifyM2v((RexCall) e);
            default:
                if (e.getClass() == RexCall.class) {
                    return simplifyGenericNode((RexCall) e);
                } else {
                    return e;
                }
        }
    }

    /** Applies NOT to an expression. */
    RexNode not(RexNode e) {
        return RexUtil.not(rexBuilder, e);
    }

    /** Applies IS NOT FALSE to an expression. */
    RexNode isNotFalse(RexNode e) {
        return e.isAlwaysTrue()
                ? rexBuilder.makeLiteral(true)
                : e.isAlwaysFalse()
                        ? rexBuilder.makeLiteral(false)
                        : e.getKind() == SqlKind.NOT
                                ? isNotTrue(((RexCall) e).operands.get(0))
                                : predicates.isEffectivelyNotNull(e)
                                        ? e // would "CAST(e AS BOOLEAN NOT NULL)" better?
                                        : rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, e);
    }

    /** Applies IS NOT TRUE to an expression. */
    RexNode isNotTrue(RexNode e) {
        return e.isAlwaysTrue()
                ? rexBuilder.makeLiteral(false)
                : e.isAlwaysFalse()
                        ? rexBuilder.makeLiteral(true)
                        : e.getKind() == SqlKind.NOT
                                ? isNotFalse(((RexCall) e).operands.get(0))
                                : predicates.isEffectivelyNotNull(e)
                                        ? not(e)
                                        : rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, e);
    }

    /** Applies IS TRUE to an expression. */
    RexNode isTrue(RexNode e) {
        return e.isAlwaysTrue()
                ? rexBuilder.makeLiteral(true)
                : e.isAlwaysFalse()
                        ? rexBuilder.makeLiteral(false)
                        : e.getKind() == SqlKind.NOT
                                ? isFalse(((RexCall) e).operands.get(0))
                                : predicates.isEffectivelyNotNull(e)
                                        ? e // would "CAST(e AS BOOLEAN NOT NULL)" better?
                                        : rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, e);
    }

    /** Applies IS FALSE to an expression. */
    RexNode isFalse(RexNode e) {
        return e.isAlwaysTrue()
                ? rexBuilder.makeLiteral(false)
                : e.isAlwaysFalse()
                        ? rexBuilder.makeLiteral(true)
                        : e.getKind() == SqlKind.NOT
                                ? isTrue(((RexCall) e).operands.get(0))
                                : predicates.isEffectivelyNotNull(e)
                                        ? not(e)
                                        : rexBuilder.makeCall(SqlStdOperatorTable.IS_FALSE, e);
    }

    /** Runs simplification inside a non-specialized node. */
    private RexNode simplifyGenericNode(RexCall e) {
        final List<RexNode> operands = new ArrayList<>(e.operands);
        simplifyList(operands, UNKNOWN);
        if (e.operands.equals(operands)) {
            return e;
        }
        return rexBuilder.makeCall(e.getParserPosition(), e.getType(), e.getOperator(), operands);
    }

    /**
     * Try to find a literal with the given value in the input list. The type of the literal must be
     * one of the numeric types.
     */
    private static int findLiteralIndex(List<RexNode> operands, BigDecimal value) {
        for (int i = 0; i < operands.size(); i++) {
            if (operands.get(i).isA(SqlKind.LITERAL)) {
                Comparable comparable = ((RexLiteral) operands.get(i)).getValue();
                if (comparable instanceof BigDecimal
                        && value.compareTo((BigDecimal) comparable) == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private RexNode simplifyArithmetic(RexCall e) {
        if (e.getType().getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC
                || e.getOperands().stream()
                        .anyMatch(
                                o ->
                                        e.getType().getSqlTypeName().getFamily()
                                                != SqlTypeFamily.NUMERIC)) {
            // we only support simplifying numeric types.
            return simplifyGenericNode(e);
        }

        assert e.getOperands().size() == 2;

        switch (e.getKind()) {
            // These simplifications are safe for both checked and unchecked arithemtic.
            case PLUS:
            case CHECKED_PLUS:
                return simplifyPlus(e);
            case MINUS:
            case CHECKED_MINUS:
                return simplifyMinus(e);
            case TIMES:
            case CHECKED_TIMES:
                return simplifyMultiply(e);
            case DIVIDE:
            case CHECKED_DIVIDE:
                return simplifyDivide(e);
            default:
                throw new IllegalArgumentException(
                        "Unsupported arithmeitc operation " + e.getKind());
        }
    }

    private RexNode simplifyPlus(RexCall e) {
        final int zeroIndex = findLiteralIndex(e.operands, BigDecimal.ZERO);
        if (zeroIndex >= 0) {
            // return the other operand.
            RexNode other = e.getOperands().get((zeroIndex + 1) % 2);
            return other.getType().equals(e.getType())
                    ? other
                    : rexBuilder.makeCast(e.getParserPosition(), e.getType(), other);
        }
        return simplifyGenericNode(e);
    }

    private RexNode simplifyMinus(RexCall e) {
        final int zeroIndex = findLiteralIndex(e.operands, BigDecimal.ZERO);
        if (zeroIndex == 1) {
            RexNode leftOperand = e.getOperands().get(0);
            return leftOperand.getType().equals(e.getType())
                    ? leftOperand
                    : rexBuilder.makeCast(e.getParserPosition(), e.getType(), leftOperand);
        }
        return simplifyGenericNode(e);
    }

    private RexNode simplifyMultiply(RexCall e) {
        final int oneIndex = findLiteralIndex(e.operands, BigDecimal.ONE);
        if (oneIndex >= 0) {
            // return the other operand.
            RexNode other = e.getOperands().get((oneIndex + 1) % 2);
            return other.getType().equals(e.getType())
                    ? other
                    : rexBuilder.makeCast(e.getParserPosition(), e.getType(), other);
        }
        return simplifyGenericNode(e);
    }

    private RexNode simplifyDivide(RexCall e) {
        final int oneIndex = findLiteralIndex(e.operands, BigDecimal.ONE);
        if (oneIndex == 1) {
            RexNode leftOperand = e.getOperands().get(0);
            return leftOperand.getType().equals(e.getType())
                    ? leftOperand
                    : rexBuilder.makeCast(e.getParserPosition(), e.getType(), leftOperand);
        }
        return simplifyGenericNode(e);
    }

    private RexNode simplifyLike(RexCall e, RexUnknownAs unknownAs) {
        if (e.operands.get(1) instanceof RexLiteral) {
            final RexLiteral literal = (RexLiteral) e.operands.get(1);
            String likeStr = requireNonNull(literal.getValueAs(String.class));
            Pattern pattern = Pattern.compile("%+");
            String value = pattern.matcher(likeStr).replaceAll("%");
            if ("%".equals(value)) {
                // "x LIKE '%'" or "x LIKE '%...'" simplifies to "x = x"
                final RexNode x = e.operands.get(0);
                return simplify(
                        rexBuilder.makeCall(
                                e.getParserPosition(), SqlStdOperatorTable.EQUALS, x, x),
                        unknownAs);
            }
            // simplify "x LIKE '%%\%%a%%%'" to "x LIKE '%\%%a%'", default escape is '\'
            if (e.operands.size() == 2) {
                e =
                        (RexCall)
                                rexBuilder.makeCall(
                                        e.getParserPosition(),
                                        e.getOperator(),
                                        e.operands.get(0),
                                        rexBuilder.makeLiteral(
                                                simplifyLikeString(likeStr, '\\', '%')));
            }
            if (e.operands.size() == 3 && e.operands.get(2) instanceof RexLiteral) {
                final RexLiteral escapeLiteral = (RexLiteral) e.operands.get(2);
                // FLINK MODIFICATION BEGIN
                final String escapeStr = requireNonNull(escapeLiteral.getValueAs(String.class));
                if (escapeStr.length() == 1) {
                    char escape = escapeStr.charAt(0);
                    e =
                            (RexCall)
                                    rexBuilder.makeCall(
                                            e.getParserPosition(),
                                            e.getOperator(),
                                            e.operands.get(0),
                                            rexBuilder.makeLiteral(
                                                    simplifyLikeString(likeStr, escape, '%')),
                                            escapeLiteral);
                }
                // FLINK MODIFICATION END
            }
        }
        return simplifyGenericNode(e);
    }

    // string 'AA%%__%%AA' simplify to 'AA__%AA'
    // string with even escapes 'AA\\\\%%__%%AA' simplify to 'AA\\__%AA'
    // string with odd escapes 'AA\\\\\\%%__%%AA' simplify to 'AA\\\\\\%__%AA'
    private String simplifyMixedWildcards(String str, char escape) {
        // FLINK MODIFICATION BEGIN
        Pattern pattern = getMixedWildCardPattern(escape);
        // FLINK MODIFICATION END
        Matcher matcher = pattern.matcher(str);
        StringBuilder builder = new StringBuilder();
        int from = 0;
        while (matcher.find()) {
            int start = matcher.start();
            String group = requireNonNull(matcher.group(0));
            if (start > 0
                    && str.charAt(start - 1) == escape
                    && consecutiveSameCharCountBefore(str, start - 1, escape) % 2 == 1) {
                builder.append(str, from, start + 1);
                builder.append(simplifyPercentAndUnderline(group.substring(1)));
            } else {
                builder.append(str, from, start);
                builder.append(simplifyPercentAndUnderline(group));
            }
            from = matcher.end();
        }
        if (from < str.length()) {
            builder.append(str.substring(from));
        }
        return builder.toString();
    }

    // FLINK MODIFICATION BEGIN
    private static Pattern getMixedWildCardPattern(char escape) {
        switch (escape) {
            case '%':
                return Pattern.compile("_+");
            case '_':
                return Pattern.compile("%+");
            default:
                return Pattern.compile("[_%]+");
        }
    }

    // FLINK MODIFICATION END

    // Tool method: count the number of consecutive identical characters before index
    private int consecutiveSameCharCountBefore(String str, int index, char escape) {
        int count = 0;
        while (index >= 0) {
            if (str.charAt(index) != escape) {
                break;
            }
            count++;
            index--;
        }
        return count;
    }

    // Tool method: simplified string mixed with '%' and '_'
    private String simplifyPercentAndUnderline(String str) {
        StringBuilder builder = new StringBuilder();
        boolean containsPercent = false;
        for (int index = 0; index < str.length(); index++) {
            if (str.charAt(index) == '%') {
                containsPercent = true;
                continue;
            }
            if (str.charAt(index) == '_') {
                builder.append('_');
            }
        }
        if (containsPercent) {
            builder.append('%');
        }
        return builder.toString();
    }

    /**
     * Simplifies like string with escape. A like '%%#%%A%%' escape '#' should simplify to A like
     * '%#%%A%' escape '#'.
     */
    private String simplifyLikeString(String content, char escape, char wildcard) {
        int escapeCount = 0;
        int wildcardCount = 0;
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < content.length(); index++) {
            char c = content.charAt(index);
            if (c == escape) {
                builder.append(c);
                escapeCount++;
                wildcardCount = 0;
                continue;
            }
            if (c == wildcard) {
                if (escapeCount % 2 == 1) {
                    builder.append(wildcard);
                } else if (wildcardCount == 0) {
                    builder.append(wildcard);
                    wildcardCount++;
                }
                escapeCount = 0;
                continue;
            }
            builder.append(c);
            escapeCount = 0;
            wildcardCount = 0;
        }
        return simplifyMixedWildcards(builder.toString(), escape);
    }

    // e must be a comparison (=, >, >=, <, <=, !=)
    private RexNode simplifyComparison(RexCall e, RexUnknownAs unknownAs) {
        //noinspection unchecked
        return simplifyComparison(e, unknownAs, Comparable.class);
    }

    // e must be a comparison (=, >, >=, <, <=, !=)
    private <C extends Comparable<C>> RexNode simplifyComparison(
            RexCall e, RexUnknownAs unknownAs, Class<C> clazz) {
        final List<RexNode> operands = new ArrayList<>(e.operands);
        // UNKNOWN mode is warranted: false = null
        simplifyList(operands, UNKNOWN);

        // Simplify "x <op> x"
        final RexNode o0 = operands.get(0);
        final RexNode o1 = operands.get(1);
        if (o0.equals(o1) && RexUtil.isDeterministic(o0)) {
            RexNode newExpr;
            switch (e.getKind()) {
                case EQUALS:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                    // "x = x" simplifies to "null or x is not null" (similarly <= and >=)
                    newExpr =
                            rexBuilder.makeCall(
                                    e.getParserPosition(),
                                    SqlStdOperatorTable.OR,
                                    rexBuilder.makeNullLiteral(e.getType()),
                                    rexBuilder.makeCall(
                                            e.getParserPosition(),
                                            SqlStdOperatorTable.IS_NOT_NULL,
                                            o0));
                    return simplify(newExpr, unknownAs);
                case NOT_EQUALS:
                case LESS_THAN:
                case GREATER_THAN:
                    // "x != x" simplifies to "null and x is null" (similarly < and >)
                    newExpr =
                            rexBuilder.makeCall(
                                    e.getParserPosition(),
                                    SqlStdOperatorTable.AND,
                                    rexBuilder.makeNullLiteral(e.getType()),
                                    rexBuilder.makeCall(
                                            e.getParserPosition(),
                                            SqlStdOperatorTable.IS_NULL,
                                            o0));
                    return simplify(newExpr, unknownAs);
                default:
                    // unknown kind
            }
        }

        if (o0.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            Comparison cmp =
                    Comparison.of(
                            rexBuilder.makeCall(e.getParserPosition(), e.getOperator(), o0, o1),
                            node -> true);
            if (cmp != null) {
                if (cmp.literal.isAlwaysTrue()) {
                    switch (cmp.kind) {
                        case GREATER_THAN_OR_EQUAL:
                        case EQUALS: // x=true
                            return cmp.ref;
                        case LESS_THAN:
                        case NOT_EQUALS: // x!=true
                            return simplify(not(cmp.ref), unknownAs);
                        case GREATER_THAN:
                            /* this is false, but could be null if x is null */
                            if (!cmp.ref.getType().isNullable()) {
                                return rexBuilder.makeLiteral(false);
                            }
                            break;
                        case LESS_THAN_OR_EQUAL:
                            /* this is true, but could be null if x is null */
                            if (!cmp.ref.getType().isNullable()) {
                                return rexBuilder.makeLiteral(true);
                            }
                            break;
                        default:
                            break;
                    }
                }
                if (cmp.literal.isAlwaysFalse()) {
                    switch (cmp.kind) {
                        case EQUALS:
                        case LESS_THAN_OR_EQUAL:
                            return simplify(not(cmp.ref), unknownAs);
                        case NOT_EQUALS:
                        case GREATER_THAN:
                            return cmp.ref;
                        case GREATER_THAN_OR_EQUAL:
                            /* this is true, but could be null if x is null */
                            if (!cmp.ref.getType().isNullable()) {
                                return rexBuilder.makeLiteral(true);
                            }
                            break;
                        case LESS_THAN:
                            /* this is false, but could be null if x is null */
                            if (!cmp.ref.getType().isNullable()) {
                                return rexBuilder.makeLiteral(false);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // Simplify "<literal1> <op> <literal2>"
        // For example, "1 = 2" becomes FALSE;
        // "1 != 1" becomes FALSE;
        // "1 != NULL" becomes UNKNOWN (or FALSE if unknownAsFalse);
        // "1 != '1'" is unchanged because the types are not the same.
        if (o0.isA(SqlKind.LITERAL)
                && o1.isA(SqlKind.LITERAL)
                && SqlTypeUtil.equalSansNullability(
                        rexBuilder.getTypeFactory(), o0.getType(), o1.getType())) {
            final C v0 = ((RexLiteral) o0).getValueAs(clazz);
            final C v1 = ((RexLiteral) o1).getValueAs(clazz);
            if (v0 == null || v1 == null) {
                return unknownAs == FALSE
                        ? rexBuilder.makeLiteral(false)
                        : rexBuilder.makeNullLiteral(e.getType());
            }
            final int comparisonResult = v0.compareTo(v1);
            switch (e.getKind()) {
                case EQUALS:
                    return rexBuilder.makeLiteral(comparisonResult == 0);
                case GREATER_THAN:
                    return rexBuilder.makeLiteral(comparisonResult > 0);
                case GREATER_THAN_OR_EQUAL:
                    return rexBuilder.makeLiteral(comparisonResult >= 0);
                case LESS_THAN:
                    return rexBuilder.makeLiteral(comparisonResult < 0);
                case LESS_THAN_OR_EQUAL:
                    return rexBuilder.makeLiteral(comparisonResult <= 0);
                case NOT_EQUALS:
                    return rexBuilder.makeLiteral(comparisonResult != 0);
                default:
                    throw new AssertionError();
            }
        }

        RexNode node = simplifyComparisonWithNull(e, unknownAs);
        if (node instanceof RexLiteral) {
            return node;
        }

        // If none of the arguments were simplified, return the call unchanged.
        final RexNode e2;
        if (operands.equals(e.operands)) {
            e2 = e;
        } else {
            e2 = rexBuilder.makeCall(e.getParserPosition(), e.op, operands);
        }
        return simplifyUsingPredicates(e2, clazz);
    }

    /**
     * If this RexNode is a comparison against NULL, return FALSE, otherwise return it unchanged.
     */
    static RexNode simplifyComparisonWithNull(
            RexNode e, RexBuilder rexBuilder, RexUnknownAs unknownAs) {
        final RexSimplify.Comparison comparison = RexSimplify.Comparison.of(e);
        if (comparison != null) {
            boolean againstNull = comparison.literal.isNull();
            // There is another possibility to check: in a comparison like 1 = null,
            // the "non-literal" side of the Comparison can be null
            if (comparison.ref instanceof RexLiteral) {
                againstNull = againstNull || ((RexLiteral) comparison.ref).isNull();
            }
            if (againstNull) {
                return unknownAs == FALSE
                        ? rexBuilder.makeLiteral(false)
                        : rexBuilder.makeNullLiteral(e.getType());
            }
        }
        return e;
    }

    public static RexNode simplifyComparisonWithNull(RexNode e, RexBuilder rexBuilder) {
        return RexSimplify.simplifyComparisonWithNull(e, rexBuilder, FALSE);
    }

    /**
     * If this RexNode is a comparison against NULL, return a simplified form, otherwise return it
     * unchanged.
     */
    public RexNode simplifyComparisonWithNull(RexNode e, RexUnknownAs unknownAs) {
        return simplifyComparisonWithNull(e, this.rexBuilder, unknownAs);
    }

    /** Simplifies a conjunction of boolean expressions. */
    @Deprecated // to be removed before 2.0
    public RexNode simplifyAnds(Iterable<? extends RexNode> nodes) {
        ensureParanoidOff();
        return simplifyAnds(nodes, defaultUnknownAs);
    }

    // package-protected only for a deprecated method; treat as private
    RexNode simplifyAnds(Iterable<? extends RexNode> nodes, RexUnknownAs unknownAs) {
        final List<RexNode> terms = new ArrayList<>();
        final List<RexNode> notTerms = new ArrayList<>();
        for (RexNode e : nodes) {
            RelOptUtil.decomposeConjunction(e, terms, notTerms);
        }
        simplifyList(terms, UNKNOWN);
        simplifyList(notTerms, UNKNOWN);
        if (unknownAs == FALSE) {
            return simplifyAnd2ForUnknownAsFalse(terms, notTerms);
        }
        return simplifyAnd2(terms, notTerms);
    }

    private void simplifyList(List<RexNode> terms, RexUnknownAs unknownAs) {
        terms.replaceAll(e -> simplify(e, unknownAs));
    }

    private void simplifyAndTerms(List<RexNode> terms, RexUnknownAs unknownAs) {
        RexSimplify simplify = this;
        for (int i = 0; i < terms.size(); i++) {
            RexNode t = terms.get(i);
            if (Predicate.of(t) == null) {
                continue;
            }
            terms.set(i, simplify.simplify(t, unknownAs));
            RelOptPredicateList newPredicates =
                    simplify.predicates.union(
                            rexBuilder,
                            RelOptPredicateList.of(rexBuilder, terms.subList(i, i + 1)));
            simplify = simplify.withPredicates(newPredicates);
        }
        for (int i = 0; i < terms.size(); i++) {
            RexNode t = terms.get(i);
            if (Predicate.of(t) != null) {
                continue;
            }
            terms.set(i, simplify.simplify(t, unknownAs));
        }
    }

    private void simplifyOrTerms(List<RexNode> terms, RexUnknownAs unknownAs) {
        // Suppose we are processing "e1(x) OR e2(x) OR e3(x)". When we are
        // visiting "e3(x)" we know both "e1(x)" and "e2(x)" are not true (they
        // may be unknown), because if either of them were true we would have
        // stopped.
        RexSimplify simplify = this;

        // 'doneTerms' prevents us from visiting a term in both first and second
        // loops. If we did this, the second visit would have a predicate saying
        // that 'term' is false. Effectively, we sort terms: visiting
        // 'allowedAsPredicate' terms in the first loop, and
        // non-'allowedAsPredicate' in the second. Each term is visited once.
        final BitSet doneTerms = new BitSet();
        for (int i = 0; i < terms.size(); i++) {
            final RexNode t = terms.get(i);
            if (!simplify.allowedAsPredicateDuringOrSimplification(t)) {
                continue;
            }
            doneTerms.set(i);
            final RexNode t2 = simplify.simplify(t, unknownAs);
            terms.set(i, t2);
            final RexNode inverse = simplify.simplify(isNotTrue(t2), RexUnknownAs.UNKNOWN);
            final RelOptPredicateList newPredicates =
                    simplify.predicates.union(
                            rexBuilder,
                            RelOptPredicateList.of(rexBuilder, ImmutableList.of(inverse)));
            simplify = simplify.withPredicates(newPredicates);
        }
        for (int i = 0; i < terms.size(); i++) {
            final RexNode t = terms.get(i);
            if (doneTerms.get(i)) {
                continue; // we visited this term in the first loop
            }
            terms.set(i, simplify.simplify(t, unknownAs));
        }
    }

    /**
     * Decides whether the given node could be used as a predicate during the simplification of
     * other OR operands.
     */
    private boolean allowedAsPredicateDuringOrSimplification(final RexNode t) {
        Predicate predicate = Predicate.of(t);
        return predicate != null && predicate.allowedInOr(predicates);
    }

    private RexNode simplifyNot(RexCall call, RexUnknownAs unknownAs) {
        final RexNode a = call.getOperands().get(0);
        final List<RexNode> newOperands;
        switch (a.getKind()) {
            case NOT:
                // NOT NOT x ==> x
                return simplify(((RexCall) a).getOperands().get(0), unknownAs);

            case SEARCH:
                // NOT SEARCH(x, Sarg[(-inf, 10) OR NULL) ==> SEARCH(x, Sarg[[10, +inf)])
                final RexCall call2 = (RexCall) a;
                final RexNode ref = call2.operands.get(0);
                final RexLiteral literal = (RexLiteral) call2.operands.get(1);
                final Sarg sarg = literal.getValueAs(Sarg.class);
                return simplifySearch(
                        call2.clone(
                                call2.type,
                                ImmutableList.of(
                                        ref,
                                        rexBuilder.makeLiteral(
                                                requireNonNull(sarg, "sarg").negate(),
                                                literal.getType(),
                                                literal.getTypeName()))),
                        unknownAs.negate());

            case LITERAL:
                if (a.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
                        && !RexLiteral.isNullLiteral(a)) {
                    return rexBuilder.makeLiteral(!RexLiteral.booleanValue(a));
                }
                break;

            case AND:
                // NOT distributivity for AND
                newOperands = new ArrayList<>();
                for (RexNode operand : ((RexCall) a).getOperands()) {
                    newOperands.add(simplify(not(operand), unknownAs));
                }
                return simplify(
                        rexBuilder.makeCall(
                                call.getParserPosition(), SqlStdOperatorTable.OR, newOperands),
                        unknownAs);

            case OR:
                // NOT distributivity for OR
                newOperands = new ArrayList<>();
                for (RexNode operand : ((RexCall) a).getOperands()) {
                    newOperands.add(simplify(not(operand), unknownAs));
                }
                return simplify(
                        rexBuilder.makeCall(
                                call.getParserPosition(), SqlStdOperatorTable.AND, newOperands),
                        unknownAs);

            case CASE:
                newOperands = new ArrayList<>();
                List<RexNode> operands = ((RexCall) a).getOperands();
                for (int i = 0; i < operands.size(); i += 2) {
                    if (i + 1 == operands.size()) {
                        newOperands.add(not(operands.get(i)));
                    } else {
                        newOperands.add(operands.get(i));
                        newOperands.add(not(operands.get(i + 1)));
                    }
                }
                return simplify(
                        rexBuilder.makeCall(
                                call.getParserPosition(), SqlStdOperatorTable.CASE, newOperands),
                        unknownAs);

            case IN:
            case NOT_IN:
                // do not try to negate
                break;

            default:
                final SqlKind negateKind = a.getKind().negate();
                if (a.getKind() != negateKind) {
                    return simplify(
                            rexBuilder.makeCall(
                                    call.getParserPosition(),
                                    RexUtil.op(negateKind),
                                    ((RexCall) a).getOperands()),
                            unknownAs);
                }
                final SqlKind negateKind2 = a.getKind().negateNullSafe();
                if (a.getKind() != negateKind2) {
                    return simplify(
                            rexBuilder.makeCall(
                                    call.getParserPosition(),
                                    RexUtil.op(negateKind2),
                                    ((RexCall) a).getOperands()),
                            unknownAs);
                }
        }

        RexNode a2 = simplify(a, unknownAs.negate());
        if (a == a2) {
            return call;
        }
        return not(a2);
    }

    private RexNode simplifyUnaryMinus(RexCall call, RexUnknownAs unknownAs) {
        final RexNode a = call.getOperands().get(0);
        if (a.getKind() == SqlKind.MINUS_PREFIX) {
            // -(-(x)) ==> x
            return simplify(((RexCall) a).getOperands().get(0), unknownAs);
        }
        return simplifyGenericNode(call);
    }

    private RexNode simplifyUnaryPlus(RexCall call, RexUnknownAs unknownAs) {
        return simplify(call.getOperands().get(0), unknownAs);
    }

    private RexNode simplifyIs(RexCall call, RexUnknownAs unknownAs) {
        final SqlKind kind = call.getKind();
        final RexNode a = call.getOperands().get(0);
        final RexNode simplified = simplifyIs1(kind, a, unknownAs);
        return simplified == null ? call : simplified;
    }

    private @Nullable RexNode simplifyIs1(SqlKind kind, RexNode a, RexUnknownAs unknownAs) {
        // UnknownAs.FALSE corresponds to x IS TRUE evaluation
        // UnknownAs.TRUE to x IS NOT FALSE
        // Note that both UnknownAs.TRUE and UnknownAs.FALSE only changes the meaning of Unknown
        // (1) if we are already in UnknownAs.FALSE mode; x IS TRUE can be simplified to x
        // (2) similarly in UnknownAs.TRUE mode; x IS NOT FALSE can be simplified to x
        // (3) x IS FALSE could be rewritten to (NOT x) IS TRUE and from there the 1. rule applies
        // (4) x IS NOT TRUE can be rewritten to (NOT x) IS NOT FALSE and from there the 2. rule
        // applies
        if (kind == SqlKind.IS_TRUE && unknownAs == RexUnknownAs.FALSE) {
            return simplify(a, unknownAs);
        }
        if (kind == SqlKind.IS_FALSE && unknownAs == RexUnknownAs.FALSE) {
            return simplify(not(a), unknownAs);
        }
        if (kind == SqlKind.IS_NOT_FALSE && unknownAs == RexUnknownAs.TRUE) {
            return simplify(a, unknownAs);
        }
        if (kind == SqlKind.IS_NOT_TRUE && unknownAs == RexUnknownAs.TRUE) {
            return simplify(not(a), unknownAs);
        }
        final RexNode pred = simplifyIsPredicate(kind, a);
        if (pred != null) {
            return pred;
        }

        return simplifyIs2(kind, a, unknownAs);
    }

    private @Nullable RexNode simplifyIsPredicate(SqlKind kind, RexNode a) {
        if (!(RexUtil.isReferenceOrAccess(a, true) || RexUtil.isDeterministic(a))) {
            return null;
        }

        for (RexNode p : predicates.pulledUpPredicates) {
            IsPredicate pred = IsPredicate.of(p);
            if (pred == null || !a.equals(pred.ref)) {
                continue;
            }
            if (kind == pred.kind) {
                return rexBuilder.makeLiteral(true);
            }
        }
        return null;
    }

    private @Nullable RexNode simplifyIs2(SqlKind kind, RexNode a, RexUnknownAs unknownAs) {
        final RexNode simplified;
        switch (kind) {
            case IS_NULL:
                // x IS NULL ==> FALSE (if x is not nullable)
                validateStrongPolicy(a);
                simplified = simplifyIsNull(a);
                if (simplified != null) {
                    return simplified;
                }
                break;
            case IS_NOT_NULL:
                // x IS NOT NULL ==> TRUE (if x is not nullable)
                validateStrongPolicy(a);
                simplified = simplifyIsNotNull(a);
                if (simplified != null) {
                    return simplified;
                }
                break;

            case IS_TRUE:
                // x IS TRUE ==> x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(a, unknownAs);
                }
                simplified = simplify(a, RexUnknownAs.FALSE);
                if (simplified == a) {
                    return null;
                }
                return isTrue(simplified);

            case IS_NOT_FALSE:
                // x IS NOT FALSE ==> x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(a, unknownAs);
                }
                simplified = simplify(a, RexUnknownAs.TRUE);
                if (simplified == a) {
                    return null;
                }
                return isNotFalse(simplified);

            case IS_FALSE:
            case IS_NOT_TRUE:
                // x IS NOT TRUE ==> NOT x (if x is not nullable)
                // x IS FALSE ==> NOT x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(not(a), unknownAs);
                }
                break;

            default:
                break;
        }
        switch (a.getKind()) {
            case NOT:
                // (NOT x) IS TRUE ==> x IS FALSE
                // Similarly for IS NOT TRUE, IS FALSE, etc.
                //
                // Note that
                //   (NOT x) IS TRUE !=> x IS FALSE
                // because of null values.
                final SqlOperator notKind = RexUtil.op(kind.negateNullSafe());
                final RexNode arg = ((RexCall) a).operands.get(0);
                return simplify(rexBuilder.makeCall(notKind, arg), UNKNOWN);
            default:
                break;
        }
        final RexNode a2 = simplify(a, UNKNOWN);
        if (a != a2) {
            return rexBuilder.makeCall(RexUtil.op(kind), ImmutableList.of(a2));
        }
        return null; // cannot be simplified
    }

    private @Nullable RexNode simplifyIsNotNull(RexNode a) {
        // Simplify the argument first,
        // call ourselves recursively to see whether we can make more progress.
        // For example, given
        // "(CASE WHEN FALSE THEN 1 ELSE 2) IS NOT NULL" we first simplify the
        // argument to "2", and only then we can simplify "2 IS NOT NULL" to "TRUE".
        a = simplify(a, UNKNOWN);
        if (!a.getType().isNullable() && isSafeExpression(a)) {
            return rexBuilder.makeLiteral(true);
        }
        if (RexUtil.isLosslessCast(a)) {
            if (!a.getType().isNullable()) {
                return rexBuilder.makeLiteral(true);
            }
            return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, RexUtil.removeCast(a));
        }
        if (predicates.pulledUpPredicates.contains(a)) {
            return rexBuilder.makeLiteral(true);
        }
        if (hasCustomNullabilityRules(a.getKind())) {
            return null;
        }
        switch (Strong.policy(a)) {
            case NOT_NULL:
                return rexBuilder.makeLiteral(true);
            case ANY:
                // "f" is a strong operator, so "f(operand0, operand1) IS NOT NULL"
                // simplifies to "operand0 IS NOT NULL AND operand1 IS NOT NULL"
                final List<RexNode> operands = new ArrayList<>();
                for (RexNode operand : ((RexCall) a).getOperands()) {
                    final RexNode simplified = simplifyIsNotNull(operand);
                    if (simplified == null) {
                        operands.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
                    } else if (simplified.isAlwaysFalse()) {
                        return rexBuilder.makeLiteral(false);
                    } else {
                        operands.add(simplified);
                    }
                }
                return RexUtil.composeConjunction(rexBuilder, operands);
            case CUSTOM:
                switch (a.getKind()) {
                    case LITERAL:
                        return rexBuilder.makeLiteral(!((RexLiteral) a).isNull());
                    default:
                        throw new AssertionError(
                                "every CUSTOM policy needs a handler, " + a.getKind());
                }
            case AS_IS:
            default:
                return null;
        }
    }

    private @Nullable RexNode simplifyIsNull(RexNode a) {
        // Simplify the argument first,
        // call ourselves recursively to see whether we can make more progress.
        // For example, given
        // "(CASE WHEN FALSE THEN 1 ELSE 2) IS NULL" we first simplify the
        // argument to "2", and only then we can simplify "2 IS NULL" to "FALSE".
        a = simplify(a, UNKNOWN);
        if (!a.getType().isNullable() && isSafeExpression(a)) {
            return rexBuilder.makeLiteral(false);
        }
        if (RexUtil.isLosslessCast(a)) {
            if (!a.getType().isNullable()) {
                return rexBuilder.makeLiteral(false);
            }
            return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, RexUtil.removeCast(a));
        }
        if (RexUtil.isNull(a)) {
            return rexBuilder.makeLiteral(true);
        }
        if (hasCustomNullabilityRules(a.getKind())) {
            return null;
        }
        switch (Strong.policy(a)) {
            case NOT_NULL:
                return rexBuilder.makeLiteral(false);
            case ANY:
                // "f" is a strong operator, so "f(operand0, operand1) IS NULL" simplifies
                // to "operand0 IS NULL OR operand1 IS NULL"
                final List<RexNode> operands = new ArrayList<>();
                for (RexNode operand : ((RexCall) a).getOperands()) {
                    final RexNode simplified = simplifyIsNull(operand);
                    if (simplified == null) {
                        operands.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand));
                    } else {
                        operands.add(simplified);
                    }
                }
                return RexUtil.composeDisjunction(rexBuilder, operands, false);
            case AS_IS:
            default:
                return null;
        }
    }

    /**
     * Validates strong policy for specified {@link RexNode}.
     *
     * @param rexNode Rex node to validate the strong policy
     * @throws AssertionError If the validation fails
     */
    private static void validateStrongPolicy(RexNode rexNode) {
        if (hasCustomNullabilityRules(rexNode.getKind())) {
            return;
        }
        switch (Strong.policy(rexNode)) {
            case NOT_NULL:
                assert !rexNode.getType().isNullable();
                break;
            case ANY:
                List<RexNode> operands = ((RexCall) rexNode).getOperands();
                if (rexNode.getType().isNullable()) {
                    assert operands.stream()
                            .map(RexNode::getType)
                            .anyMatch(RelDataType::isNullable);
                } else {
                    assert operands.stream()
                            .map(RexNode::getType)
                            .noneMatch(RelDataType::isNullable);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Returns {@code true} if specified {@link SqlKind} has custom nullability rules which depend
     * not only on the nullability of input operands.
     *
     * <p>For example, CAST may be used to change the nullability of its operand type, so it may be
     * nullable, though the argument type was non-nullable.
     *
     * @param sqlKind Sql kind to check
     * @return {@code true} if specified {@link SqlKind} has custom nullability rules
     */
    private static boolean hasCustomNullabilityRules(SqlKind sqlKind) {
        switch (sqlKind) {
            case CAST:
            case ITEM:
                return true;
            default:
                return false;
        }
    }

    private RexNode simplifyCoalesce(RexCall call) {
        final Set<RexNode> operandSet = new HashSet<>();
        final List<RexNode> operands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            operand = simplify(operand, UNKNOWN);
            if (!RexUtil.isNull(operand) && operandSet.add(operand)) {
                operands.add(operand);
            }
            if (!operand.getType().isNullable()) {
                break;
            }
        }
        switch (operands.size()) {
            case 0:
                return rexBuilder.makeNullLiteral(call.type);
            case 1:
                return operands.get(0);
            default:
                if (operands.equals(call.operands)) {
                    return call;
                }
                return call.clone(call.type, operands);
        }
    }

    private RexNode simplifyCase(RexCall call, RexUnknownAs unknownAs) {
        List<CaseBranch> inputBranches =
                CaseBranch.fromCaseOperands(rexBuilder, new ArrayList<>(call.getOperands()));

        // run simplification on all operands
        RexSimplify condSimplifier = this.withPredicates(RelOptPredicateList.EMPTY);
        RexSimplify valueSimplifier = this;
        RelDataType caseType = call.getType();

        boolean conditionNeedsSimplify = false;
        CaseBranch lastBranch = null;
        List<CaseBranch> branches = new ArrayList<>();
        for (CaseBranch inputBranch : inputBranches) {
            // simplify the condition
            RexNode newCond = condSimplifier.simplify(inputBranch.cond, RexUnknownAs.FALSE);
            if (newCond.isAlwaysFalse()) {
                // If the condition is false, we do not need to add it
                continue;
            }

            // simplify the value
            RexNode newValue = valueSimplifier.simplify(inputBranch.value, unknownAs);

            // create new branch
            if (lastBranch != null) {
                if (lastBranch.value.equals(newValue) && isSafeExpression(newCond)) {
                    // in this case, last branch and new branch have the same conclusion,
                    // hence we create a new composite condition and we do not add it to
                    // the final branches for the time being
                    newCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, lastBranch.cond, newCond);
                    conditionNeedsSimplify = true;
                } else {
                    // if we reach here, the new branch is not mergeable with the last one,
                    // hence we are going to add the last branch to the final branches.
                    // if the last branch was merged, then we will simplify it first.
                    // otherwise, we just add it
                    CaseBranch branch =
                            generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch);
                    if (!branch.cond.isAlwaysFalse()) {
                        // If the condition is not false, we add it to the final result
                        branches.add(branch);
                        if (branch.cond.isAlwaysTrue()) {
                            // If the condition is always true, we are done
                            lastBranch = null;
                            break;
                        }
                    }
                    conditionNeedsSimplify = false;
                }
            }
            lastBranch = new CaseBranch(newCond, newValue);

            if (newCond.isAlwaysTrue()) {
                // If the condition is always true, we are done (useful in first loop iteration)
                break;
            }
        }
        if (lastBranch != null) {
            // we need to add the last pending branch once we have finished
            // with the for loop
            CaseBranch branch = generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch);
            if (!branch.cond.isAlwaysFalse()) {
                branches.add(branch);
            }
        }

        if (branches.size() == 1) {
            // we can just return the value in this case (matching the case type)
            final RexNode value = branches.get(0).value;
            if (sameTypeOrNarrowsNullability(caseType, value.getType())) {
                return value;
            } else {
                return rexBuilder.makeAbstractCast(
                        call.getParserPosition(), caseType, value, false);
            }
        }

        if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            final RexNode result = simplifyBooleanCase(rexBuilder, branches, unknownAs, caseType);
            if (result != null) {
                if (sameTypeOrNarrowsNullability(caseType, result.getType())) {
                    return simplify(result, unknownAs);
                } else {
                    // If the simplification would widen the nullability
                    RexNode simplified = simplify(result, UNKNOWN);
                    if (!simplified.getType().isNullable()) {
                        return simplified;
                    } else {
                        return rexBuilder.makeCast(
                                call.getParserPosition(), call.getType(), simplified);
                    }
                }
            }
        }
        List<RexNode> newOperands = CaseBranch.toCaseOperands(branches);
        if (newOperands.equals(call.getOperands())) {
            return call;
        }
        return rexBuilder.makeCall(call.getParserPosition(), SqlStdOperatorTable.CASE, newOperands);
    }

    /**
     * If boolean is true, simplify cond in input branch and return new branch. Otherwise, simply
     * return input branch.
     */
    private static CaseBranch generateBranch(
            boolean simplifyCond, RexSimplify simplifier, CaseBranch branch) {
        if (simplifyCond) {
            // the previous branch was merged, time to simplify it and
            // add it to the final result
            return new CaseBranch(
                    simplifier.simplify(branch.cond, RexUnknownAs.FALSE), branch.value);
        }
        return branch;
    }

    /** Return if the new type is the same and at most narrows the nullability. */
    private boolean sameTypeOrNarrowsNullability(RelDataType oldType, RelDataType newType) {
        return oldType.equals(newType)
                || (SqlTypeUtil.equalSansNullability(rexBuilder.typeFactory, oldType, newType)
                        && oldType.isNullable());
    }

    /** Object to describe a CASE branch. */
    static final class CaseBranch {

        private final RexNode cond;
        private final RexNode value;

        CaseBranch(RexNode cond, RexNode value) {
            this.cond = cond;
            this.value = value;
        }

        @Override
        public String toString() {
            return cond + " => " + value;
        }

        /** Given "CASE WHEN p1 THEN v1 ... ELSE e END" returns [(p1, v1), ..., (true, e)]. */
        private static List<CaseBranch> fromCaseOperands(
                RexBuilder rexBuilder, List<RexNode> operands) {
            List<CaseBranch> ret = new ArrayList<>();
            for (int i = 0; i < operands.size() - 1; i += 2) {
                ret.add(new CaseBranch(operands.get(i), operands.get(i + 1)));
            }
            ret.add(new CaseBranch(rexBuilder.makeLiteral(true), Util.last(operands)));
            return ret;
        }

        private static List<RexNode> toCaseOperands(List<CaseBranch> branches) {
            List<RexNode> ret = new ArrayList<>();
            for (int i = 0; i < branches.size() - 1; i++) {
                CaseBranch branch = branches.get(i);
                ret.add(branch.cond);
                ret.add(branch.value);
            }
            CaseBranch lastBranch = Util.last(branches);
            assert lastBranch.cond.isAlwaysTrue();
            ret.add(lastBranch.value);
            return ret;
        }
    }

    /** Decides whether it is safe to flatten the given CASE part into ANDs/ORs. */
    enum SafeRexVisitor implements RexVisitor<Boolean> {
        INSTANCE;

        @SuppressWarnings("ImmutableEnumChecker")
        private final Set<SqlKind> safeOps;

        @SuppressWarnings("ImmutableEnumChecker")
        private final ImmutableSet<SqlOperator> safeOperators;

        SafeRexVisitor() {
            ImmutableSet.Builder<SqlOperator> builder = ImmutableSet.builder();
            builder.addAll(SqlStdOperatorTable.QUANTIFY_OPERATORS);
            safeOperators = builder.build();

            Set<SqlKind> safeOps = EnumSet.noneOf(SqlKind.class);
            safeOps.addAll(SqlKind.COMPARISON);
            safeOps.add(SqlKind.ARRAY_VALUE_CONSTRUCTOR);
            safeOps.add(SqlKind.PLUS_PREFIX);
            safeOps.add(SqlKind.MINUS_PREFIX);
            safeOps.add(SqlKind.CHECKED_MINUS_PREFIX);
            safeOps.add(SqlKind.PLUS);
            safeOps.add(SqlKind.MINUS);
            safeOps.add(SqlKind.TIMES);
            safeOps.add(SqlKind.CHECKED_PLUS);
            safeOps.add(SqlKind.CHECKED_MINUS);
            safeOps.add(SqlKind.CHECKED_TIMES);
            safeOps.add(SqlKind.IS_FALSE);
            safeOps.add(SqlKind.IS_NOT_FALSE);
            safeOps.add(SqlKind.IS_TRUE);
            safeOps.add(SqlKind.IS_NOT_TRUE);
            safeOps.add(SqlKind.IS_NULL);
            safeOps.add(SqlKind.IS_NOT_NULL);
            safeOps.add(SqlKind.IS_DISTINCT_FROM);
            safeOps.add(SqlKind.IS_NOT_DISTINCT_FROM);
            safeOps.add(SqlKind.IN);
            safeOps.add(SqlKind.SEARCH);
            safeOps.add(SqlKind.OR);
            safeOps.add(SqlKind.AND);
            safeOps.add(SqlKind.NOT);
            safeOps.add(SqlKind.CASE);
            safeOps.add(SqlKind.LIKE);
            safeOps.add(SqlKind.COALESCE);
            safeOps.add(SqlKind.TRIM);
            safeOps.add(SqlKind.LTRIM);
            safeOps.add(SqlKind.RTRIM);
            safeOps.add(SqlKind.BETWEEN);
            safeOps.add(SqlKind.CEIL);
            safeOps.add(SqlKind.FLOOR);
            safeOps.add(SqlKind.REVERSE);
            safeOps.add(SqlKind.TIMESTAMP_ADD);
            safeOps.add(SqlKind.TIMESTAMP_DIFF);
            this.safeOps = Sets.immutableEnumSet(safeOps);
        }

        @Override
        public Boolean visitInputRef(RexInputRef inputRef) {
            return true;
        }

        @Override
        public Boolean visitLocalRef(RexLocalRef localRef) {
            return false;
        }

        @Override
        public Boolean visitLiteral(RexLiteral literal) {
            return true;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            SqlKind sqlKind = call.getKind();
            SqlOperator sqlOperator = call.getOperator();

            switch (sqlKind) {
                case DIVIDE:
                case MOD:
                    List<RexNode> operands = call.getOperands();
                    boolean isSafe =
                            RexVisitorImpl.visitArrayAnd(this, ImmutableList.of(operands.get(0)));
                    if (!isSafe) {
                        return false;
                    }
                    if (operands.get(1) instanceof RexLiteral) {
                        RexLiteral literal = (RexLiteral) operands.get(1);
                        return RexUtil.isNullLiteral(literal, true);
                    }
                    return false;
                default:
                    break;
            }

            if (sqlOperator.isSafeOperator()
                    || RexUtil.isLosslessCast(call)
                    || safeOps.contains(sqlKind)
                    || safeOperators.contains(sqlOperator)) {
                return RexVisitorImpl.visitArrayAnd(this, call.operands);
            }

            return false;
        }

        @Override
        public Boolean visitOver(RexOver over) {
            return false;
        }

        @Override
        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
            return false;
        }

        @Override
        public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
            return false;
        }

        @Override
        public Boolean visitRangeRef(RexRangeRef rangeRef) {
            return false;
        }

        @Override
        public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
            return true;
        }

        @Override
        public Boolean visitSubQuery(RexSubQuery subQuery) {
            return false;
        }

        @Override
        public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
            return false;
        }

        @Override
        public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return false;
        }

        @Override
        public Boolean visitLambda(RexLambda lambda) {
            return lambda.getExpression().accept(this);
        }

        @Override
        public Boolean visitLambdaRef(RexLambdaRef lambdaRef) {
            return true;
        }

        @Override
        public Boolean visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
            return true;
        }
    }

    /**
     * Analyzes a given {@link RexNode} and decides whenever it is safe to unwind.
     *
     * <p>"Safe" means that it only contains a combination of known good operators.
     *
     * <p>Division is an unsafe operator; consider the following:
     *
     * <pre>case when a &gt; 0 then 1 / a else null end</pre>
     */
    static boolean isSafeExpression(RexNode r) {
        return r.accept(SafeRexVisitor.INSTANCE);
    }

    private @Nullable RexNode simplifyBooleanCase(
            RexBuilder rexBuilder,
            List<CaseBranch> inputBranches,
            @SuppressWarnings("unused") RexUnknownAs unknownAs,
            RelDataType branchType) {
        RexNode result;

        // prepare all condition/branches for boolean interpretation
        // It's done here make these interpretation changes available to case2or simplifications
        // but not interfere with the normal simplification recursion
        List<CaseBranch> branches = new ArrayList<>();
        for (CaseBranch branch : inputBranches) {
            if ((!branches.isEmpty() && !isSafeExpression(branch.cond))
                    || !isSafeExpression(branch.value)) {
                return null;
            }
            final RexNode cond = isTrue(branch.cond);
            final RexNode value;
            if (!branchType.equals(branch.value.getType())) {
                value = rexBuilder.makeAbstractCast(branchType, branch.value, false);
            } else {
                value = branch.value;
            }
            branches.add(new CaseBranch(cond, value));
        }

        result = simplifyBooleanCaseGeneric(rexBuilder, branches);
        return result;
    }

    /**
     * Generic boolean case simplification.
     *
     * <p>Rewrites:
     *
     * <pre>
     * CASE
     *   WHEN p1 THEN x
     *   WHEN p2 THEN y
     *   ELSE z
     * END
     * </pre>
     *
     * to
     *
     * <pre>(p1 and x) or (p2 and y and not(p1)) or (true and z and not(p1) and not(p2))</pre>
     */
    private static RexNode simplifyBooleanCaseGeneric(
            RexBuilder rexBuilder, List<CaseBranch> branches) {

        boolean booleanBranches =
                branches.stream()
                        .allMatch(
                                branch ->
                                        branch.value.isAlwaysTrue()
                                                || branch.value.isAlwaysFalse());
        final List<RexNode> terms = new ArrayList<>();
        final List<RexNode> notTerms = new ArrayList<>();
        for (CaseBranch branch : branches) {
            boolean useBranch = !branch.value.isAlwaysFalse();
            if (useBranch) {
                final RexNode branchTerm;
                if (branch.value.isAlwaysTrue()) {
                    branchTerm = branch.cond;
                } else {
                    branchTerm =
                            rexBuilder.makeCall(SqlStdOperatorTable.AND, branch.cond, branch.value);
                }
                terms.add(RexUtil.andNot(rexBuilder, branchTerm, notTerms));
            }
            if (booleanBranches && useBranch) {
                // we are safe to ignore this branch because for boolean true branches:
                // a || (b && !a) === a || b
            } else {
                notTerms.add(branch.cond);
            }
        }
        return RexUtil.composeDisjunction(rexBuilder, terms);
    }

    @Deprecated // to be removed before 2.0
    public RexNode simplifyAnd(RexCall e) {
        ensureParanoidOff();
        return simplifyAnd(e, defaultUnknownAs);
    }

    RexNode simplifyAnd(RexCall e, RexUnknownAs unknownAs) {
        List<RexNode> operands = RelOptUtil.conjunctions(e);

        if (unknownAs == FALSE && predicateElimination) {
            simplifyAndTerms(operands, FALSE);
        } else {
            simplifyList(operands, unknownAs);
        }

        final List<RexNode> terms = new ArrayList<>();
        final List<RexNode> notTerms = new ArrayList<>();

        final SargCollector sargCollector = new SargCollector(rexBuilder, true);
        operands.forEach(t -> sargCollector.accept(t, terms));
        if (sargCollector.needToFix()) {
            operands.clear();
            terms.forEach(t -> operands.add(SargCollector.fix(rexBuilder, t, unknownAs)));
        }
        terms.clear();

        for (RexNode o : operands) {
            RelOptUtil.decomposeConjunction(o, terms, notTerms);
        }

        switch (unknownAs) {
            case FALSE:
                return simplifyAnd2ForUnknownAsFalse(terms, notTerms, Comparable.class);
            default:
                break;
        }
        return simplifyAnd2(terms, notTerms);
    }

    // package-protected only to support a deprecated method; treat as private
    RexNode simplifyAnd2(List<RexNode> terms, List<RexNode> notTerms) {
        for (RexNode term : terms) {
            if (term.isAlwaysFalse()) {
                return rexBuilder.makeLiteral(false);
            }
        }
        if (terms.isEmpty() && notTerms.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        }
        // If one of the not-disjunctions is a disjunction that is wholly
        // contained in the disjunctions list, the expression is not
        // satisfiable.
        //
        // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
        // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
        // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
        List<RexNode> notSatisfiableNullables = null;
        for (RexNode notDisjunction : notTerms) {
            final List<RexNode> terms2 = RelOptUtil.conjunctions(notDisjunction);
            if (!terms.containsAll(terms2)) {
                // may be satisfiable ==> check other terms
                continue;
            }
            if (!notDisjunction.getType().isNullable()) {
                // x is NOT nullable, then x AND NOT(x) ==> FALSE
                return rexBuilder.makeLiteral(false);
            }
            // x AND NOT(x) is UNKNOWN for NULL input
            // So we search for the shortest notDisjunction then convert
            // original expression to NULL and x IS NULL
            if (notSatisfiableNullables == null) {
                notSatisfiableNullables = new ArrayList<>();
            }
            notSatisfiableNullables.add(notDisjunction);
        }

        if (notSatisfiableNullables != null) {
            // Remove the intersection of "terms" and "notTerms"
            terms.removeAll(notSatisfiableNullables);
            notTerms.removeAll(notSatisfiableNullables);

            // The intersection simplify to "null and x1 is null and x2 is null..."
            terms.add(rexBuilder.makeNullLiteral(notSatisfiableNullables.get(0).getType()));
            for (RexNode notSatisfiableNullable : notSatisfiableNullables) {
                terms.add(
                        simplifyIs(
                                (RexCall)
                                        rexBuilder.makeCall(
                                                SqlStdOperatorTable.IS_NULL,
                                                notSatisfiableNullable),
                                UNKNOWN));
            }
        }
        // Add the NOT disjunctions back in.
        for (RexNode notDisjunction : notTerms) {
            terms.add(simplify(not(notDisjunction), UNKNOWN));
        }
        return RexUtil.composeConjunction(rexBuilder, terms);
    }

    /**
     * As {@link #simplifyAnd2(List, List)} but we assume that if the expression returns UNKNOWN it
     * will be interpreted as FALSE.
     */
    RexNode simplifyAnd2ForUnknownAsFalse(List<RexNode> terms, List<RexNode> notTerms) {
        //noinspection unchecked
        return simplifyAnd2ForUnknownAsFalse(terms, notTerms, Comparable.class);
    }

    private <C extends Comparable<C>> RexNode simplifyAnd2ForUnknownAsFalse(
            List<RexNode> terms, List<RexNode> notTerms, Class<C> clazz) {
        for (RexNode term : terms) {
            if (term.isAlwaysFalse() || RexLiteral.isNullLiteral(term)) {
                return rexBuilder.makeLiteral(false);
            }
        }
        if (terms.isEmpty() && notTerms.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        }
        if (terms.size() == 1 && notTerms.isEmpty()) {
            // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
            return simplify(terms.get(0), FALSE);
        }
        // Try to simplify the expression
        final Multimap<RexNode, Pair<RexNode, RexNode>> equalityTerms = ArrayListMultimap.create();
        final Map<RexNode, Pair<Range<C>, List<RexNode>>> rangeTerms = new HashMap<>();
        final Map<RexNode, RexLiteral> equalityConstantTerms = new HashMap<>();
        final Set<RexNode> negatedTerms = new HashSet<>();
        final Set<RexNode> nullOperands = new HashSet<>();
        final Set<RexNode> notNullOperands = new LinkedHashSet<>();

        // Add the predicates from the source to the range terms.
        for (RexNode predicate : predicates.pulledUpPredicates) {
            final Comparison comparison = Comparison.of(predicate);
            if (comparison != null && comparison.kind != SqlKind.NOT_EQUALS) { // not supported yet
                final C v0 = comparison.literal.getValueAs(clazz);
                if (v0 != null) {
                    final RexNode result =
                            processRange(
                                    rexBuilder,
                                    terms,
                                    rangeTerms,
                                    predicate,
                                    comparison.ref,
                                    v0,
                                    comparison.kind);
                    if (result != null) {
                        // Not satisfiable
                        return result;
                    }
                }
            }
        }

        for (int i = 0; i < terms.size(); i++) {
            RexNode term = terms.get(i);
            if (!RexUtil.isDeterministic(term)) {
                continue;
            }
            // Simplify BOOLEAN expressions if possible
            while (term.getKind() == SqlKind.EQUALS) {
                RexCall call = (RexCall) term;
                if (call.getOperands().get(0).isAlwaysTrue()) {
                    term = call.getOperands().get(1);
                    terms.set(i, term);
                    continue;
                } else if (call.getOperands().get(1).isAlwaysTrue()) {
                    term = call.getOperands().get(0);
                    terms.set(i, term);
                    continue;
                }
                break;
            }
            switch (term.getKind()) {
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case GREATER_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    RexCall call = (RexCall) term;
                    final RexNode left = call.getOperands().get(0);
                    final RexNode right = call.getOperands().get(1);
                    final Comparison comparison = Comparison.of(term);
                    // Check for comparison with null values
                    if (comparison != null && comparison.literal.getValue() == null) {
                        return rexBuilder.makeLiteral(false);
                    }
                    // Check for equality on different constants. If the same ref or CAST(ref)
                    // is equal to different constants, this condition cannot be satisfied,
                    // and hence it can be evaluated to FALSE
                    if (term.getKind() == SqlKind.EQUALS) {
                        if (comparison != null) {
                            final RexLiteral literal = comparison.literal;
                            final RexLiteral prevLiteral =
                                    equalityConstantTerms.put(comparison.ref, literal);

                            if (prevLiteral != null
                                    && literal.getType().equals(prevLiteral.getType())
                                    && !literal.equals(prevLiteral)) {
                                return rexBuilder.makeLiteral(false);
                            }
                        } else if (RexUtil.isReferenceOrAccess(left, true)
                                && RexUtil.isReferenceOrAccess(right, true)) {
                            equalityTerms.put(left, Pair.of(right, term));
                        }
                    }
                    // Assume the expression a > 5 is part of a Filter condition.
                    // Then we can derive the negated term: a <= 5.
                    // But as the comparison is string based and thus operands order dependent,
                    // we should also add the inverted negated term: 5 >= a.
                    // Observe that for creating the inverted term we invert the list of operands.
                    RexNode negatedTerm = RexUtil.negate(rexBuilder, call);
                    if (negatedTerm != null) {
                        negatedTerms.add(negatedTerm);
                        RexNode invertNegatedTerm =
                                RexUtil.invert(rexBuilder, (RexCall) negatedTerm);
                        if (invertNegatedTerm != null) {
                            negatedTerms.add(invertNegatedTerm);
                        }
                    }
                    // Remove terms that are implied by predicates on the input,
                    // or weaken terms that are partially implied.
                    // E.g. given predicate "x >= 5" and term "x between 3 and 10"
                    // we weaken to term to "x between 5 and 10".
                    final RexNode term2 = simplifyUsingPredicates(term, clazz);
                    if (term2 != term) {
                        terms.set(i, term = term2);
                    }
                    // Range
                    if (comparison != null
                            && comparison.kind != SqlKind.NOT_EQUALS) { // not supported yet
                        final C constant = comparison.literal.getValueAs(clazz);
                        if (constant == null) {
                            break;
                        }
                        final RexNode result =
                                processRange(
                                        rexBuilder,
                                        terms,
                                        rangeTerms,
                                        term,
                                        comparison.ref,
                                        constant,
                                        comparison.kind);
                        if (result != null) {
                            // Not satisfiable
                            return result;
                        }
                    }
                    break;
                case IS_NOT_NULL:
                    notNullOperands.add(((RexCall) term).getOperands().get(0));
                    terms.remove(i);
                    --i;
                    break;
                case IS_NULL:
                    nullOperands.add(((RexCall) term).getOperands().get(0));
                    break;
                default:
                    break;
            }
        }
        // Check for equality of two refs wrt equality with constants
        // Example #1. x=5 AND y=5 AND x=y : x=5 AND y=5
        // Example #2. x=5 AND y=6 AND x=y - not satisfiable
        for (RexNode ref1 : equalityTerms.keySet()) {
            final RexLiteral literal1 = equalityConstantTerms.get(ref1);
            if (literal1 == null) {
                continue;
            }
            Collection<Pair<RexNode, RexNode>> references = equalityTerms.get(ref1);
            for (Pair<RexNode, RexNode> ref2 : references) {
                final RexLiteral literal2 = equalityConstantTerms.get(ref2.left);
                if (literal2 == null) {
                    continue;
                }
                if (literal1.getType().equals(literal2.getType()) && !literal1.equals(literal2)) {
                    // If an expression is equal to two different constants,
                    // it is not satisfiable
                    return rexBuilder.makeLiteral(false);
                }
                // Otherwise we can remove the term, as we already know that
                // the expression is equal to two constants
                terms.remove(ref2.right);
            }
        }
        // If one of the not-disjunctions is a disjunction that is wholly
        // contained in the disjunctions list, the expression is not
        // satisfiable.
        //
        // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
        // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
        // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
        final Set<RexNode> termsSet = new HashSet<>(terms);
        for (RexNode notDisjunction : notTerms) {
            if (!RexUtil.isDeterministic(notDisjunction)) {
                continue;
            }
            final List<RexNode> terms2Set = RelOptUtil.conjunctions(notDisjunction);
            if (termsSet.containsAll(terms2Set)) {
                return rexBuilder.makeLiteral(false);
            }
        }
        // The negated terms: only deterministic expressions
        for (RexNode negatedTerm : negatedTerms) {
            if (termsSet.contains(negatedTerm)) {
                return rexBuilder.makeLiteral(false);
            }
        }
        // Add the NOT disjunctions back in.
        for (RexNode notDisjunction : notTerms) {
            terms.add(not(notDisjunction));
        }
        // Find operands that make will let whole expression evaluate to FALSE if set to NULL
        final Set<RexNode> strongOperands = new HashSet<>();
        for (RexNode term : terms) {
            if (!RexUtil.isDeterministic(term)) {
                continue;
            }
            final VariableCollector collector = new VariableCollector();
            term.accept(collector);
            for (RexInputRef ref : collector.refs) {
                final boolean strong = Strong.isNotTrue(term, ImmutableBitSet.of(ref.index));
                if (strong) {
                    strongOperands.add(ref);
                }
            }
            final RexUtil.FieldAccessFinder fieldAccessFinder = new RexUtil.FieldAccessFinder();
            term.accept(fieldAccessFinder);
            for (RexFieldAccess rexFieldAccess : fieldAccessFinder.getFieldAccessList()) {
                final boolean strong = Strong.of(ImmutableSet.of(rexFieldAccess)).isNotTrue(term);
                if (strong) {
                    strongOperands.add(rexFieldAccess);
                }
            }
        }
        // If one column should be null and is in a comparison predicate,
        // it is not satisfiable.
        // Example. IS NULL(x) AND x < 5  - not satisfiable
        if (!Collections.disjoint(nullOperands, strongOperands)) {
            return rexBuilder.makeLiteral(false);
        }
        // Remove not necessary IS NOT NULL expressions.
        // Example. IS NOT NULL(x) AND x < 5  : x < 5
        for (RexNode operand : notNullOperands) {
            if (!strongOperands.contains(operand)) {
                terms.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
            }
        }
        return RexUtil.composeConjunction(rexBuilder, terms);
    }

    private <C extends Comparable<C>> RexNode simplifyUsingPredicates(RexNode e, Class<C> clazz) {
        if (predicates.pulledUpPredicates.isEmpty()) {
            return e;
        }

        final Comparison comparison = Comparison.of(e);
        // Check for comparison with null values
        if (comparison == null || comparison.literal.getValue() == null) {
            return e;
        }

        final C v0 = comparison.literal.getValueAs(clazz);
        if (v0 == null) {
            return e;
        }
        final RangeSet<C> rangeSet = rangeSet(comparison.kind, v0);
        final RangeSet<C> rangeSet2 =
                residue(comparison.ref, rangeSet, predicates.pulledUpPredicates, clazz);
        if (rangeSet2.isEmpty()) {
            // Term is impossible to satisfy given these predicates
            return rexBuilder.makeLiteral(false);
        } else if (rangeSet2.equals(rangeSet)) {
            // no change
            return e;
        } else if (rangeSet2.equals(RangeSets.rangeSetAll())) {
            // Range is always satisfied given these predicates; but nullability might
            // be problematic
            return simplify(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, comparison.ref),
                    RexUnknownAs.UNKNOWN);
        } else if (rangeSet2.asRanges().size() == 1
                && Iterables.getOnlyElement(rangeSet2.asRanges()).hasLowerBound()
                && Iterables.getOnlyElement(rangeSet2.asRanges()).hasUpperBound()
                && Iterables.getOnlyElement(rangeSet2.asRanges())
                        .lowerEndpoint()
                        .equals(Iterables.getOnlyElement(rangeSet2.asRanges()).upperEndpoint())) {
            final Range<C> r = Iterables.getOnlyElement(rangeSet2.asRanges());
            // range is now a point; it's worth simplifying
            return rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    comparison.ref,
                    rexBuilder.makeLiteral(
                            r.lowerEndpoint(),
                            comparison.literal.getType(),
                            comparison.literal.getTypeName()));
        } else {
            // range has been reduced but it's not worth simplifying
            return e;
        }
    }

    /**
     * Weakens a term so that it checks only what is not implied by predicates.
     *
     * <p>The term is broken into "ref comparison constant", for example "$0 &lt; 5".
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>{@code residue($0 < 10, [$0 < 5])} returns {@code true}
     *   <li>{@code residue($0 < 10, [$0 < 20, $0 > 0])} returns {@code $0 < 10}
     * </ul>
     */
    private static <C extends Comparable<C>> RangeSet<C> residue(
            RexNode ref, RangeSet<C> r0, List<RexNode> predicates, Class<C> clazz) {
        RangeSet<C> result = r0;
        for (RexNode predicate : predicates) {
            switch (predicate.getKind()) {
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    final RexCall call = (RexCall) predicate;
                    final Comparison comparison = Comparison.of(call);
                    if (comparison != null && comparison.ref.equals(ref)) {
                        final C c1 = comparison.literal.getValueAs(clazz);
                        if (c1 == null) {
                            throw new AssertionError(
                                    "value must not be null in " + comparison.literal);
                        }
                        switch (predicate.getKind()) {
                            case NOT_EQUALS:
                                // We want to intersect result with the range set of everything but
                                // c1. We subtract the point c1 from result, which is equivalent.
                                final Range<C> pointRange = range(SqlKind.EQUALS, c1);
                                final RangeSet<C> notEqualsRangeSet =
                                        ImmutableRangeSet.of(pointRange).complement();
                                if (result.enclosesAll(notEqualsRangeSet)) {
                                    result = RangeSets.rangeSetAll();
                                    continue;
                                }
                                result = RangeSets.minus(result, pointRange);
                                break;
                            default:
                                final Range<C> r1 = range(comparison.kind, c1);
                                if (result.encloses(r1)) {
                                    // Given these predicates, term is always satisfied.
                                    // e.g. r0 is "$0 < 10", r1 is "$0 < 5"
                                    result = RangeSets.rangeSetAll();
                                    continue;
                                }
                                result = result.subRangeSet(r1);
                        }
                        if (result.isEmpty()) {
                            break; // short-cut
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    /**
     * Simplifies OR(x, x) into x, and similar. The simplified expression returns UNKNOWN values as
     * is (not as FALSE).
     */
    @Deprecated // to be removed before 2.0
    public RexNode simplifyOr(RexCall call) {
        ensureParanoidOff();
        return simplifyOr(call, UNKNOWN);
    }

    private RexNode simplifyOr(RexCall call, RexUnknownAs unknownAs) {
        assert call.getKind() == SqlKind.OR;
        final List<RexNode> terms0 = RelOptUtil.disjunctions(call);
        final List<RexNode> terms;
        if (predicateElimination) {
            terms = Util.moveToHead(terms0, e -> e.getKind() == SqlKind.IS_NULL);
            simplifyOrTerms(terms, unknownAs);
        } else {
            terms = terms0;
            simplifyList(terms, unknownAs);
        }
        return simplifyOrs(terms, unknownAs);
    }

    /**
     * Simplifies a list of terms and combines them into an OR. Modifies the list in place. The
     * simplified expression returns UNKNOWN values as is (not as FALSE).
     */
    @Deprecated // to be removed before 2.0
    public RexNode simplifyOrs(List<RexNode> terms) {
        ensureParanoidOff();
        return simplifyOrs(terms, UNKNOWN);
    }

    private void ensureParanoidOff() {
        if (paranoid) {
            throw new UnsupportedOperationException("Paranoid is not supported for this method");
        }
    }

    /** Simplifies a list of terms and combines them into an OR. Modifies the list in place. */
    private RexNode simplifyOrs(List<RexNode> terms, RexUnknownAs unknownAs) {
        final SargCollector sargCollector = new SargCollector(rexBuilder, false);
        final List<RexNode> newTerms = new ArrayList<>();
        terms.forEach(t -> sargCollector.accept(t, newTerms));
        if (sargCollector.needToFix()) {
            terms.clear();
            newTerms.forEach(t -> terms.add(SargCollector.fix(rexBuilder, t, unknownAs)));
        }

        // CALCITE-3198 Auxiliary map to simplify cases like:
        //   X <> A OR X <> B => X IS NOT NULL or NULL
        // The map key will be the 'X'; and the value the first call 'X<>A' that is found,
        // or 'X IS NOT NULL' if a simplification takes place (because another 'X<>B' is found)
        final Map<RexNode, RexNode> notEqualsComparisonMap = new HashMap<>();
        final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
        for (int i = 0; i < terms.size(); i++) {
            final RexNode term = terms.get(i);
            switch (term.getKind()) {
                case LITERAL:
                    if (RexLiteral.isNullLiteral(term)) {
                        if (unknownAs == FALSE) {
                            terms.remove(i);
                            --i;
                            continue;
                        } else if (unknownAs == TRUE) {
                            return trueLiteral;
                        }
                    } else {
                        if (RexLiteral.booleanValue(term)) {
                            return term; // true
                        } else {
                            terms.remove(i);
                            --i;
                            continue;
                        }
                    }
                    break;
                case NOT_EQUALS:
                    final Comparison notEqualsComparison = Comparison.of(term);
                    if (notEqualsComparison != null) {
                        // We are dealing with a X<>A term, check if we saw before another
                        // NOT_EQUALS involving X
                        final RexNode prevNotEquals =
                                notEqualsComparisonMap.get(notEqualsComparison.ref);
                        if (prevNotEquals == null) {
                            // This is the first NOT_EQUALS involving X, put it in the map
                            notEqualsComparisonMap.put(notEqualsComparison.ref, term);
                        } else {
                            // There is already in the map another NOT_EQUALS involving X:
                            //   - if it is already an IS_NOT_NULL: it was already simplified,
                            // ignore this term
                            //   - if it is not an IS_NOT_NULL (i.e. it is a NOT_EQUALS): check
                            // comparison values
                            if (prevNotEquals.getKind() != SqlKind.IS_NOT_NULL) {
                                final Comparable comparable1 =
                                        notEqualsComparison.literal.getValue();
                                final Comparable comparable2 =
                                        castNonNull(Comparison.of(prevNotEquals))
                                                .literal
                                                .getValue();
                                //noinspection unchecked
                                if (comparable1 != null
                                        && comparable2 != null
                                        && comparable1.compareTo(comparable2) != 0) {
                                    // X <> A OR X <> B => X IS NOT NULL OR NULL
                                    final RexNode isNotNull =
                                            rexBuilder.makeCall(
                                                    SqlStdOperatorTable.IS_NOT_NULL,
                                                    notEqualsComparison.ref);
                                    final RexNode constantNull =
                                            rexBuilder.makeNullLiteral(trueLiteral.getType());
                                    final RexNode newCondition =
                                            simplify(
                                                    rexBuilder.makeCall(
                                                            SqlStdOperatorTable.OR,
                                                            isNotNull,
                                                            constantNull),
                                                    unknownAs);
                                    if (newCondition.isAlwaysTrue()) {
                                        return trueLiteral;
                                    }
                                    notEqualsComparisonMap.put(notEqualsComparison.ref, isNotNull);
                                    final int pos = terms.indexOf(prevNotEquals);
                                    terms.set(pos, newCondition);
                                }
                            }
                            terms.remove(i);
                            --i;
                            continue;
                        }
                    }
                    break;
                case IS_NOT_TRUE:
                    RexNode arg = ((RexCall) term).getOperands().get(0);
                    if (isSafeExpression(arg) && terms.contains(arg)) {
                        return trueLiteral;
                    }
                    break;
                case NOT:
                    RexNode x = ((RexCall) term).getOperands().get(0);
                    if (isSafeExpression(x) && terms.contains(x)) {
                        if (!x.getType().isNullable()) {
                            return trueLiteral;
                        }

                        final RexNode isNotNull =
                                rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, x);
                        terms.set(terms.indexOf(x), simplifyIs((RexCall) isNotNull, unknownAs));
                        terms.set(i, rexBuilder.makeNullLiteral(x.getType()));
                        i--;
                    }
                    break;
                default:
                    break;
            }
        }
        return RexUtil.composeDisjunction(rexBuilder, terms);
    }

    private void verify(RexNode before, RexNode simplified, RexUnknownAs unknownAs) {
        if (simplified.isAlwaysFalse() && before.isAlwaysTrue()) {
            throw new AssertionError(
                    "always true [" + before + "] simplified to always false [" + simplified + "]");
        }
        if (simplified.isAlwaysTrue() && before.isAlwaysFalse()) {
            throw new AssertionError(
                    "always false [" + before + "] simplified to always true [" + simplified + "]");
        }
        final RexAnalyzer foo0 = new RexAnalyzer(before, predicates);
        final RexAnalyzer foo1 = new RexAnalyzer(simplified, predicates);
        if (foo0.unsupportedCount > 0 || foo1.unsupportedCount > 0) {
            // Analyzer cannot handle this expression currently
            return;
        }
        if (!foo0.variables.containsAll(foo1.variables)) {
            throw new AssertionError(
                    "variable mismatch: "
                            + before
                            + " has "
                            + foo0.variables
                            + ", "
                            + simplified
                            + " has "
                            + foo1.variables);
        }
        assignment_loop:
        for (Map<RexNode, Comparable> map : foo0.assignments()) {
            for (RexNode predicate : predicates.pulledUpPredicates) {
                final Comparable v = RexInterpreter.evaluate(predicate, map);
                if (!Boolean.TRUE.equals(v)) {
                    continue assignment_loop;
                }
            }
            Comparable v0 = RexInterpreter.evaluate(foo0.e, map);
            if (v0 == null) {
                throw new AssertionError("interpreter returned null for " + foo0.e);
            }
            Comparable v1 = RexInterpreter.evaluate(foo1.e, map);
            if (v1 == null) {
                throw new AssertionError("interpreter returned null for " + foo1.e);
            }
            if (before.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                switch (unknownAs) {
                    case FALSE:
                    case TRUE:
                        if (v0 == NullSentinel.INSTANCE) {
                            v0 = unknownAs.toBoolean();
                        }
                        if (v1 == NullSentinel.INSTANCE) {
                            v1 = unknownAs.toBoolean();
                        }
                        break;
                    default:
                        break;
                }
            }
            if (!v0.equals(v1)) {
                throw new AssertionError(
                        "result mismatch (unknown as "
                                + unknownAs
                                + "): when applied to "
                                + map
                                + ",\n"
                                + before
                                + " yielded "
                                + v0
                                + ";\n"
                                + simplified
                                + " yielded "
                                + v1);
            }
        }
    }

    private RexNode simplifySearch(RexCall call, RexUnknownAs unknownAs) {
        assert call.getKind() == SqlKind.SEARCH;
        final RexNode operand = call.getOperands().get(0);
        final RexNode simplifiedOperand = simplify(operand, unknownAs);
        final boolean operandUnchanged = operand.equals(simplifiedOperand);
        final RexNode searchOperand = operandUnchanged ? operand : simplifiedOperand;
        if (call.getOperands().get(1) instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) call.getOperands().get(1);
            final Sarg sarg = castNonNull(literal.getValueAs(Sarg.class));
            if (sarg.isAll() || sarg.isNone()) {
                RexNode rexNode = RexUtil.simpleSarg(rexBuilder, searchOperand, sarg, unknownAs);
                return simplify(rexNode, unknownAs);
            }
            // Remove null from sarg if the left-hand side is never null
            if (sarg.nullAs != UNKNOWN) {
                final RexNode simplified = simplifyIs1(SqlKind.IS_NULL, searchOperand, unknownAs);
                if (simplified != null && simplified.isAlwaysFalse()) {
                    final Sarg sarg2 = Sarg.of(UNKNOWN, sarg.rangeSet);
                    final RexLiteral literal2 =
                            rexBuilder.makeLiteral(sarg2, literal.getType(), literal.getTypeName());
                    // Now we've strengthened the Sarg, try to simplify again
                    return simplifySearch(
                            call.clone(call.type, ImmutableList.of(searchOperand, literal2)),
                            unknownAs);
                }
            } else if (sarg.isPoints() && sarg.pointCount <= 1) {
                // Expand "SEARCH(x, Sarg([point])" to "x = point"
                // and "SEARCH(x, Sarg([])" to "false"
                return RexUtil.expandSearch(rexBuilder, null, call);
            }
        }
        return operandUnchanged
                ? call
                : call.clone(
                        call.type, ImmutableList.of(simplifiedOperand, call.getOperands().get(1)));
    }

    private RexNode simplifyCast(RexCall e) {
        RexNode operand = e.getOperands().get(0);
        operand = simplify(operand, UNKNOWN);
        // The type of DYNAMIC_PARAM is indeterminate, so the cast cannot be eliminated
        if (operand.getKind() != SqlKind.DYNAMIC_PARAM
                && sameTypeOrNarrowsNullability(e.getType(), operand.getType())
                // DECIMAL casts are never no-ops: they perform bounds checking
                && e.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
            return operand;
        }
        if (RexUtil.isLosslessCast(operand)) {
            // x :: y below means cast(x as y) (which is PostgreSQL-specific cast by the way)
            // A) Remove lossless casts:
            // A.1) intExpr :: bigint :: int => intExpr
            // A.2) char2Expr :: char(5) :: char(2) => char2Expr
            // B) There are cases when we can't remove two casts, but we could probably remove inner
            // one
            // B.1) char2expression :: char(4) :: char(5) -> char2expression :: char(5)
            // B.2) char2expression :: char(10) :: char(5) -> char2expression :: char(5)
            // B.3) char2expression :: varchar(10) :: char(5) -> char2expression :: char(5)
            // B.4) char6expression :: varchar(10) :: char(5) -> char6expression :: char(5)
            // C) Simplification is not possible:
            // C.1) char6expression :: char(3) :: char(5) -> must not be changed
            //      the input is truncated to 3 chars, so we can't use char6expression :: char(5)
            // C.2) varchar2Expr :: char(5) :: varchar(2) -> must not be changed
            //      the input have to be padded with spaces (up to 2 chars)
            // C.3) char2expression :: char(4) :: varchar(5) -> must not be changed
            //      would not have the padding

            // The approach seems to be:
            // 1) Ensure inner cast is lossless (see if above)
            // 2) If operand of the inner cast has the same type as the outer cast,
            //    remove two casts except C.2 or C.3-like pattern (== inner cast is CHAR)
            // 3) If outer cast is lossless, remove inner cast (B-like cases)

            // Here we try to remove two casts in one go (A-like cases)
            RexNode intExpr = ((RexCall) operand).operands.get(0);
            // intExpr == CHAR detects A.1
            // operand != CHAR detects C.2
            if ((intExpr.getType().getSqlTypeName() == SqlTypeName.CHAR
                            || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)
                    && sameTypeOrNarrowsNullability(e.getType(), intExpr.getType())) {
                return intExpr;
            }
            // Here we try to remove inner cast (B-like cases)
            if (RexUtil.isLosslessCast(intExpr.getType(), operand.getType())
                    && (e.getType().getSqlTypeName() == operand.getType().getSqlTypeName()
                            || e.getType().getSqlTypeName() == SqlTypeName.CHAR
                            || operand.getType().getSqlTypeName() != SqlTypeName.CHAR)
                    && SqlTypeCoercionRule.instance()
                            .canApplyFrom(
                                    intExpr.getType().getSqlTypeName(),
                                    e.getType().getSqlTypeName())) {
                return rexBuilder.makeCast(e.getParserPosition(), e.getType(), intExpr);
            }
        }
        final boolean safe = e.getKind() == SqlKind.SAFE_CAST;
        switch (operand.getKind()) {
            case LITERAL:
                final RexLiteral literal = (RexLiteral) operand;
                final Comparable value = literal.getValueAs(Comparable.class);
                final SqlTypeName typeName = literal.getTypeName();

                // First, try to remove the cast without changing the value.
                // makeCast and canRemoveCastFromLiteral have the same logic, so we are
                // sure to be able to remove the cast.
                if (rexBuilder.canRemoveCastFromLiteral(e.getType(), value, typeName)) {
                    return rexBuilder.makeCast(e.getParserPosition(), e.getType(), operand);
                }

                // Next, try to convert the value to a different type,
                // e.g. CAST('123' as integer)
                switch (literal.getTypeName()) {
                    case TIME:
                        switch (e.getType().getSqlTypeName()) {
                            case TIMESTAMP:
                                return e;
                            default:
                                break;
                        }
                        break;
                    default:
                        break;
                }
                final List<RexNode> reducedValues = new ArrayList<>();
                final RexNode simplifiedExpr =
                        e.operandCount() == 2
                                ? rexBuilder.makeCast(
                                        e.getParserPosition(),
                                        e.getType(),
                                        operand,
                                        safe,
                                        safe,
                                        (RexLiteral) e.getOperands().get(1))
                                : rexBuilder.makeCast(e.getType(), operand, safe, safe);
                executor.reduce(rexBuilder, ImmutableList.of(simplifiedExpr), reducedValues);
                RexNode reducedRexNode = requireNonNull(Iterables.getOnlyElement(reducedValues));
                if (reducedRexNode.isA(SqlKind.CAST)) {
                    RexNode reducedOperand = ((RexCall) reducedRexNode).getOperands().get(0);
                    if (sameTypeOrNarrowsNullability(
                            reducedRexNode.getType(), reducedOperand.getType())) {
                        return reducedOperand;
                    }
                }
                return reducedRexNode;
            default:
                if (operand == e.getOperands().get(0)) {
                    return e;
                } else {
                    return e.operands.size() > 1
                            ? rexBuilder.makeCast(
                                    e.getParserPosition(),
                                    e.getType(),
                                    operand,
                                    safe,
                                    safe,
                                    (RexLiteral) e.getOperands().get(1))
                            : rexBuilder.makeCast(e.getType(), operand, safe, safe);
                }
        }
    }

    /**
     * Tries to simplify CEIL/FLOOR function on top of CEIL/FLOOR.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>{@code floor(floor($0, flag(hour)), flag(day))} returns {@code floor($0, flag(day))}
     *   <li>{@code ceil(ceil($0, flag(second)), flag(day))} returns {@code ceil($0, flag(day))}
     *   <li>{@code floor(floor($0, flag(day)), flag(second))} does not change
     * </ul>
     */
    private RexNode simplifyCeilFloor(RexCall e) {
        if (e.getOperands().size() != 2) {
            // Bail out since we only simplify floor <date>
            return e;
        }
        final RexNode operand = simplify(e.getOperands().get(0), UNKNOWN);
        if (e.getKind() == operand.getKind()) {
            assert e.getKind() == SqlKind.CEIL || e.getKind() == SqlKind.FLOOR;
            // CEIL/FLOOR on top of CEIL/FLOOR
            final RexCall child = (RexCall) operand;
            if (child.getOperands().size() != 2) {
                // Bail out since we only simplify ceil/floor <date>
                return e;
            }
            final RexLiteral parentFlag = (RexLiteral) e.operands.get(1);
            final TimeUnitRange parentFlagValue = (TimeUnitRange) parentFlag.getValue();
            final RexLiteral childFlag = (RexLiteral) child.operands.get(1);
            final TimeUnitRange childFlagValue = (TimeUnitRange) childFlag.getValue();
            if (parentFlagValue != null && childFlagValue != null) {
                if (canRollUp(parentFlagValue.startUnit, childFlagValue.startUnit)) {
                    return e.clone(
                            e.getType(), ImmutableList.of(child.getOperands().get(0), parentFlag));
                }
            }
        }
        return e.clone(e.getType(), ImmutableList.of(operand, e.getOperands().get(1)));
    }

    /**
     * Simplify TRIM function by eliminating nested duplication.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>{@code trim(trim(' aa '))} returns {@code trim(' aa ')}
     *   <li>{@code trim(BOTH ' ' from trim(BOTH ' ' from ' aa '))} returns {@code trim(BOTH ' '
     *       from ' aa ')}
     *   <li>{@code trim(LEADING 'a' from trim(BOTH ' ' from ' aa '))} does not change
     * </ul>
     */
    private RexNode simplifyTrim(RexCall e) {
        if (e.getOperands().size() != 3) {
            return e;
        }

        RexNode trimType = simplify(e.operands.get(0));
        RexNode trimed = simplify(e.operands.get(1));
        if (e.getOperands().get(2) instanceof RexCall) {
            RexCall childNode = (RexCall) e.getOperands().get(2);
            // only strings with the same trim method and deduplication will be eliminated.
            if (childNode.getKind() == SqlKind.TRIM
                    && trimType.equals(simplify(childNode.operands.get(0)))
                    && trimed.equals(simplify(childNode.operands.get(1)))) {
                return simplifyTrim(childNode);
            }
        }

        ArrayList<RexNode> rexNodes = new ArrayList<>();
        rexNodes.add(trimType);
        rexNodes.add(trimed);
        rexNodes.add(simplify(e.operands.get(2)));
        RexNode rexNode = rexBuilder.makeCall(e.getType(), e.getOperator(), rexNodes);
        return rexNode;
    }

    /** Method that returns whether we can rollup from inner time unit to outer time unit. */
    private static boolean canRollUp(TimeUnit outer, TimeUnit inner) {
        // Special handling for QUARTER as it is not in the expected
        // order in TimeUnit
        switch (outer) {
            case YEAR:
            case MONTH:
            case DAY:
            case HOUR:
            case MINUTE:
            case SECOND:
            case MILLISECOND:
            case MICROSECOND:
                switch (inner) {
                    case YEAR:
                    case QUARTER:
                    case MONTH:
                    case DAY:
                    case HOUR:
                    case MINUTE:
                    case SECOND:
                    case MILLISECOND:
                    case MICROSECOND:
                        if (inner == TimeUnit.QUARTER) {
                            return outer == TimeUnit.YEAR;
                        }
                        return outer.ordinal() <= inner.ordinal();
                    default:
                        break;
                }
                break;
            case QUARTER:
                switch (inner) {
                    case QUARTER:
                    case MONTH:
                    case DAY:
                    case HOUR:
                    case MINUTE:
                    case SECOND:
                    case MILLISECOND:
                    case MICROSECOND:
                        return true;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return false;
    }

    /**
     * Simplifies a measure being converted immediately (in the same SELECT clause) back to a value.
     *
     * <p>For most expressions {@code e}, simplifies "{@code m2v(v2m(e))}" to "{@code e}". For
     * example, "{@code SELECT deptno + 1 AS MEASURE m}" is equivalent to "{@code SELECT deptno + 1
     * AS m}".
     *
     * <p>The exception is aggregate functions. "{@code SELECT COUNT(*) + 1 AS MEASURE m}"
     * simplifies to "{@code SELECT COUNT(*) OVER (ROWS CURRENT ROW) + 1 AS MEASURE m}".
     *
     * @param e Call to {@code M2V} to be simplified
     * @return Simplified call
     */
    private RexNode simplifyM2v(RexCall e) {
        assert e.op.kind == SqlKind.M2V;
        final RexNode operand = e.getOperands().get(0);
        switch (operand.getKind()) {
            case V2M:
                // M2V(V2M(x))  -->  x
                return flattenAggregate(((RexCall) operand).operands.get(0));
            default:
                return e;
        }
    }

    /**
     * Traverses over an expression, converting aggregate functions into single-row aggregate
     * functions.
     */
    private RexNode flattenAggregate(RexNode e) {
        return e.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (call.op.isAggregator()) {
                            final RexWindow w =
                                    rexBuilder.makeWindow(
                                            ImmutableList.of(),
                                            ImmutableList.of(),
                                            RexWindowBounds.CURRENT_ROW,
                                            RexWindowBounds.CURRENT_ROW,
                                            true);
                            return new RexOver(
                                    call.type,
                                    (SqlAggFunction) call.op,
                                    call.operands,
                                    w,
                                    false,
                                    false);
                        }
                        return super.visitCall(call);
                    }
                });
    }

    /**
     * Removes any casts that change nullability but not type.
     *
     * <p>For example, {@code CAST(1 = 0 AS BOOLEAN)} becomes {@code 1 = 0}.
     */
    public RexNode removeNullabilityCast(RexNode e) {
        return RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), e);
    }

    private static <C extends Comparable<C>> @Nullable RexNode processRange(
            RexBuilder rexBuilder,
            List<RexNode> terms,
            Map<RexNode, Pair<Range<C>, List<RexNode>>> rangeTerms,
            RexNode term,
            RexNode ref,
            C v0,
            SqlKind comparison) {
        Pair<Range<C>, List<RexNode>> p = rangeTerms.get(ref);
        if (p == null) {
            rangeTerms.put(ref, Pair.of(range(comparison, v0), ImmutableList.of(term)));
        } else {
            // Exists
            boolean removeUpperBound = false;
            boolean removeLowerBound = false;
            Range<C> r = p.left;
            final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
            switch (comparison) {
                case EQUALS:
                    if (!r.contains(v0)) {
                        // Range is empty, not satisfiable
                        return rexBuilder.makeLiteral(false);
                    }
                    rangeTerms.put(ref, Pair.of(Range.singleton(v0), ImmutableList.of(term)));
                    // remove
                    for (RexNode e : p.right) {
                        replaceLast(terms, e, trueLiteral);
                    }
                    break;
                case LESS_THAN:
                    {
                        int comparisonResult = 0;
                        if (r.hasUpperBound()) {
                            comparisonResult = v0.compareTo(r.upperEndpoint());
                        }
                        if (comparisonResult <= 0) {
                            // 1) No upper bound, or
                            // 2) We need to open the upper bound, or
                            // 3) New upper bound is lower than old upper bound
                            if (r.hasLowerBound()) {
                                if (v0.compareTo(r.lowerEndpoint()) <= 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false);
                                }
                                // a <= x < b OR a < x < b
                                r =
                                        Range.range(
                                                r.lowerEndpoint(),
                                                r.lowerBoundType(),
                                                v0,
                                                BoundType.OPEN);
                            } else {
                                // x < b
                                r = Range.lessThan(v0);
                            }

                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false);
                            }

                            // remove prev upper bound
                            removeUpperBound = true;
                        } else {
                            // Remove this term as it is contained in current upper bound
                            replaceLast(terms, term, trueLiteral);
                        }
                        break;
                    }
                case LESS_THAN_OR_EQUAL:
                    {
                        int comparisonResult = -1;
                        if (r.hasUpperBound()) {
                            comparisonResult = v0.compareTo(r.upperEndpoint());
                        }
                        if (comparisonResult < 0) {
                            // 1) No upper bound, or
                            // 2) New upper bound is lower than old upper bound
                            if (r.hasLowerBound()) {
                                if (v0.compareTo(r.lowerEndpoint()) < 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false);
                                }
                                // a <= x <= b OR a < x <= b
                                r =
                                        Range.range(
                                                r.lowerEndpoint(),
                                                r.lowerBoundType(),
                                                v0,
                                                BoundType.CLOSED);
                            } else {
                                // x <= b
                                r = Range.atMost(v0);
                            }

                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false);
                            }

                            // remove prev upper bound
                            removeUpperBound = true;
                        } else {
                            // Remove this term as it is contained in current upper bound
                            replaceLast(terms, term, trueLiteral);
                        }
                        break;
                    }
                case GREATER_THAN:
                    {
                        int comparisonResult = 0;
                        if (r.hasLowerBound()) {
                            comparisonResult = v0.compareTo(r.lowerEndpoint());
                        }
                        if (comparisonResult >= 0) {
                            // 1) No lower bound, or
                            // 2) We need to open the lower bound, or
                            // 3) New lower bound is greater than old lower bound
                            if (r.hasUpperBound()) {
                                if (v0.compareTo(r.upperEndpoint()) >= 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false);
                                }
                                // a < x <= b OR a < x < b
                                r =
                                        Range.range(
                                                v0,
                                                BoundType.OPEN,
                                                r.upperEndpoint(),
                                                r.upperBoundType());
                            } else {
                                // x > a
                                r = Range.greaterThan(v0);
                            }

                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false);
                            }

                            // remove prev lower bound
                            removeLowerBound = true;
                        } else {
                            // Remove this term as it is contained in current lower bound
                            replaceLast(terms, term, trueLiteral);
                        }
                        break;
                    }
                case GREATER_THAN_OR_EQUAL:
                    {
                        int comparisonResult = 1;
                        if (r.hasLowerBound()) {
                            comparisonResult = v0.compareTo(r.lowerEndpoint());
                        }
                        if (comparisonResult > 0) {
                            // 1) No lower bound, or
                            // 2) New lower bound is greater than old lower bound
                            if (r.hasUpperBound()) {
                                if (v0.compareTo(r.upperEndpoint()) > 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false);
                                }
                                // a <= x <= b OR a <= x < b
                                r =
                                        Range.range(
                                                v0,
                                                BoundType.CLOSED,
                                                r.upperEndpoint(),
                                                r.upperBoundType());
                            } else {
                                // x >= a
                                r = Range.atLeast(v0);
                            }

                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false);
                            }

                            // remove prev lower bound
                            removeLowerBound = true;
                        } else {
                            // Remove this term as it is contained in current lower bound
                            replaceLast(terms, term, trueLiteral);
                        }
                        break;
                    }
                default:
                    throw new AssertionError();
            }
            if (removeUpperBound) {
                ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
                for (RexNode e : p.right) {
                    if (isUpperBound(e)) {
                        replaceLast(terms, e, trueLiteral);
                    } else {
                        newBounds.add(e);
                    }
                }
                newBounds.add(term);
                rangeTerms.put(ref, Pair.of(r, newBounds.build()));
            } else if (removeLowerBound) {
                ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
                for (RexNode e : p.right) {
                    if (isLowerBound(e)) {
                        replaceLast(terms, e, trueLiteral);
                    } else {
                        newBounds.add(e);
                    }
                }
                newBounds.add(term);
                rangeTerms.put(ref, Pair.of(r, newBounds.build()));
            }
        }
        // Default
        return null;
    }

    private static <C extends Comparable<C>> Range<C> range(SqlKind comparison, C c) {
        switch (comparison) {
            case EQUALS:
                return Range.singleton(c);
            case LESS_THAN:
                return Range.lessThan(c);
            case LESS_THAN_OR_EQUAL:
                return Range.atMost(c);
            case GREATER_THAN:
                return Range.greaterThan(c);
            case GREATER_THAN_OR_EQUAL:
                return Range.atLeast(c);
            default:
                throw new AssertionError();
        }
    }

    private static <C extends Comparable<C>> RangeSet<C> rangeSet(SqlKind comparison, C c) {
        switch (comparison) {
            case EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return ImmutableRangeSet.of(range(comparison, c));
            case NOT_EQUALS:
                return ImmutableRangeSet.<C>builder()
                        .add(range(SqlKind.LESS_THAN, c))
                        .add(range(SqlKind.GREATER_THAN, c))
                        .build();
            default:
                throw new AssertionError();
        }
    }

    /** Marker interface for predicates (expressions that evaluate to BOOLEAN). */
    private interface Predicate {
        /** Wraps an expression in a Predicate or returns null. */
        static @Nullable Predicate of(RexNode t) {
            final Predicate p = Comparison.of(t);
            if (p != null) {
                return p;
            }
            return IsPredicate.of(t);
        }

        /** Returns whether this predicate can be used while simplifying other OR operands. */
        default boolean allowedInOr(RelOptPredicateList predicates) {
            return true;
        }
    }

    /** Visitor which finds all inputs used by an expressions. */
    private static class VariableCollector extends RexVisitorImpl<Void> {
        private final Set<RexInputRef> refs = new HashSet<>();

        VariableCollector() {
            super(true);
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            refs.add(inputRef);
            return super.visitInputRef(inputRef);
        }
    }

    /**
     * Represents a simple Comparison.
     *
     * <p>Left hand side is a {@link RexNode}, right hand side is a literal.
     */
    private static class Comparison implements Predicate {
        final RexNode ref;
        final SqlKind kind;
        final RexLiteral literal;

        private Comparison(RexNode ref, SqlKind kind, RexLiteral literal) {
            this.ref = requireNonNull(ref, "ref");
            this.kind = requireNonNull(kind, "kind");
            this.literal = requireNonNull(literal, "literal");
        }

        /**
         * Creates a comparison, between a {@link RexInputRef} or {@link RexFieldAccess} or
         * deterministic {@link RexCall} and a literal.
         */
        static @Nullable Comparison of(RexNode e) {
            return of(
                    e,
                    node ->
                            RexUtil.isReferenceOrAccess(node, true)
                                    || RexUtil.isDeterministic(node));
        }

        /** Creates a comparison, or returns null. */
        static @Nullable Comparison of(
                RexNode e, java.util.function.Predicate<RexNode> nodePredicate) {
            switch (e.getKind()) {
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case GREATER_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    final RexCall call = (RexCall) e;
                    final RexNode left = call.getOperands().get(0);
                    final RexNode right = call.getOperands().get(1);
                    switch (right.getKind()) {
                        case LITERAL:
                            if (nodePredicate.test(left)) {
                                return new Comparison(left, e.getKind(), (RexLiteral) right);
                            }
                            break;
                        default:
                            break;
                    }
                    switch (left.getKind()) {
                        case LITERAL:
                            if (nodePredicate.test(right)) {
                                return new Comparison(
                                        right, e.getKind().reverse(), (RexLiteral) left);
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
            return null;
        }

        @Override
        public boolean allowedInOr(RelOptPredicateList predicates) {
            // if ref is not a 'loss-less' cast then can't be allowed to be used
            // while simplifying other OR operands
            if (ref.isA(SqlKind.CAST) && !RexUtil.isLosslessCast(ref)) {
                return false;
            }
            return !ref.getType().isNullable() || predicates.isEffectivelyNotNull(ref);
        }
    }

    /** Represents an IS Predicate. */
    private static class IsPredicate implements Predicate {
        final RexNode ref;
        final SqlKind kind;

        private IsPredicate(RexNode ref, SqlKind kind) {
            this.ref = requireNonNull(ref, "ref");
            this.kind = requireNonNull(kind, "kind");
        }

        /** Creates an IS predicate, or returns null. */
        static @Nullable IsPredicate of(RexNode e) {
            switch (e.getKind()) {
                case IS_NULL:
                case IS_NOT_NULL:
                    RexNode pA = ((RexCall) e).getOperands().get(0);
                    if (!(RexUtil.isReferenceOrAccess(pA, true) || RexUtil.isDeterministic(pA))) {
                        return null;
                    }
                    return new IsPredicate(pA, e.getKind());
                default:
                    break;
            }
            return null;
        }
    }

    private static boolean isUpperBound(final RexNode e) {
        final List<RexNode> operands;
        switch (e.getKind()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                operands = ((RexCall) e).getOperands();
                return RexUtil.isReferenceOrAccess(operands.get(0), true)
                        && operands.get(1).isA(SqlKind.LITERAL);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                operands = ((RexCall) e).getOperands();
                return RexUtil.isReferenceOrAccess(operands.get(1), true)
                        && operands.get(0).isA(SqlKind.LITERAL);
            default:
                return false;
        }
    }

    private static boolean isLowerBound(final RexNode e) {
        final List<RexNode> operands;
        switch (e.getKind()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                operands = ((RexCall) e).getOperands();
                return RexUtil.isReferenceOrAccess(operands.get(1), true)
                        && operands.get(0).isA(SqlKind.LITERAL);
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                operands = ((RexCall) e).getOperands();
                return RexUtil.isReferenceOrAccess(operands.get(0), true)
                        && operands.get(1).isA(SqlKind.LITERAL);
            default:
                return false;
        }
    }

    /**
     * Combines predicates AND, optimizes, and returns null if the result is always false.
     *
     * <p>The expression is simplified on the assumption that an UNKNOWN value is always treated as
     * FALSE. Therefore the simplified expression may sometimes evaluate to FALSE where the original
     * expression would evaluate to UNKNOWN.
     *
     * @param predicates Filter condition predicates
     * @return simplified conjunction of predicates for the filter, null if always false
     */
    public @Nullable RexNode simplifyFilterPredicates(Iterable<? extends RexNode> predicates) {
        final RexNode simplifiedAnds =
                withPredicateElimination(Bug.CALCITE_2401_FIXED)
                        .simplifyUnknownAsFalse(RexUtil.composeConjunction(rexBuilder, predicates));
        if (simplifiedAnds.isAlwaysFalse()) {
            return null;
        }

        // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
        // nullable and not-nullable conditions, but a CAST might get in the way of
        // other rewrites.
        return removeNullabilityCast(simplifiedAnds);
    }

    /**
     * Replaces the last occurrence of one specified value in a list with another.
     *
     * <p>Does not change the size of the list.
     *
     * <p>Returns whether the value was found.
     */
    private static <E> boolean replaceLast(List<E> list, E oldVal, E newVal) {
        @SuppressWarnings("argument.type.incompatible")
        final int index = list.lastIndexOf(oldVal);
        if (index < 0) {
            return false;
        }
        list.set(index, newVal);
        return true;
    }

    /** Gathers expressions that can be converted into {@link Sarg search arguments}. */
    static class SargCollector {
        final Map<RexNode, RexSargBuilder> map = new HashMap<>();
        private final RexBuilder rexBuilder;
        private final boolean negate;

        /**
         * Count of the new terms after converting all the operands to {@code SEARCH} on a {@link
         * Sarg}. It is used to decide whether the new terms are simpler.
         */
        private int newTermsCount;

        SargCollector(RexBuilder rexBuilder, boolean negate) {
            this.rexBuilder = rexBuilder;
            this.negate = negate;
        }

        /**
         * Accepts an expression and converts it to a Sarg if possible.
         *
         * @param term the expression to convert
         * @param newTerms the list holding the result of the conversion or the original term if it
         *     cannot be converted
         */
        private void accept(RexNode term, List<RexNode> newTerms) {
            if (!accept_(term, newTerms)) {
                newTerms.add(term);
            }
            newTermsCount = newTerms.size();
        }

        /**
         * Accepts an expression and converts it to a Sarg if possible. Only certain kinds of
         * expressions can be converted to Sargs.
         *
         * @param e the expression to convert
         * @param newTerms the list to which the Sarg will be added if the expression is accepted
         * @return true if the expression can be converted to a Sarg, false otherwise
         */
        private boolean accept_(RexNode e, List<RexNode> newTerms) {
            switch (e.getKind()) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case EQUALS:
                case NOT_EQUALS:
                case SEARCH:
                case IS_NOT_DISTINCT_FROM:
                case IS_DISTINCT_FROM:
                    return accept2(
                            ((RexCall) e).operands.get(0),
                            ((RexCall) e).operands.get(1),
                            e.getKind(),
                            newTerms);
                case IS_NULL:
                case IS_NOT_NULL:
                    final RexNode arg = ((RexCall) e).operands.get(0);
                    return accept1(arg, e.getKind(), newTerms);
                default:
                    return false;
            }
        }

        /**
         * Accepts two operands from a binary comparison operator and converts them to a Sarg if
         * possible. Only comparisons between a literal and a deterministic expression can be
         * converted. Simplifications with non-deterministic expressions are generally avoided to
         * ensure consistent results.
         *
         * @param left the left operand of the comparison
         * @param right the right operand of the comparison
         * @param kind the kind of comparison operator
         * @param newTerms the list to which the Sarg will be added if accepted
         * @return true if the operands can be converted to a Sarg, false otherwise
         */
        private boolean accept2(RexNode left, RexNode right, SqlKind kind, List<RexNode> newTerms) {
            if (right.isA(SqlKind.LITERAL) && RexUtil.isDeterministic(left)) {
                return accept2b(left, kind, (RexLiteral) right, newTerms);
            }
            if (left.isA(SqlKind.LITERAL) && RexUtil.isDeterministic(right)) {
                return accept2b(right, kind.reverse(), (RexLiteral) left, newTerms);
            }
            return false;
        }

        private static <E> E addFluent(List<? super E> list, E e) {
            list.add(e);
            return e;
        }

        /**
         * Accepts an operand from a null predicate and converts it to a Sarg.
         *
         * @param e the operand of the null predicate
         * @param kind the kind of the null predicate
         * @param newTerms the list to which the Sarg is added
         * @return true since the operand is always converted to a Sarg
         */
        private boolean accept1(RexNode e, SqlKind kind, List<RexNode> newTerms) {
            final RexSargBuilder b =
                    map.computeIfAbsent(
                            e,
                            e2 -> addFluent(newTerms, new RexSargBuilder(e2, rexBuilder, negate)));
            switch (negate ? kind.negate() : kind) {
                case IS_NULL:
                    b.nullAs = b.nullAs.or(TRUE);
                    return true;
                case IS_NOT_NULL:
                    b.nullAs = b.nullAs.or(FALSE);
                    b.addAll();
                    return true;
                default:
                    throw new AssertionError("unexpected " + kind);
            }
        }

        /**
         * Accepts two operands from a binary comparison operator and converts them to a Sarg when
         * the literal is not null. The conversion depends on the kind of comparison operator and
         * only certain operators are supported.
         *
         * @param e any kind of deterministic expression
         * @param kind the kind of comparison operator
         * @param literal the literal operand of the comparison
         * @param newTerms the list to which the Sarg is added if accepted
         * @return false if the literal operand is null, true otherwise
         */
        private boolean accept2b(
                RexNode e, SqlKind kind, RexLiteral literal, List<RexNode> newTerms) {
            if (literal.getValue() == null) {
                // Cannot include expressions 'x > NULL' in a Sarg. Comparing to a NULL
                // literal is a daft thing to do, because it always returns UNKNOWN. It
                // is better handled by other simplifications.
                return false;
            }
            final RexSargBuilder b =
                    map.computeIfAbsent(
                            e,
                            e2 -> addFluent(newTerms, new RexSargBuilder(e2, rexBuilder, negate)));
            if (negate) {
                kind = kind.negateNullSafe();
            }
            final Comparable value = requireNonNull(literal.getValueAs(Comparable.class), "value");
            switch (kind) {
                case LESS_THAN:
                    b.addRange(Range.lessThan(value), literal.getType());
                    return true;
                case LESS_THAN_OR_EQUAL:
                    b.addRange(Range.atMost(value), literal.getType());
                    return true;
                case GREATER_THAN:
                    b.addRange(Range.greaterThan(value), literal.getType());
                    return true;
                case GREATER_THAN_OR_EQUAL:
                    b.addRange(Range.atLeast(value), literal.getType());
                    return true;
                case EQUALS:
                    b.addRange(Range.singleton(value), literal.getType());
                    return true;
                case IS_NOT_DISTINCT_FROM:
                    b.addRange(Range.singleton(value), literal.getType(), FALSE);
                    return true;
                case NOT_EQUALS:
                    b.addRange(Range.lessThan(value), literal.getType());
                    b.addRange(Range.greaterThan(value), literal.getType());
                    return true;
                case IS_DISTINCT_FROM:
                    b.addRange(Range.lessThan(value), literal.getType(), TRUE);
                    b.addRange(Range.greaterThan(value), literal.getType(), TRUE);
                    return true;
                case SEARCH:
                    final Sarg sarg = (Sarg) value;
                    b.addSarg(sarg, negate, literal.getType());
                    return true;
                default:
                    throw new AssertionError("unexpected " + kind);
            }
        }

        /** Returns whether it is worth to fix and convert to {@code SEARCH} calls. */
        boolean needToFix() {
            // Fix and converts to SEARCH if:
            // 1. A Sarg has complexity greater than 1;
            // 2. A Sarg was merged with another Sarg or range;
            // 3. The terms are reduced as simpler Sarg points;
            // 4. The terms are reduced as simpler Sarg comparison.

            // Ignore 'negate' just to be compatible with previous versions of this
            // method. "build().complexity()" would be a better estimate, if we could
            // switch to it breaking lots of plans.
            final Collection<RexSargBuilder> builders = map.values();
            return builders.stream().anyMatch(b -> b.build(false).complexity() > 1 || b.mergedSarg)
                    || newTermsCount == 1 && builders.stream().allMatch(b -> simpleSarg(b.build()));
        }

        /**
         * Returns whether this Sarg can be expanded to more simple form, e.g. the IN call or single
         * comparison.
         */
        private static boolean simpleSarg(Sarg sarg) {
            return sarg.isPoints()
                    || RangeSets.isOpenInterval(sarg.rangeSet)
                    || sarg.isComplementedPoints();
        }

        /**
         * If a term is a call to {@code SEARCH} on a {@link RexSargBuilder}, converts it to a
         * {@code SEARCH} on a {@link Sarg}.
         */
        static RexNode fix(RexBuilder rexBuilder, RexNode term, RexUnknownAs unknownAs) {
            if (term instanceof RexSargBuilder) {
                final RexSargBuilder sargBuilder = (RexSargBuilder) term;
                final Sarg sarg = sargBuilder.build();
                boolean isSmall = sarg.complexity() <= 1 || sarg.isAll() || sarg.isNone();
                if (isSmall && simpleSarg(sarg)) {
                    // Expand small sargs into comparisons in order to avoid plan changes
                    // and better readability.
                    return RexUtil.sargRef(
                            rexBuilder, sargBuilder.ref, sarg, term.getType(), unknownAs);
                }
                return rexBuilder.makeCall(
                        SqlStdOperatorTable.SEARCH,
                        sargBuilder.ref,
                        rexBuilder.makeSearchArgumentLiteral(sarg, term.getType()));
            }
            return term;
        }
    }

    /**
     * Equivalent to a {@link RexLiteral} whose value is a {@link Sarg}, but mutable, so that the
     * Sarg can be expanded as {@link SargCollector} traverses a list of OR or AND terms.
     *
     * <p>The {@link SargCollector#fix} method converts it to an immutable literal.
     *
     * <p>The {@link #nullAs} field will become {@link Sarg#nullAs}, as follows:
     *
     * <ul>
     *   <li>If there is at least one term that returns TRUE when the argument is NULL, then the
     *       overall value will be TRUE; failing that,
     *   <li>if there is at least one term that returns UNKNOWN when the argument is NULL, then the
     *       overall value will be UNKNOWN; failing that,
     *   <li>the value will be FALSE.
     * </ul>
     *
     * <p>This is analogous to the behavior of OR in three-valued logic: {@code TRUE OR UNKNOWN OR
     * FALSE} returns {@code TRUE}; {@code UNKNOWN OR FALSE OR UNKNOWN} returns {@code UNKNOWN};
     * {@code FALSE OR FALSE} returns {@code FALSE}.
     */
    private static class RexSargBuilder extends RexNode {
        final RexNode ref;
        final RexBuilder rexBuilder;
        final boolean negate;
        final List<RelDataType> types = new ArrayList<>();
        final RangeSet<Comparable> rangeSet = TreeRangeSet.create();
        boolean hasSarg;
        boolean mergedSarg;
        RexUnknownAs nullAs = FALSE;

        RexSargBuilder(RexNode ref, RexBuilder rexBuilder, boolean negate) {
            this.ref = requireNonNull(ref, "ref");
            this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder");
            this.negate = negate;
        }

        @Override
        public String toString() {
            return "SEARCH("
                    + ref
                    + ", "
                    + (negate ? "NOT " : "")
                    + rangeSet
                    + "; NULL AS "
                    + nullAs
                    + ")";
        }

        <C extends Comparable<C>> Sarg<C> build() {
            return build(negate);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        <C extends Comparable<C>> Sarg<C> build(boolean negate) {
            final RangeSet<C> r = (RangeSet) this.rangeSet;
            if (negate) {
                return Sarg.of(nullAs.negate(), r.complement());
            } else {
                return Sarg.of(nullAs, r);
            }
        }

        @Override
        public RelDataType getType() {
            if (this.types.isEmpty()) {
                // Expression is "x IS NULL"
                return ref.getType();
            }
            final List<RelDataType> distinctTypes = Util.distinctList(this.types);
            return requireNonNull(
                    rexBuilder.typeFactory.leastRestrictive(distinctTypes),
                    () -> "Can't find leastRestrictive type among " + distinctTypes);
        }

        @Override
        public <R> R accept(RexVisitor<R> visitor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(@Nullable Object obj) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        void addAll() {
            rangeSet.add(Range.all());
        }

        void addRange(Range<Comparable> range, RelDataType type) {
            addRange(range, type, UNKNOWN);
        }

        void addRange(Range<Comparable> range, RelDataType type, RexUnknownAs unknownAs) {
            types.add(type);
            rangeSet.add(range);
            mergedSarg |= hasSarg;
            nullAs = nullAs.or(unknownAs);
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        void addSarg(Sarg sarg, boolean negate, RelDataType type) {
            final RangeSet r;
            final RexUnknownAs nullAs;
            if (negate) {
                r = sarg.rangeSet.complement();
                nullAs = sarg.nullAs.negate();
            } else {
                r = sarg.rangeSet;
                nullAs = sarg.nullAs;
            }
            types.add(type);
            rangeSet.addAll(r);
            mergedSarg |= !rangeSet.isEmpty();
            hasSarg = true;
            switch (nullAs) {
                case TRUE:
                    this.nullAs = this.nullAs.or(TRUE);
                    break;
                case FALSE:
                    this.nullAs = this.nullAs.or(FALSE);
                    break;
                case UNKNOWN:
                    this.nullAs = this.nullAs.or(UNKNOWN);
                    break;
            }
        }
    }
}
