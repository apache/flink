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

package org.apache.flink.state.table;

import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Converts {@link ResolvedExpression} key filter predicates into {@link SavepointKeyFilter}
 * instances that can be used to prune key groups and key iterations during savepoint reads.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class SavepointFilterTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointFilterTranslator.class);

    private static final Map<
                    FunctionDefinition,
                    BiFunction<SavepointFilterTranslator, CallExpression, KeyFilterPlan>>
            FILTERS =
                    Map.of(
                            BuiltInFunctionDefinitions.EQUALS,
                            SavepointFilterTranslator::fromEquals,
                            BuiltInFunctionDefinitions.OR,
                            SavepointFilterTranslator::fromOr,
                            BuiltInFunctionDefinitions.AND,
                            SavepointFilterTranslator::fromAnd,
                            BuiltInFunctionDefinitions.BETWEEN,
                            SavepointFilterTranslator::fromBetween,
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            (t, call) -> t.fromComparison(call, Comparison.GT),
                            BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                            (t, call) -> t.fromComparison(call, Comparison.GTE),
                            BuiltInFunctionDefinitions.LESS_THAN,
                            (t, call) -> t.fromComparison(call, Comparison.LT),
                            BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                            (t, call) -> t.fromComparison(call, Comparison.LTE));

    private final int keyColumnIndex;
    private final DataType keyColumnType;

    SavepointFilterTranslator(int keyColumnIndex, DataType keyColumnType) {
        this.keyColumnIndex = keyColumnIndex;
        this.keyColumnType = keyColumnType;
    }

    Result apply(List<ResolvedExpression> filters) {
        final List<ResolvedExpression> accepted = new ArrayList<>();
        final List<ResolvedExpression> remaining = new ArrayList<>();

        KeyFilterPlan keyFilter = null;
        for (ResolvedExpression filter : filters) {
            KeyFilterPlan extracted = extractFilter(filter);
            if (extracted == null) {
                remaining.add(filter);
                continue;
            }

            keyFilter = keyFilter == null ? extracted : keyFilter.intersect(extracted);
            accepted.add(filter);
        }

        return new Result(
                accepted, remaining, keyFilter == null ? null : keyFilter.toSavepointKeyFilter());
    }

    /**
     * Shared {@code SupportsFilterPushDown.applyFilters} implementation for {@link
     * SavepointDynamicTableSource} and {@link FlattenedSavepointDynamicTableSource}: translates
     * {@code filters} against the key column at {@code keyColumnIndex}, reports the extracted key
     * filter to {@code keyFilterSetter}, and returns the accepted/remaining split.
     */
    static SupportsFilterPushDown.Result applyKeyColumnFilters(
            int keyColumnIndex,
            RowType rowType,
            List<ResolvedExpression> filters,
            Consumer<SavepointKeyFilter> keyFilterSetter) {
        Result result =
                new SavepointFilterTranslator(
                                keyColumnIndex,
                                TypeConversions.fromLogicalToDataType(
                                        rowType.getTypeAt(keyColumnIndex)))
                        .apply(filters);
        keyFilterSetter.accept(result.keyFilter());
        return SupportsFilterPushDown.Result.of(result.accepted(), result.remaining());
    }

    @Nullable
    private KeyFilterPlan extractFilter(ResolvedExpression expr) {
        final BiFunction<SavepointFilterTranslator, CallExpression, KeyFilterPlan> extractor =
                expr instanceof CallExpression
                        ? FILTERS.get(((CallExpression) expr).getFunctionDefinition())
                        : null;
        if (extractor == null) {
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into savepoint key filter.", expr);
            return null;
        }
        return extractor.apply(this, (CallExpression) expr);
    }

    // -------------------------------------------------------------------------
    //  Equality
    // -------------------------------------------------------------------------

    @Nullable
    private KeyFilterPlan fromEquals(CallExpression call) {
        if (!isBinaryValid(call)) {
            return null;
        }
        ResolvedExpression left = call.getResolvedChildren().get(0);
        ResolvedExpression right = call.getResolvedChildren().get(1);

        Object value = null;
        if (isKeyField(left)) {
            value = extractValue(right);
        } else if (isKeyField(right)) {
            value = extractValue(left);
        }

        if (value == null) {
            return null;
        }
        return KeyFilterPlan.exact(value);
    }

    @Nullable
    private KeyFilterPlan fromOr(CallExpression call) {
        Set<Object> keys = new HashSet<>();
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            KeyFilterPlan sub = extractFilter(arg);
            if (sub == null) {
                return null;
            }
            Set<Object> subKeys = sub.exactKeys;
            // OR can only absorb finite key sets; a range branch cannot be merged via union.
            if (subKeys == null) {
                return null;
            }
            keys.addAll(subKeys);
        }
        return KeyFilterPlan.exact(keys);
    }

    // -------------------------------------------------------------------------
    //  Range
    // -------------------------------------------------------------------------

    @Nullable
    private KeyFilterPlan fromAnd(CallExpression call) {
        KeyFilterPlan merged = null;
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            KeyFilterPlan sub = extractFilter(arg);
            // AND only absorbs range filters; exact (or null) children break pushdown.
            if (sub == null || sub.exactKeys != null) {
                return null;
            }
            merged = (merged == null) ? sub : merged.intersect(sub);
            if (merged.isEmpty()) {
                return merged;
            }
        }
        return merged;
    }

    @Nullable
    private KeyFilterPlan fromBetween(CallExpression call) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 3) {
            return null;
        }
        ResolvedExpression valueExpr = args.get(0);
        ResolvedExpression lowerExpr = args.get(1);
        ResolvedExpression upperExpr = args.get(2);

        if (!isKeyField(valueExpr)) {
            return null;
        }

        Object lower = extractValue(lowerExpr);
        Object upper = extractValue(upperExpr);
        if (lower == null || upper == null) {
            return null;
        }
        if (!(lower instanceof Comparable) || !(upper instanceof Comparable)) {
            LOG.debug(
                    "BETWEEN predicate on non-comparable key type {} cannot be pushed into savepoint key filter.",
                    lower.getClass().getName());
            return null;
        }
        return KeyFilterPlan.range((Comparable) lower, true, (Comparable) upper, true);
    }

    @Nullable
    private KeyFilterPlan fromComparison(CallExpression call, Comparison cmp) {
        if (!isBinaryValid(call)) {
            return null;
        }
        ResolvedExpression left = call.getResolvedChildren().get(0);
        ResolvedExpression right = call.getResolvedChildren().get(1);

        final boolean keyOnLeft = isKeyField(left);
        final boolean keyOnRight = isKeyField(right);
        if (!keyOnLeft && !keyOnRight) {
            return null;
        }
        Object bound = extractValue(keyOnLeft ? right : left);
        if (bound == null) {
            return null;
        }
        if (!(bound instanceof Comparable)) {
            LOG.debug(
                    "Range predicate on non-comparable key type {} cannot be pushed into savepoint key filter.",
                    bound.getClass().getName());
            return null;
        }
        Comparable b = (Comparable) bound;
        Comparison keyLeftCmp = keyOnLeft ? cmp : cmp.flip();
        switch (keyLeftCmp) {
            case GT:
                return KeyFilterPlan.range(b, false, null, true);
            case GTE:
                return KeyFilterPlan.range(b, true, null, true);
            case LT:
                return KeyFilterPlan.range(null, true, b, false);
            case LTE:
                return KeyFilterPlan.range(null, true, b, true);
            default:
                throw new IllegalStateException("Unknown Comparison: " + keyLeftCmp);
        }
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private static boolean isBinaryValid(CallExpression call) {
        return call.getResolvedChildren().size() == 2;
    }

    private boolean isKeyField(ResolvedExpression expr) {
        return expr instanceof FieldReferenceExpression
                && ((FieldReferenceExpression) expr).getFieldIndex() == keyColumnIndex;
    }

    @Nullable
    private Object extractValue(ResolvedExpression expr) {
        if (!(expr instanceof ValueLiteralExpression)) {
            LOG.debug("Refusing pushdown: predicate operand [{}] is not a literal value.", expr);
            return null;
        }
        ValueLiteralExpression literal = (ValueLiteralExpression) expr;
        Class<?> literalClass = literal.getOutputDataType().getConversionClass();
        Object value = literal.getValueAs(literalClass).orElse(null);
        if (value == null) {
            LOG.debug(
                    "Refusing pushdown: literal {} of type {} cannot be read as its conversion"
                            + " class {}.",
                    literal,
                    literal.getOutputDataType(),
                    literalClass.getName());
            return null;
        }
        return widenToKeyType(value);
    }

    @Nullable
    private Object widenToKeyType(Object value) {
        Class<?> keyClass = keyColumnType.getConversionClass();
        if (keyClass.isInstance(value)) {
            return value;
        }
        if (value instanceof Number) {
            if (keyClass == Long.class) {
                return ((Number) value).longValue();
            }
            if (keyClass == Double.class) {
                return ((Number) value).doubleValue();
            }
        }
        LOG.debug(
                "Refusing pushdown: literal value {} of type {} cannot be widened to key type {}.",
                value,
                value.getClass().getName(),
                keyColumnType);
        return null;
    }

    static final class Result {
        private final List<ResolvedExpression> accepted;
        private final List<ResolvedExpression> remaining;
        @Nullable private final SavepointKeyFilter keyFilter;

        private Result(
                List<ResolvedExpression> accepted,
                List<ResolvedExpression> remaining,
                @Nullable SavepointKeyFilter keyFilter) {
            this.accepted = accepted;
            this.remaining = remaining;
            this.keyFilter = keyFilter;
        }

        List<ResolvedExpression> accepted() {
            return accepted;
        }

        List<ResolvedExpression> remaining() {
            return remaining;
        }

        @Nullable
        SavepointKeyFilter keyFilter() {
            return keyFilter;
        }
    }

    private static final class KeyFilterPlan {
        @Nullable private final Set<Object> exactKeys;
        @Nullable private final Comparable lower;
        private final boolean lowerInclusive;
        @Nullable private final Comparable upper;
        private final boolean upperInclusive;

        private KeyFilterPlan(
                @Nullable Set<Object> exactKeys,
                @Nullable Comparable lower,
                boolean lowerInclusive,
                @Nullable Comparable upper,
                boolean upperInclusive) {
            this.exactKeys = exactKeys;
            this.lower = lower;
            this.lowerInclusive = lowerInclusive;
            this.upper = upper;
            this.upperInclusive = upperInclusive;
        }

        private static KeyFilterPlan exact(Object key) {
            return exact(Set.of(key));
        }

        private static KeyFilterPlan exact(Set<Object> keys) {
            return new KeyFilterPlan(Set.copyOf(keys), null, true, null, true);
        }

        private static KeyFilterPlan range(
                @Nullable Comparable lower,
                boolean lowerInclusive,
                @Nullable Comparable upper,
                boolean upperInclusive) {
            return new KeyFilterPlan(null, lower, lowerInclusive, upper, upperInclusive);
        }

        private KeyFilterPlan intersect(KeyFilterPlan other) {
            Set<Object> newExactKeys = exactKeys;
            if (newExactKeys == null) {
                newExactKeys = other.exactKeys;
            } else if (other.exactKeys != null) {
                newExactKeys = new HashSet<>(newExactKeys);
                newExactKeys.retainAll(other.exactKeys);
            }

            Comparable newLower = lower;
            boolean newLowerInclusive = lowerInclusive;
            if (other.lower != null) {
                final int comparison = newLower == null ? -1 : newLower.compareTo(other.lower);
                if (newLower == null || comparison < 0) {
                    newLower = other.lower;
                    newLowerInclusive = other.lowerInclusive;
                } else if (comparison == 0) {
                    newLowerInclusive &= other.lowerInclusive;
                }
            }

            Comparable newUpper = upper;
            boolean newUpperInclusive = upperInclusive;
            if (other.upper != null) {
                final int comparison = newUpper == null ? 1 : newUpper.compareTo(other.upper);
                if (newUpper == null || comparison > 0) {
                    newUpper = other.upper;
                    newUpperInclusive = other.upperInclusive;
                } else if (comparison == 0) {
                    newUpperInclusive &= other.upperInclusive;
                }
            }

            return new KeyFilterPlan(
                    newExactKeys, newLower, newLowerInclusive, newUpper, newUpperInclusive);
        }

        private boolean isEmpty() {
            if (exactKeys != null && exactKeys.isEmpty()) {
                return true;
            }
            if (lower == null || upper == null) {
                return false;
            }
            final int comparison = lower.compareTo(upper);
            return comparison > 0 || (comparison == 0 && (!lowerInclusive || !upperInclusive));
        }

        private SavepointKeyFilter<Object> toSavepointKeyFilter() {
            if (isEmpty()) {
                return SavepointKeyFilter.exact(Set.of());
            }
            if (exactKeys != null && lower == null && upper == null) {
                return SavepointKeyFilter.exact(exactKeys);
            }
            final SavepointKeyFilter<Object> rangeFilter =
                    SavepointKeyFilter.range(lower, lowerInclusive, upper, upperInclusive);
            if (exactKeys == null) {
                return rangeFilter;
            }
            final Set<Object> retainedKeys = new HashSet<>(exactKeys);
            retainedKeys.removeIf(key -> !rangeFilter.test(key));
            return SavepointKeyFilter.exact(retainedKeys);
        }
    }

    private enum Comparison {
        GT,
        GTE,
        LT,
        LTE;

        Comparison flip() {
            switch (this) {
                case GT:
                    return LT;
                case GTE:
                    return LTE;
                case LT:
                    return GT;
                case LTE:
                    return GTE;
                default:
                    throw new IllegalStateException("Unknown Comparison: " + this);
            }
        }
    }
}
