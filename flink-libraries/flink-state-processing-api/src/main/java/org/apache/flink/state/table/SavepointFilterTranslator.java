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

import java.math.BigDecimal;
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
    private static final long FLOAT_EXACT_INTEGER_LIMIT = 1L << 24;
    private static final long DOUBLE_EXACT_INTEGER_LIMIT = 1L << 53;

    private static final Map<
                    FunctionDefinition,
                    BiFunction<SavepointFilterTranslator, CallExpression, KeyFilterPlan>>
            FILTERS =
                    Map.ofEntries(
                            Map.entry(
                                    BuiltInFunctionDefinitions.EQUALS,
                                    SavepointFilterTranslator::fromEquals),
                            Map.entry(
                                    BuiltInFunctionDefinitions.NOT_EQUALS,
                                    SavepointFilterTranslator::fromNotEquals),
                            Map.entry(
                                    BuiltInFunctionDefinitions.NOT,
                                    SavepointFilterTranslator::fromNot),
                            Map.entry(
                                    BuiltInFunctionDefinitions.OR,
                                    SavepointFilterTranslator::fromOr),
                            Map.entry(
                                    BuiltInFunctionDefinitions.AND,
                                    SavepointFilterTranslator::fromAnd),
                            Map.entry(
                                    BuiltInFunctionDefinitions.BETWEEN,
                                    SavepointFilterTranslator::fromBetween),
                            Map.entry(
                                    BuiltInFunctionDefinitions.NOT_BETWEEN,
                                    SavepointFilterTranslator::fromNotBetween),
                            Map.entry(
                                    BuiltInFunctionDefinitions.GREATER_THAN,
                                    (t, call) -> t.fromComparison(call, Comparison.GT)),
                            Map.entry(
                                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                    (t, call) -> t.fromComparison(call, Comparison.GTE)),
                            Map.entry(
                                    BuiltInFunctionDefinitions.LESS_THAN,
                                    (t, call) -> t.fromComparison(call, Comparison.LT)),
                            Map.entry(
                                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                    (t, call) -> t.fromComparison(call, Comparison.LTE)));

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
        final Object value = extractBinaryKeyValue(call);
        return value == null ? null : KeyFilterPlan.exact(value);
    }

    @Nullable
    private KeyFilterPlan fromNotEquals(CallExpression call) {
        final Object value = extractBinaryKeyValue(call);
        return value == null ? null : KeyFilterPlan.not(KeyFilterPlan.exact(value));
    }

    @Nullable
    private KeyFilterPlan fromNot(CallExpression call) {
        if (call.getResolvedChildren().size() != 1) {
            return null;
        }
        final KeyFilterPlan inner = extractFilter(call.getResolvedChildren().get(0));
        return inner == null ? null : KeyFilterPlan.not(inner);
    }

    @Nullable
    private KeyFilterPlan fromOr(CallExpression call) {
        KeyFilterPlan merged = null;
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            final KeyFilterPlan sub = extractFilter(arg);
            if (sub == null) {
                return null;
            }
            merged = merged == null ? sub : merged.union(sub);
        }
        return merged;
    }

    // -------------------------------------------------------------------------
    //  Range
    // -------------------------------------------------------------------------

    @Nullable
    private KeyFilterPlan fromAnd(CallExpression call) {
        KeyFilterPlan merged = null;
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            final KeyFilterPlan sub = extractFilter(arg);
            if (sub == null) {
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
    private KeyFilterPlan fromNotBetween(CallExpression call) {
        final KeyFilterPlan between = fromBetween(call);
        return between == null ? null : KeyFilterPlan.not(between);
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

    @Nullable
    private Object extractBinaryKeyValue(CallExpression call) {
        if (!isBinaryValid(call)) {
            return null;
        }
        final ResolvedExpression left = call.getResolvedChildren().get(0);
        final ResolvedExpression right = call.getResolvedChildren().get(1);
        if (isKeyField(left)) {
            return extractValue(right);
        }
        if (isKeyField(right)) {
            return extractValue(left);
        }
        return null;
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
                final boolean unsafeLongConversion =
                        (value instanceof Float
                                        && Math.abs((Float) value) >= FLOAT_EXACT_INTEGER_LIMIT)
                                || (value instanceof Double
                                        && Math.abs((Double) value) >= DOUBLE_EXACT_INTEGER_LIMIT);
                if (!unsafeLongConversion) {
                    try {
                        return new BigDecimal(value.toString()).longValueExact();
                    } catch (NumberFormatException | ArithmeticException lossy) {
                        // Not a decimal (NaN, Infinity), fractional, or outside the long range:
                        // fall through and refuse the pushdown.
                    }
                }
            } else if (keyClass == Double.class) {
                final double converted = ((Number) value).doubleValue();
                if (Double.isFinite(converted)) {
                    return converted;
                }
            }
        }
        LOG.debug(
                "Refusing pushdown: literal value {} of type {} cannot be widened to key type {}"
                        + " without loss.",
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

    private interface KeyFilterPlan {
        static KeyFilterPlan exact(Object key) {
            return exact(Set.of(key));
        }

        static KeyFilterPlan exact(Set<Object> keys) {
            return new ExactKeyFilterPlan(keys);
        }

        static KeyFilterPlan empty() {
            return exact(Set.of());
        }

        static KeyFilterPlan range(
                @Nullable Comparable lower,
                boolean lowerInclusive,
                @Nullable Comparable upper,
                boolean upperInclusive) {
            final RangeKeyFilterPlan range =
                    new RangeKeyFilterPlan(lower, lowerInclusive, upper, upperInclusive);
            return range.isEmpty() ? empty() : range;
        }

        static KeyFilterPlan and(KeyFilterPlan left, KeyFilterPlan right) {
            return new AndKeyFilterPlan(left, right);
        }

        static KeyFilterPlan or(KeyFilterPlan left, KeyFilterPlan right) {
            return new OrKeyFilterPlan(left, right);
        }

        static KeyFilterPlan not(KeyFilterPlan plan) {
            return new NotKeyFilterPlan(plan);
        }

        default KeyFilterPlan intersect(KeyFilterPlan other) {
            if (isEmpty() || other.isEmpty()) {
                return KeyFilterPlan.empty();
            }
            return other.getExactKeys() == null ? and(this, other) : other.intersect(this);
        }

        default KeyFilterPlan union(KeyFilterPlan other) {
            if (isEmpty()) {
                return other;
            }
            if (other.isEmpty()) {
                return this;
            }
            return or(this, other);
        }

        @Nullable
        default Set<Object> getExactKeys() {
            return null;
        }

        @Nullable
        default RangeBounds getRangeBounds() {
            return null;
        }

        default boolean isEmpty() {
            return false;
        }

        SavepointKeyFilter<Object> toSavepointKeyFilter();
    }

    private static final class ExactKeyFilterPlan implements KeyFilterPlan {
        private final Set<Object> exactKeys;

        private ExactKeyFilterPlan(Set<Object> exactKeys) {
            this.exactKeys = Set.copyOf(exactKeys);
        }

        @Override
        public KeyFilterPlan intersect(KeyFilterPlan other) {
            if (isEmpty() || other.isEmpty()) {
                return KeyFilterPlan.empty();
            }
            final SavepointKeyFilter<Object> filter = other.toSavepointKeyFilter();
            final Set<Object> retainedKeys = new HashSet<>(exactKeys);
            retainedKeys.removeIf(key -> !filter.test(key));
            return KeyFilterPlan.exact(retainedKeys);
        }

        @Override
        public KeyFilterPlan union(KeyFilterPlan other) {
            if (isEmpty()) {
                return other;
            }
            if (other.isEmpty()) {
                return this;
            }
            final Set<Object> otherExactKeys = other.getExactKeys();
            if (otherExactKeys == null) {
                return KeyFilterPlan.or(this, other);
            }
            final Set<Object> keys = new HashSet<>(exactKeys);
            keys.addAll(otherExactKeys);
            return KeyFilterPlan.exact(keys);
        }

        @Override
        public Set<Object> getExactKeys() {
            return exactKeys;
        }

        @Override
        public boolean isEmpty() {
            return exactKeys.isEmpty();
        }

        @Override
        public SavepointKeyFilter<Object> toSavepointKeyFilter() {
            return SavepointKeyFilter.exact(exactKeys);
        }
    }

    private static final class RangeKeyFilterPlan implements KeyFilterPlan {
        private final RangeBounds bounds;

        private RangeKeyFilterPlan(
                @Nullable Comparable lower,
                boolean lowerInclusive,
                @Nullable Comparable upper,
                boolean upperInclusive) {
            this.bounds = new RangeBounds(lower, lowerInclusive, upper, upperInclusive);
        }

        @Override
        public KeyFilterPlan intersect(KeyFilterPlan other) {
            if (isEmpty() || other.isEmpty()) {
                return KeyFilterPlan.empty();
            }
            if (other.getExactKeys() != null) {
                return other.intersect(this);
            }
            final RangeBounds range = other.getRangeBounds();
            if (range == null) {
                return KeyFilterPlan.and(this, other);
            }
            Comparable newLower = bounds.lower;
            boolean newLowerInclusive = bounds.lowerInclusive;
            if (range.lower != null) {
                final int comparison = newLower == null ? -1 : newLower.compareTo(range.lower);
                if (newLower == null || comparison < 0) {
                    newLower = range.lower;
                    newLowerInclusive = range.lowerInclusive;
                } else if (comparison == 0) {
                    newLowerInclusive &= range.lowerInclusive;
                }
            }

            Comparable newUpper = bounds.upper;
            boolean newUpperInclusive = bounds.upperInclusive;
            if (range.upper != null) {
                final int comparison = newUpper == null ? 1 : newUpper.compareTo(range.upper);
                if (newUpper == null || comparison > 0) {
                    newUpper = range.upper;
                    newUpperInclusive = range.upperInclusive;
                } else if (comparison == 0) {
                    newUpperInclusive &= range.upperInclusive;
                }
            }
            return KeyFilterPlan.range(newLower, newLowerInclusive, newUpper, newUpperInclusive);
        }

        @Override
        public RangeBounds getRangeBounds() {
            return bounds;
        }

        @Override
        public boolean isEmpty() {
            return bounds.isEmpty();
        }

        @Override
        public SavepointKeyFilter<Object> toSavepointKeyFilter() {
            return SavepointKeyFilter.range(
                    bounds.lower, bounds.lowerInclusive, bounds.upper, bounds.upperInclusive);
        }
    }

    private static final class RangeBounds {
        @Nullable private final Comparable lower;
        private final boolean lowerInclusive;
        @Nullable private final Comparable upper;
        private final boolean upperInclusive;

        private RangeBounds(
                @Nullable Comparable lower,
                boolean lowerInclusive,
                @Nullable Comparable upper,
                boolean upperInclusive) {
            this.lower = lower;
            this.lowerInclusive = lowerInclusive;
            this.upper = upper;
            this.upperInclusive = upperInclusive;
        }

        private boolean isEmpty() {
            if (lower == null || upper == null) {
                return false;
            }
            final int comparison = lower.compareTo(upper);
            return comparison > 0 || (comparison == 0 && (!lowerInclusive || !upperInclusive));
        }
    }

    private static final class AndKeyFilterPlan implements KeyFilterPlan {
        private final KeyFilterPlan left;
        private final KeyFilterPlan right;

        private AndKeyFilterPlan(KeyFilterPlan left, KeyFilterPlan right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public SavepointKeyFilter<Object> toSavepointKeyFilter() {
            final SavepointKeyFilter<Object> leftFilter = left.toSavepointKeyFilter();
            final SavepointKeyFilter<Object> rightFilter = right.toSavepointKeyFilter();
            return key -> leftFilter.test(key) && rightFilter.test(key);
        }
    }

    private static final class OrKeyFilterPlan implements KeyFilterPlan {
        private final KeyFilterPlan left;
        private final KeyFilterPlan right;

        private OrKeyFilterPlan(KeyFilterPlan left, KeyFilterPlan right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public SavepointKeyFilter<Object> toSavepointKeyFilter() {
            final SavepointKeyFilter<Object> leftFilter = left.toSavepointKeyFilter();
            final SavepointKeyFilter<Object> rightFilter = right.toSavepointKeyFilter();
            return key -> leftFilter.test(key) || rightFilter.test(key);
        }
    }

    private static final class NotKeyFilterPlan implements KeyFilterPlan {
        private final KeyFilterPlan child;

        private NotKeyFilterPlan(KeyFilterPlan child) {
            this.child = child;
        }

        @Override
        public SavepointKeyFilter<Object> toSavepointKeyFilter() {
            final SavepointKeyFilter<Object> filter = child.toSavepointKeyFilter();
            return key -> !filter.test(key);
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
