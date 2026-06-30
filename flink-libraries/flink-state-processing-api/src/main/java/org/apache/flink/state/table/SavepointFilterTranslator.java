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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Converts {@link ResolvedExpression} key filter predicates into {@link SavepointKeyFilter}
 * instances that can be used to prune key groups and key iterations during savepoint reads.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class SavepointFilterTranslator {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointFilterTranslator.class);

    private static final Map<
                    FunctionDefinition,
                    BiFunction<SavepointFilterTranslator, CallExpression, SavepointKeyFilter>>
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
                            SavepointFilterTranslator::fromGreaterThan,
                            BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                            SavepointFilterTranslator::fromGreaterThanOrEqual,
                            BuiltInFunctionDefinitions.LESS_THAN,
                            SavepointFilterTranslator::fromLessThan,
                            BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                            SavepointFilterTranslator::fromLessThanOrEqual);

    private final int keyColumnIndex;
    private final DataType keyColumnType;

    SavepointFilterTranslator(int keyColumnIndex, DataType keyColumnType) {
        this.keyColumnIndex = keyColumnIndex;
        this.keyColumnType = keyColumnType;
    }

    Result apply(List<ResolvedExpression> filters) {
        final List<ResolvedExpression> accepted = new ArrayList<>();
        final List<ResolvedExpression> remaining = new ArrayList<>();

        SavepointKeyFilter keyFilter = null;
        for (ResolvedExpression filter : filters) {
            SavepointKeyFilter extracted = extractFilter(filter);
            if (extracted == null) {
                remaining.add(filter);
                continue;
            }

            keyFilter = keyFilter == null ? extracted : keyFilter.intersect(extracted);
            accepted.add(filter);
        }

        return new Result(accepted, remaining, keyFilter);
    }

    @Nullable
    private SavepointKeyFilter extractFilter(ResolvedExpression expr) {
        final BiFunction<SavepointFilterTranslator, CallExpression, SavepointKeyFilter> extractor =
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
    private SavepointKeyFilter fromEquals(CallExpression call) {
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
        return SavepointKeyFilter.exact(value);
    }

    @Nullable
    private SavepointKeyFilter fromOr(CallExpression call) {
        Set<Object> keys = new HashSet<>();
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            SavepointKeyFilter sub = extractFilter(arg);
            if (sub == null) {
                return null;
            }
            Set<Object> subKeys = sub.getExactKeys();
            // OR can only absorb finite key sets; a range branch cannot be merged via union.
            if (subKeys == null) {
                return null;
            }
            keys.addAll(subKeys);
        }
        return SavepointKeyFilter.exact(keys);
    }

    // -------------------------------------------------------------------------
    //  Range
    // -------------------------------------------------------------------------

    @Nullable
    private SavepointKeyFilter fromAnd(CallExpression call) {
        SavepointKeyFilter merged = null;
        for (ResolvedExpression arg : call.getResolvedChildren()) {
            SavepointKeyFilter sub = extractFilter(arg);
            // AND only absorbs range filters; exact (or null) children break pushdown.
            if (sub == null || sub.getExactKeys() != null) {
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
    private SavepointKeyFilter fromBetween(CallExpression call) {
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
        return SavepointKeyFilter.range(
                (Comparable) lower, true,
                (Comparable) upper, true);
    }

    @Nullable
    private SavepointKeyFilter fromGreaterThan(CallExpression call) {
        return fromComparison(call, Comparison.GT);
    }

    @Nullable
    private SavepointKeyFilter fromGreaterThanOrEqual(CallExpression call) {
        return fromComparison(call, Comparison.GTE);
    }

    @Nullable
    private SavepointKeyFilter fromLessThan(CallExpression call) {
        return fromComparison(call, Comparison.LT);
    }

    @Nullable
    private SavepointKeyFilter fromLessThanOrEqual(CallExpression call) {
        return fromComparison(call, Comparison.LTE);
    }

    @Nullable
    private SavepointKeyFilter fromComparison(CallExpression call, Comparison cmp) {
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
                return SavepointKeyFilter.range(b, false, null, true);
            case GTE:
                return SavepointKeyFilter.range(b, true, null, true);
            case LT:
                return SavepointKeyFilter.range(null, true, b, false);
            case LTE:
                return SavepointKeyFilter.range(null, true, b, true);
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
