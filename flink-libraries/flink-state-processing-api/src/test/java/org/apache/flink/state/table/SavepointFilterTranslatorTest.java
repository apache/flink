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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit tests for {@link SavepointFilterTranslator}. */
class SavepointFilterTranslatorTest {

    private static final int KEY_COL = 0;
    private static final DataType LONG_KEY_TYPE = DataTypes.BIGINT().notNull();

    // -------------------------------------------------------------------------
    //  Exact key filter — fromEquals
    // -------------------------------------------------------------------------

    @Test
    void equalsKeyOnLeft() {
        // key = 42 -> {42}
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), longLit(42L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(43L)).isFalse();
    }

    @Test
    void equalsKeyOnRight() {
        // 42 = key -> {42}
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longLit(42L), longKeyRef()));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(0L)).isFalse();
    }

    @Test
    void equalsNeitherSideIsKeyColumn_returnsNull() {
        // val = 42 -> not the key column, not pushed
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(otherRef(), longLit(42L)));
        assertThat(filter).isNull();
    }

    @Test
    void equalsNeitherSideIsLiteral_returnsNull() {
        // key = val -> no literal to match, not pushed
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), otherRef()));
        assertThat(filter).isNull();
    }

    // -------------------------------------------------------------------------
    //  Exact key filter — fromOr
    // -------------------------------------------------------------------------

    @Test
    void orOfEqualsProducesMergedExactFilter() {
        // key = 1 OR key = 2 OR 3 = key -> {1, 2, 3}
        CallExpression expr =
                or(
                        eq(longKeyRef(), longLit(1L)),
                        eq(longKeyRef(), longLit(2L)),
                        eq(longLit(3L), longKeyRef()));

        SavepointKeyFilter<Object> filter = keyFilterOf(expr);
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(filter.test(4L)).isFalse();
    }

    @Test
    void orWithNonPushableChild_returnsNull() {
        // One branch is a range, which OR cannot absorb
        CallExpression expr = or(eq(longKeyRef(), longLit(1L)), gt(longKeyRef(), longLit(5L)));
        assertThat(keyFilterOf(expr)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Range key filter — fromBetween
    // -------------------------------------------------------------------------

    @Test
    void betweenProducesInclusiveRange() {
        // key BETWEEN 10 AND 20 -> [10, 20]
        SavepointKeyFilter<Object> filter =
                keyFilterOf(between(longKeyRef(), longLit(10L), longLit(20L)));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(9L)).isFalse();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(15L)).isTrue();
        assertThat(filter.test(20L)).isTrue();
        assertThat(filter.test(21L)).isFalse();
    }

    @Test
    void betweenWithNonKeyField_returnsNull() {
        // val BETWEEN 1 AND 10 -> not the key column, not pushed
        SavepointKeyFilter<Object> filter =
                keyFilterOf(between(otherRef(), longLit(1L), longLit(10L)));
        assertThat(filter).isNull();
    }

    // -------------------------------------------------------------------------
    //  Range key filter — comparison operators
    // -------------------------------------------------------------------------

    @Test
    void greaterThanProducesExclusiveLowerBound() {
        // key > 5 -> (5, +∞)
        SavepointKeyFilter<Object> filter = keyFilterOf(gt(longKeyRef(), longLit(5L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(5L)).isFalse();
        assertThat(filter.test(6L)).isTrue();
    }

    @Test
    void greaterThanOrEqualProducesInclusiveLowerBound() {
        // key >= 5 -> [5, +∞)
        SavepointKeyFilter<Object> filter = keyFilterOf(gte(longKeyRef(), longLit(5L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(6L)).isTrue();
    }

    @Test
    void lessThanProducesExclusiveUpperBound() {
        // key < 10 -> (-∞, 10)
        SavepointKeyFilter<Object> filter = keyFilterOf(lt(longKeyRef(), longLit(10L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(9L)).isTrue();
        assertThat(filter.test(10L)).isFalse();
    }

    @Test
    void lessThanOrEqualProducesInclusiveUpperBound() {
        // key <= 10 -> (-∞, 10]
        SavepointKeyFilter<Object> filter = keyFilterOf(lte(longKeyRef(), longLit(10L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void comparisonWithLiteralOnLeft_flipsDirection() {
        // literal > key  →  key < literal  →  upper bound (exclusive)
        SavepointKeyFilter<Object> filter = keyFilterOf(gt(longLit(10L), longKeyRef()));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(9L)).isTrue();
        assertThat(filter.test(10L)).isFalse();
    }

    @Test
    void comparisonWithLiteralOnLeft_lte_flipsDirection() {
        // literal <= key  →  key >= literal  →  lower bound (inclusive)
        SavepointKeyFilter<Object> filter = keyFilterOf(lte(longLit(5L), longKeyRef()));
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
    }

    // -------------------------------------------------------------------------
    //  AND — range intersection
    // -------------------------------------------------------------------------

    @Test
    void andOfTwoRangesProducesIntersection() {
        // key >= 5 AND key <= 10
        CallExpression expr = and(gte(longKeyRef(), longLit(5L)), lte(longKeyRef(), longLit(10L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void andWithProvablyEmptyIntersection_matchesNothing() {
        // key > 10 AND key < 5 — disjoint
        CallExpression expr = and(gt(longKeyRef(), longLit(10L)), lt(longKeyRef(), longLit(5L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isEmpty();
    }

    @Test
    void andWithEqualInclusiveBounds_matchesOnlyBound() {
        // key >= 7 AND key <= 7 -> [7, 7]
        CallExpression expr = and(gte(longKeyRef(), longLit(7L)), lte(longKeyRef(), longLit(7L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(6L)).isFalse();
        assertThat(filter.test(7L)).isTrue();
        assertThat(filter.test(8L)).isFalse();
    }

    @Test
    void andWithEqualBoundsAndExclusiveLower_matchesNothing() {
        // key > 7 AND key <= 7 -> empty
        CallExpression expr = and(gt(longKeyRef(), longLit(7L)), lte(longKeyRef(), longLit(7L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isEmpty();
    }

    @Test
    void andWithExactKeyChildIsNotPushable() {
        // AND requires all children to be range filters; exact filter breaks pushdown
        CallExpression expr = and(eq(longKeyRef(), longLit(5L)), gt(longKeyRef(), longLit(3L)));
        assertThat(keyFilterOf(expr)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Unsupported predicates
    // -------------------------------------------------------------------------

    @Test
    void nonCallExpressionReturnsNull() {
        // A bare field reference is not a predicate, not pushed
        assertThat(keyFilterOf(longKeyRef())).isNull();
    }

    @Test
    void unrecognizedFunctionReturnsNull() {
        // key IS NULL -> unsupported function, not pushed
        CallExpression isNull =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.IS_NULL,
                        Collections.singletonList(longKeyRef()),
                        DataTypes.BOOLEAN());
        assertThat(keyFilterOf(isNull)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Range key filter behavior — string keys (natural comparison)
    // -------------------------------------------------------------------------

    @Test
    void rangeFilterOnStringKey() {
        // key BETWEEN 'beta' AND 'delta' -> natural String order
        SavepointKeyFilter<Object> filter =
                keyFilterOf(between(stringKeyRef(), stringLit("beta"), stringLit("delta")));

        assertNotNull(filter);
        assertThat(filter.test("alpha")).isFalse();
        assertThat(filter.test("beta")).isTrue();
        assertThat(filter.test("gamma")).isFalse(); // "gamma" > "delta" lexicographically
        assertThat(filter.test("delta")).isTrue();
        assertThat(filter.test("epsilon")).isFalse();
    }

    @Test
    void rangeFilterWithDoubleComparison() {
        // key BETWEEN 1.5 AND 3.5 on a FLOAT key -> [1.5, 3.5]
        ValueLiteralExpression floatLower =
                new ValueLiteralExpression(1.5f, DataTypes.FLOAT().notNull());
        ValueLiteralExpression floatUpper =
                new ValueLiteralExpression(3.5f, DataTypes.FLOAT().notNull());
        FieldReferenceExpression floatKey =
                new FieldReferenceExpression("key", DataTypes.FLOAT().notNull(), 0, KEY_COL);

        SavepointKeyFilter<Object> filter = keyFilterOf(between(floatKey, floatLower, floatUpper));

        assertNotNull(filter);
        assertThat(filter.test(1.5f)).isTrue();
        assertThat(filter.test(2.0f)).isTrue();
        assertThat(filter.test(3.5f)).isTrue();
        assertThat(filter.test(1.0f)).isFalse();
        assertThat(filter.test(4.0f)).isFalse();
    }

    // -------------------------------------------------------------------------
    //  SavepointFilters.apply — intersection handling
    // -------------------------------------------------------------------------

    @Test
    void applyAccumulatesRangeAndRange() {
        // key >= 3, key <= 8 -> [3, 8]
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(gte(longKeyRef(), longLit(3L)), lte(longKeyRef(), longLit(8L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();
        assertNotNull(result);

        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.test(2L)).isFalse();
        assertThat(result.test(3L)).isTrue();
        assertThat(result.test(8L)).isTrue();
        assertThat(result.test(9L)).isFalse();
    }

    @Test
    void applyAccumulatesExactAndExact() {
        // key IN (1, 2, 3), key IN (2, 3, 4) -> {2, 3}
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                or(
                                        eq(longKeyRef(), longLit(1L)),
                                        eq(longKeyRef(), longLit(2L)),
                                        eq(longKeyRef(), longLit(3L))),
                                or(
                                        eq(longKeyRef(), longLit(2L)),
                                        eq(longKeyRef(), longLit(3L)),
                                        eq(longKeyRef(), longLit(4L)))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).containsExactlyInAnyOrder(2L, 3L);
    }

    @Test
    void applyAccumulatesExactAndExactEmptyResult_matchesNothing() {
        // key = 1, key = 2 -> disjoint, so nothing matches
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(eq(longKeyRef(), longLit(1L)), eq(longKeyRef(), longLit(2L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).isEmpty();
    }

    @Test
    void applyAccumulatesExactAndRange_keepsOnlyKeysInRange() {
        // key IN (1, 5, 10, 15), key BETWEEN 4 AND 12 -> {5, 10}
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                or(
                                        eq(longKeyRef(), longLit(1L)),
                                        eq(longKeyRef(), longLit(5L)),
                                        eq(longKeyRef(), longLit(10L)),
                                        eq(longKeyRef(), longLit(15L))),
                                between(longKeyRef(), longLit(4L), longLit(12L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).containsExactlyInAnyOrder(5L, 10L);
    }

    @Test
    void applyAccumulatesRangeAndExact_keepsOnlyKeysInRange() {
        // key BETWEEN 4 AND 12, key IN (1, 5, 10, 15) -> {5, 10}, operands swapped
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                between(longKeyRef(), longLit(4L), longLit(12L)),
                                or(
                                        eq(longKeyRef(), longLit(1L)),
                                        eq(longKeyRef(), longLit(5L)),
                                        eq(longKeyRef(), longLit(10L)),
                                        eq(longKeyRef(), longLit(15L)))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).containsExactlyInAnyOrder(5L, 10L);
    }

    // -------------------------------------------------------------------------
    //  AND with 3+ children
    // -------------------------------------------------------------------------

    @Test
    void andOfThreeRangesProducesIntersection() {
        // key >= 3 AND key <= 20 AND key < 10
        CallExpression expr =
                and(
                        gte(longKeyRef(), longLit(3L)),
                        lte(longKeyRef(), longLit(20L)),
                        lt(longKeyRef(), longLit(10L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(2L)).isFalse();
        assertThat(filter.test(3L)).isTrue();
        assertThat(filter.test(9L)).isTrue();
        assertThat(filter.test(10L)).isFalse();
        assertThat(filter.test(20L)).isFalse();
    }

    // -------------------------------------------------------------------------
    //  OR edge cases
    // -------------------------------------------------------------------------

    @Test
    void orWithSingleChild_returnsExactFilter() {
        // OR (key = 7) -> {7}
        CallExpression expr = or(eq(longKeyRef(), longLit(7L)));
        SavepointKeyFilter<Object> filter = keyFilterOf(expr);

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(7L);
    }

    // -------------------------------------------------------------------------
    //  Comparison with neither side being the key column
    // -------------------------------------------------------------------------

    @Test
    void comparisonWithNeitherSideBeingKeyColumn_returnsNull() {
        // val > 5 and 5 < val -> not the key column, not pushed
        assertThat(keyFilterOf(gt(otherRef(), longLit(5L)))).isNull();
        assertThat(keyFilterOf(lt(longLit(5L), otherRef()))).isNull();
    }

    // -------------------------------------------------------------------------
    //  SavepointFilters.apply — empty filter handling
    // -------------------------------------------------------------------------

    @Test
    void applyWithEmptyThenRange_returnsEmpty() {
        // (key > 10 AND key < 5) -> empty, key <= 10 -> empty absorbs the range
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                and(gt(longKeyRef(), longLit(10L)), lt(longKeyRef(), longLit(5L))),
                                lte(longKeyRef(), longLit(10L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).isEmpty();
    }

    @Test
    void applyWithRangeThenEmpty_returnsEmpty() {
        // key <= 10, (key > 10 AND key < 5) -> empty, operands swapped
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                lte(longKeyRef(), longLit(10L)),
                                and(gt(longKeyRef(), longLit(10L)), lt(longKeyRef(), longLit(5L)))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).isEmpty();
    }

    @Test
    void applyWithEmptyThenExact_returnsEmpty() {
        // (key > 10 AND key < 5) -> empty, key = 1 -> empty absorbs the exact set
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(
                                and(gt(longKeyRef(), longLit(10L)), lt(longKeyRef(), longLit(5L))),
                                eq(longKeyRef(), longLit(1L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).isEmpty();
    }

    @Test
    void applyWithConflictingExactPredicates_returnsEmptyFilter() {
        // key = 1, key = 2 -> disjoint, so nothing matches
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(eq(longKeyRef(), longLit(1L)), eq(longKeyRef(), longLit(2L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.getExactKeys()).isEmpty();
    }

    // -------------------------------------------------------------------------
    //  BETWEEN — wrong arg count
    // -------------------------------------------------------------------------

    @Test
    void betweenWithWrongArgCount_returnsNull() {
        // BETWEEN with two children is malformed, not pushed
        CallExpression malformed =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.BETWEEN,
                        Arrays.asList(longKeyRef(), longLit(1L)),
                        DataTypes.BOOLEAN());
        assertThat(keyFilterOf(malformed)).isNull();
    }

    // -------------------------------------------------------------------------
    //  BETWEEN — non-literal bounds
    // -------------------------------------------------------------------------

    @Test
    void betweenWithNonLiteralBound_returnsNull() {
        // key BETWEEN val AND 10 -> non-literal bound, not pushed
        CallExpression expr =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.BETWEEN,
                        Arrays.asList(longKeyRef(), otherRef(), longLit(10L)),
                        DataTypes.BOOLEAN());
        assertThat(keyFilterOf(expr)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Comparison with non-literal value side
    // -------------------------------------------------------------------------

    @Test
    void comparisonWithNonLiteralValue_returnsNull() {
        // key > val -> no literal bound, not pushed
        assertThat(keyFilterOf(gt(longKeyRef(), otherRef()))).isNull();
    }

    // -------------------------------------------------------------------------
    //  EQUALS — malformed (wrong arg count)
    // -------------------------------------------------------------------------

    @Test
    void equalsWithWrongArgCount_returnsNull() {
        // EQUALS with one child is malformed, not pushed
        CallExpression malformed =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Collections.singletonList(longKeyRef()),
                        DataTypes.BOOLEAN());
        assertThat(keyFilterOf(malformed)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Literal type widening to key type
    // -------------------------------------------------------------------------

    @Test
    void equalsWithIntLiteralIsWidenedToBigintKeyAndPushed() {
        // key = 5 (INT literal, BIGINT key) -> {5L}
        ValueLiteralExpression intLit = new ValueLiteralExpression(5, DataTypes.INT().notNull());
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), intLit));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(5L);
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(6L)).isFalse();
    }

    @Test
    void betweenWithIntLiteralBoundsIsWidenedToBigintKeyAndPushed() {
        // key BETWEEN 1 AND 10 (INT literals, BIGINT key) -> [1L, 10L]
        ValueLiteralExpression lower = new ValueLiteralExpression(1, DataTypes.INT().notNull());
        ValueLiteralExpression upper = new ValueLiteralExpression(10, DataTypes.INT().notNull());
        SavepointKeyFilter<Object> filter = keyFilterOf(between(longKeyRef(), lower, upper));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(0L)).isFalse();
        assertThat(filter.test(1L)).isTrue();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void equalsWithIntLiteralIsWidenedToDoubleKeyAndPushed() {
        // key = 5 (INT literal, DOUBLE key) -> {5.0}
        FieldReferenceExpression doubleKey =
                new FieldReferenceExpression("key", DataTypes.DOUBLE().notNull(), 0, KEY_COL);
        ValueLiteralExpression intLit = new ValueLiteralExpression(5, DataTypes.INT().notNull());

        SavepointKeyFilter<Object> filter = keyFilterOf(eq(doubleKey, intLit));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(5.0d);
        assertThat(filter.test(5.0d)).isTrue();
        assertThat(filter.test(6.0d)).isFalse();
    }

    @Test
    void lessThanFractionalLiteralAgainstBigintKeyIsNotPushed() {
        // key < 1.5 (DECIMAL literal, BIGINT key) -> truncating to 1 would drop key 1, not pushed
        SavepointFilterTranslator.Result applied =
                apply(List.of(lt(longKeyRef(), decLit("1.5"))), LONG_KEY_TYPE);

        assertThat(applied.keyFilter()).isNull();
        // The predicate must be handed back, or nothing would evaluate it.
        assertThat(applied.accepted()).isEmpty();
        assertThat(applied.remaining()).hasSize(1);
    }

    @Test
    void greaterThanOrEqualFractionalLiteralAgainstBigintKeyIsNotPushed() {
        // key >= 1.5 -> truncating to 1 would wrongly admit key 1, not pushed
        SavepointFilterTranslator.Result applied =
                apply(List.of(gte(longKeyRef(), decLit("1.5"))), LONG_KEY_TYPE);

        assertThat(applied.keyFilter()).isNull();
        assertThat(applied.accepted()).isEmpty();
        assertThat(applied.remaining()).hasSize(1);
    }

    @Test
    void betweenFractionalLiteralBoundsAgainstBigintKeyIsNotPushed() {
        // key BETWEEN 1.5 AND 4.5 -> both bounds are lossy, not pushed
        assertThat(keyFilterOf(between(longKeyRef(), decLit("1.5"), decLit("4.5")))).isNull();
    }

    @Test
    void equalsFractionalLiteralAgainstBigintKeyIsNotPushed() {
        // key = 1.5 -> no BIGINT key can equal 1.5, and truncating to 1 would match key 1
        assertThat(keyFilterOf(eq(longKeyRef(), decLit("1.5")))).isNull();
    }

    @Test
    void integralDecimalLiteralAgainstBigintKeyIsWidenedAndPushed() {
        // key = 5.0 -> the widening is exact, so the pushdown is still safe
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), decLit("5.0")));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(5L);
        assertThat(filter.test(5L)).isTrue();
    }

    @Test
    void outOfRangeLiteralAgainstBigintKeyIsNotPushed() {
        // key < 10^30 -> the value does not fit in a long, so the bound would wrap around
        ValueLiteralExpression hugeLit =
                new ValueLiteralExpression(
                        new BigDecimal("1000000000000000000000000000000"),
                        DataTypes.DECIMAL(31, 0).notNull());

        assertThat(keyFilterOf(lt(longKeyRef(), hugeLit))).isNull();
    }

    @Test
    void literalLosingPrecisionAgainstDoubleKeyIsNotPushed() {
        // key = 9007199254740993 (BIGINT literal, DOUBLE key) -> not representable as a double
        FieldReferenceExpression doubleKey =
                new FieldReferenceExpression("key", DataTypes.DOUBLE().notNull(), 0, KEY_COL);

        assertThat(keyFilterOf(eq(doubleKey, longLit(9007199254740993L)))).isNull();
    }

    @Test
    void fractionalLiteralAgainstDoubleKeyIsWidenedAndPushed() {
        // key BETWEEN 1.5 AND 3.5 (DECIMAL literals, DOUBLE key) -> exact, so pushed
        FieldReferenceExpression doubleKey =
                new FieldReferenceExpression("key", DataTypes.DOUBLE().notNull(), 0, KEY_COL);

        SavepointKeyFilter<Object> filter =
                keyFilterOf(between(doubleKey, decLit("1.5"), decLit("3.5")));

        assertNotNull(filter);
        assertThat(filter.test(1.4d)).isFalse();
        assertThat(filter.test(1.5d)).isTrue();
        assertThat(filter.test(3.5d)).isTrue();
        assertThat(filter.test(3.6d)).isFalse();
    }

    @Test
    void nonNumericLiteralAgainstNumericKeyIsNotWidenedAndNotPushed() {
        // key = '5' (STRING literal, BIGINT key) -> no widening, not pushed
        assertThat(keyFilterOf(eq(longKeyRef(), stringLit("5")))).isNull();
    }

    @Test
    void numericLiteralWithNonWidenableKeyTypeIsNotPushed() {
        // key = 5L (BIGINT literal, INT key) -> narrowing is unsafe, not pushed
        FieldReferenceExpression intKey =
                new FieldReferenceExpression("key", DataTypes.INT().notNull(), 0, KEY_COL);
        assertThat(keyFilterOf(eq(intKey, longLit(5L)))).isNull();
    }

    @Test
    void decimalKeyEqualityIsPushedDownPreservingLiteralScale() {
        // key = 5.00 on a DECIMAL(10, 2) key -> {5.00}
        FieldReferenceExpression decKey =
                new FieldReferenceExpression("key", DataTypes.DECIMAL(10, 2).notNull(), 0, KEY_COL);
        ValueLiteralExpression lit =
                new ValueLiteralExpression(
                        new BigDecimal("5.00"), DataTypes.DECIMAL(10, 2).notNull());

        SavepointKeyFilter<Object> filter = keyFilterOf(eq(decKey, lit));

        assertNotNull(filter);
        // Literal scale is preserved, so exact matching is scale sensitive: 5.00 matches, 5.0 not.
        assertThat(filter.getExactKeys()).containsExactly(new BigDecimal("5.00"));
        assertThat(filter.test(new BigDecimal("5.00"))).isTrue();
        assertThat(filter.test(new BigDecimal("5.0"))).isFalse();
    }

    // -------------------------------------------------------------------------
    //  apply — predicates that cannot be pushed stay in remaining()
    // -------------------------------------------------------------------------

    @Test
    void applyKeepsNonPushablePredicatesInRemaining() {
        // key = 5, key IS NULL -> only the first is pushed, the second must still be evaluated
        SavepointFilterTranslator.Result applied =
                apply(List.of(eq(longKeyRef(), longLit(5L)), isNull(longKeyRef())), LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(1);
        assertThat(applied.remaining()).hasSize(1);
        assertThat(result.getExactKeys()).containsExactly(5L);
    }

    @Test
    void applyWithOnlyNonPushablePredicateReturnsNoKeyFilter() {
        // key IS NULL -> nothing to push, the predicate is handed back untouched
        SavepointFilterTranslator.Result applied =
                apply(List.of(isNull(longKeyRef())), LONG_KEY_TYPE);

        assertThat(applied.keyFilter()).isNull();
        assertThat(applied.accepted()).isEmpty();
        assertThat(applied.remaining()).hasSize(1);
    }

    @Test
    void applyWithNoPredicatesReturnsNoKeyFilter() {
        // no predicates -> no filter, so the scan is not pruned at all
        SavepointFilterTranslator.Result applied = apply(List.of(), LONG_KEY_TYPE);

        assertThat(applied.keyFilter()).isNull();
        assertThat(applied.accepted()).isEmpty();
        assertThat(applied.remaining()).isEmpty();
    }

    // -------------------------------------------------------------------------
    //  Untranslatable children break their parent
    // -------------------------------------------------------------------------

    @Test
    void andWithUntranslatableChild_returnsNull() {
        // key > 5 AND key IS NULL -> the whole AND must not be pushed
        assertThat(keyFilterOf(and(gt(longKeyRef(), longLit(5L)), isNull(longKeyRef())))).isNull();
    }

    @Test
    void orWithUntranslatableChild_returnsNull() {
        // key = 1 OR key IS NULL -> the whole OR must not be pushed
        assertThat(keyFilterOf(or(eq(longKeyRef(), longLit(1L)), isNull(longKeyRef())))).isNull();
    }

    // -------------------------------------------------------------------------
    //  Literals that cannot be read
    // -------------------------------------------------------------------------

    @Test
    void equalsWithNullLiteral_returnsNull() {
        // key = NULL -> the literal has no readable value, not pushed
        assertThat(keyFilterOf(eq(longKeyRef(), nullLit()))).isNull();
    }

    @Test
    void betweenWithNonComparableLiteral_returnsNull() {
        // key BETWEEN x'01' AND x'02' on a BYTES key -> byte[] is not Comparable, not pushed
        assertThat(keyFilterOf(between(bytesKeyRef(), bytesLit((byte) 1), bytesLit((byte) 2))))
                .isNull();
    }

    @Test
    void comparisonWithNonComparableLiteral_returnsNull() {
        // key > x'01' on a BYTES key -> byte[] is not Comparable, not pushed
        assertThat(keyFilterOf(gt(bytesKeyRef(), bytesLit((byte) 1)))).isNull();
    }

    // -------------------------------------------------------------------------
    //  Comparison — remaining flip directions and arity
    // -------------------------------------------------------------------------

    @Test
    void comparisonWithLiteralOnLeft_gte_flipsDirection() {
        // 10 >= key  ->  key <= 10  ->  upper bound (inclusive)
        SavepointKeyFilter<Object> filter = keyFilterOf(gte(longLit(10L), longKeyRef()));
        assertNotNull(filter);
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void comparisonWithLiteralOnLeft_lt_flipsDirection() {
        // 5 < key  ->  key > 5  ->  lower bound (exclusive)
        SavepointKeyFilter<Object> filter = keyFilterOf(lt(longLit(5L), longKeyRef()));
        assertNotNull(filter);
        assertThat(filter.test(5L)).isFalse();
        assertThat(filter.test(6L)).isTrue();
    }

    @Test
    void comparisonWithWrongArgCount_returnsNull() {
        // GREATER_THAN with one child is malformed, not pushed
        CallExpression malformed =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        Collections.singletonList(longKeyRef()),
                        DataTypes.BOOLEAN());
        assertThat(keyFilterOf(malformed)).isNull();
    }

    // -------------------------------------------------------------------------
    //  Expression helpers
    // -------------------------------------------------------------------------

    private static SavepointKeyFilter<Object> keyFilterOf(ResolvedExpression expr) {
        DataType keyType = findKeyType(expr);
        return apply(Collections.singletonList(expr), keyType).keyFilter();
    }

    private static SavepointFilterTranslator.Result apply(
            List<ResolvedExpression> filters, DataType keyType) {
        return new SavepointFilterTranslator(KEY_COL, keyType).apply(filters);
    }

    private static DataType findKeyType(ResolvedExpression expr) {
        if (expr instanceof FieldReferenceExpression
                && ((FieldReferenceExpression) expr).getFieldIndex() == KEY_COL) {
            return expr.getOutputDataType();
        }
        for (ResolvedExpression child : expr.getResolvedChildren()) {
            DataType found = findKeyType(child);
            if (found != null) {
                return found;
            }
        }
        return LONG_KEY_TYPE;
    }

    private static FieldReferenceExpression longKeyRef() {
        return new FieldReferenceExpression("key", DataTypes.BIGINT().notNull(), 0, KEY_COL);
    }

    private static FieldReferenceExpression stringKeyRef() {
        return new FieldReferenceExpression("key", DataTypes.STRING().notNull(), 0, KEY_COL);
    }

    private static FieldReferenceExpression otherRef() {
        return new FieldReferenceExpression("val", DataTypes.BIGINT().notNull(), 0, 1);
    }

    private static ValueLiteralExpression longLit(long value) {
        return new ValueLiteralExpression(value, DataTypes.BIGINT().notNull());
    }

    private static ValueLiteralExpression decLit(String value) {
        BigDecimal decimal = new BigDecimal(value);
        return new ValueLiteralExpression(
                decimal, DataTypes.DECIMAL(decimal.precision(), decimal.scale()).notNull());
    }

    private static ValueLiteralExpression stringLit(String value) {
        return new ValueLiteralExpression(value, DataTypes.STRING().notNull());
    }

    private static CallExpression eq(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS, Arrays.asList(left, right), DataTypes.BOOLEAN());
    }

    private static CallExpression isNull(ResolvedExpression arg) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.IS_NULL,
                Collections.singletonList(arg),
                DataTypes.BOOLEAN());
    }

    private static ValueLiteralExpression nullLit() {
        return new ValueLiteralExpression(null, DataTypes.BIGINT().nullable());
    }

    private static FieldReferenceExpression bytesKeyRef() {
        return new FieldReferenceExpression("key", DataTypes.BYTES().notNull(), 0, KEY_COL);
    }

    private static ValueLiteralExpression bytesLit(byte value) {
        return new ValueLiteralExpression(new byte[] {value}, DataTypes.BYTES().notNull());
    }

    private static CallExpression or(ResolvedExpression... args) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.OR, Arrays.asList(args), DataTypes.BOOLEAN());
    }

    private static CallExpression and(ResolvedExpression... args) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.AND, Arrays.asList(args), DataTypes.BOOLEAN());
    }

    private static CallExpression between(
            ResolvedExpression value, ResolvedExpression lower, ResolvedExpression upper) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.BETWEEN,
                Arrays.asList(value, lower, upper),
                DataTypes.BOOLEAN());
    }

    private static CallExpression gt(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }

    private static CallExpression gte(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }

    private static CallExpression lt(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.LESS_THAN,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }

    private static CallExpression lte(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }
}
