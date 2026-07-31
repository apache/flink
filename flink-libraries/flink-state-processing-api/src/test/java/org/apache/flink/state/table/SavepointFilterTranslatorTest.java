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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), longLit(42L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(43L)).isFalse();
    }

    @Test
    void equalsKeyOnRight() {
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longLit(42L), longKeyRef()));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(0L)).isFalse();
    }

    @Test
    void equalsNeitherSideIsKeyColumn_returnsNull() {
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(otherRef(), longLit(42L)));
        assertThat(filter).isNull();
    }

    @Test
    void equalsNeitherSideIsLiteral_returnsNull() {
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), otherRef()));
        assertThat(filter).isNull();
    }

    // -------------------------------------------------------------------------
    //  Exact key filter — fromOr
    // -------------------------------------------------------------------------

    @Test
    void orOfEqualsProducesMergedExactFilter() {
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
        SavepointKeyFilter<Object> filter =
                keyFilterOf(between(otherRef(), longLit(1L), longLit(10L)));
        assertThat(filter).isNull();
    }

    // -------------------------------------------------------------------------
    //  Range key filter — comparison operators
    // -------------------------------------------------------------------------

    @Test
    void greaterThanProducesExclusiveLowerBound() {
        SavepointKeyFilter<Object> filter = keyFilterOf(gt(longKeyRef(), longLit(5L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(5L)).isFalse();
        assertThat(filter.test(6L)).isTrue();
    }

    @Test
    void greaterThanOrEqualProducesInclusiveLowerBound() {
        SavepointKeyFilter<Object> filter = keyFilterOf(gte(longKeyRef(), longLit(5L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(6L)).isTrue();
    }

    @Test
    void lessThanProducesExclusiveUpperBound() {
        SavepointKeyFilter<Object> filter = keyFilterOf(lt(longKeyRef(), longLit(10L)));
        assertNotNull(filter);
        assertThat(filter.getExactKeys()).isNull();
        assertThat(filter.test(9L)).isTrue();
        assertThat(filter.test(10L)).isFalse();
    }

    @Test
    void lessThanOrEqualProducesInclusiveUpperBound() {
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
        assertThat(filter.isEmpty()).isTrue();
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
        assertThat(keyFilterOf(longKeyRef())).isNull();
    }

    @Test
    void unrecognizedFunctionReturnsNull() {
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
    //  Range intersection
    // -------------------------------------------------------------------------

    @Test
    void intersectNarrowsBounds() {
        // [5, ∞) ∩ (-∞, 10] = [5, 10]
        SavepointKeyFilter<Long> lower = SavepointKeyFilter.range(5L, true, null, true);
        SavepointKeyFilter<Long> upper = SavepointKeyFilter.range(null, true, 10L, true);
        SavepointKeyFilter<Long> result = lower.intersect(upper);

        assertThat(result.isEmpty()).isFalse();
        assertThat(result.getExactKeys()).isNull();
        assertThat(result.test(4L)).isFalse();
        assertThat(result.test(5L)).isTrue();
        assertThat(result.test(10L)).isTrue();
        assertThat(result.test(11L)).isFalse();
    }

    @Test
    void intersectDisjointRangesReturnsEmpty() {
        // [10, ∞) ∩ (-∞, 5] — disjoint
        SavepointKeyFilter<Long> a = SavepointKeyFilter.range(10L, true, null, true);
        SavepointKeyFilter<Long> b = SavepointKeyFilter.range(null, true, 5L, true);
        assertThat(a.intersect(b).isEmpty()).isTrue();
    }

    @Test
    void intersectEqualBoundsInclusiveIsNonEmpty() {
        // [7, ∞) ∩ (-∞, 7] = [7, 7]
        SavepointKeyFilter<Long> a = SavepointKeyFilter.range(7L, true, null, true);
        SavepointKeyFilter<Long> b = SavepointKeyFilter.range(null, true, 7L, true);
        SavepointKeyFilter<Long> result = a.intersect(b);
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.test(7L)).isTrue();
        assertThat(result.test(6L)).isFalse();
        assertThat(result.test(8L)).isFalse();
    }

    @Test
    void intersectEqualBoundsOneExclusiveIsEmpty() {
        // (7, ∞) ∩ (-∞, 7] — empty because lower is exclusive
        SavepointKeyFilter<Long> a = SavepointKeyFilter.range(7L, false, null, true);
        SavepointKeyFilter<Long> b = SavepointKeyFilter.range(null, true, 7L, true);
        assertThat(a.intersect(b).isEmpty()).isTrue();
    }

    // -------------------------------------------------------------------------
    //  Custom comparator
    // -------------------------------------------------------------------------

    @Test
    void rangeWithCustomComparatorIsUsed() {
        // Orders strings by length — clearly not the natural String order.
        SavepointKeyFilter<String> filter =
                SavepointKeyFilter.range(
                        "aa",
                        true,
                        "cccc",
                        true,
                        (a, b) -> Integer.compare(a.length(), b.length()));

        // Length in [2, 4]: "abc" (3), passes; "a" (1) and "ccccc" (5), fail.
        assertThat(filter.test("abc")).isTrue();
        assertThat(filter.test("a")).isFalse();
        assertThat(filter.test("ccccc")).isFalse();
    }

    // -------------------------------------------------------------------------
    //  SavepointFilters.apply — intersection handling
    // -------------------------------------------------------------------------

    @Test
    void applyAccumulatesRangeAndRange() {
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
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(eq(longKeyRef(), longLit(1L)), eq(longKeyRef(), longLit(2L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void applyAccumulatesExactAndRange_keepsOnlyKeysInRange() {
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
    //  Empty key filter
    // -------------------------------------------------------------------------

    @Test
    void emptyKeyFilter_rejectsEverything() {
        SavepointKeyFilter<Object> empty = SavepointKeyFilter.empty();
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.getExactKeys()).isEmpty();
        assertThat(empty.test(42L)).isFalse();
        assertThat(empty.test("hello")).isFalse();
    }

    @Test
    void exactWithEmptySetReturnsEmptyKeyFilter() {
        SavepointKeyFilter<Object> filter = SavepointKeyFilter.exact(Collections.emptySet());
        assertThat(filter.isEmpty()).isTrue();
    }

    @Test
    void emptyKeyFilterSingletonPreservedAcrossSerialization() throws Exception {
        SavepointKeyFilter<Object> original = SavepointKeyFilter.empty();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        Object deserialized;
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            deserialized = ois.readObject();
        }
        assertThat(deserialized).isSameAs(SavepointKeyFilter.empty());
    }

    // -------------------------------------------------------------------------
    //  Exact key filter — single-value factory
    // -------------------------------------------------------------------------

    @Test
    void exactSingleValueFactory() {
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(42L);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(43L)).isFalse();
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
        assertThat(keyFilterOf(gt(otherRef(), longLit(5L)))).isNull();
        assertThat(keyFilterOf(lt(longLit(5L), otherRef()))).isNull();
    }

    // -------------------------------------------------------------------------
    //  SavepointFilters.apply — empty filter handling
    // -------------------------------------------------------------------------

    @Test
    void applyWithEmptyThenRange_returnsEmpty() {
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
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void applyWithRangeThenEmpty_returnsEmpty() {
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
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void applyWithEmptyThenExact_returnsEmpty() {
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
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void applyWithConflictingExactPredicates_returnsEmptyFilter() {
        SavepointFilterTranslator.Result applied =
                apply(
                        List.of(eq(longKeyRef(), longLit(1L)), eq(longKeyRef(), longLit(2L))),
                        LONG_KEY_TYPE);
        SavepointKeyFilter<Object> result = applied.keyFilter();

        assertNotNull(result);
        assertThat(applied.accepted()).hasSize(2);
        assertThat(applied.remaining()).isEmpty();
        assertThat(result.isEmpty()).isTrue();
    }

    // -------------------------------------------------------------------------
    //  BETWEEN — wrong arg count
    // -------------------------------------------------------------------------

    @Test
    void betweenWithWrongArgCount_returnsNull() {
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
        assertThat(keyFilterOf(gt(longKeyRef(), otherRef()))).isNull();
    }

    // -------------------------------------------------------------------------
    //  EQUALS — malformed (wrong arg count)
    // -------------------------------------------------------------------------

    @Test
    void equalsWithWrongArgCount_returnsNull() {
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
        ValueLiteralExpression intLit = new ValueLiteralExpression(5, DataTypes.INT().notNull());
        SavepointKeyFilter<Object> filter = keyFilterOf(eq(longKeyRef(), intLit));

        assertNotNull(filter);
        assertThat(filter.getExactKeys()).containsExactly(5L);
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(6L)).isFalse();
    }

    @Test
    void betweenWithIntLiteralBoundsIsWidenedToBigintKeyAndPushed() {
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
    void nonNumericLiteralAgainstNumericKeyIsNotWidenedAndNotPushed() {
        assertThat(keyFilterOf(eq(longKeyRef(), stringLit("5")))).isNull();
    }

    @Test
    void numericLiteralWithNonWidenableKeyTypeIsNotPushed() {
        FieldReferenceExpression intKey =
                new FieldReferenceExpression("key", DataTypes.INT().notNull(), 0, KEY_COL);
        assertThat(keyFilterOf(eq(intKey, longLit(5L)))).isNull();
    }

    @Test
    void decimalKeyEqualityIsPushedDownPreservingLiteralScale() {
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

    private static ValueLiteralExpression stringLit(String value) {
        return new ValueLiteralExpression(value, DataTypes.STRING().notNull());
    }

    private static CallExpression eq(ResolvedExpression left, ResolvedExpression right) {
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS, Arrays.asList(left, right), DataTypes.BOOLEAN());
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
