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

package org.apache.flink.table.planner.codegen.agg;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;
import org.apache.flink.table.planner.codegen.agg.BundledResultCombiner.Combiner;
import org.apache.flink.table.planner.codegen.agg.NonBundledAggregateUtil.NonBundledSegmentResult;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BundledResultCombiner}. */
public class BundledResultCombinerTest {

    private Combiner prepareOneBundledOneNonBundled(boolean implicitCounter) {
        // Simulate SUM
        Combiner combiner =
                new BundledResultCombiner(
                                RowType.of(RowType.of(new BigIntType()), new BigIntType()),
                                RowType.of(new BigIntType(), new BigIntType()))
                        .newCombiner();

        combiner.add(
                0,
                Optional.of(CompletableFuture.completedFuture(createBundledSum())),
                Optional.empty(),
                true,
                true,
                0,
                1);
        combiner.add(
                1,
                Optional.empty(),
                Optional.of(createNonBundledSum(1)),
                !implicitCounter,
                false,
                1,
                2);
        return combiner;
    }

    @Test
    public void testOneBundledOneNonBundled() throws Exception {
        Combiner combiner = prepareOneBundledOneNonBundled(false);
        BundledKeySegmentApplied combined = combiner.combine();
        assertThat(combined.getAccumulator())
                .isEqualTo(GenericRowData.of(GenericRowData.of(2L), 3L));
        assertThat(combined.getStartingValue()).isEqualTo(GenericRowData.of(0L, 1L));
        assertThat(combined.getFinalValue()).isEqualTo(GenericRowData.of(2L, 3L));
        assertThat(combined.getUpdatedValuesAfterEachRow())
                .isEqualTo(
                        Arrays.asList(
                                GenericRowData.of(0L, 1L),
                                GenericRowData.of(1L, 2L),
                                GenericRowData.of(2L, 3L)));
    }

    @Test
    public void testOneBundledOneNonBundledImplicitCounter() throws Exception {
        Combiner combiner = prepareOneBundledOneNonBundled(true);
        BundledKeySegmentApplied combined = combiner.combine();
        assertThat(combined.getAccumulator())
                .isEqualTo(GenericRowData.of(GenericRowData.of(2L), 3L));
        assertThat(combined.getStartingValue()).isEqualTo(GenericRowData.of(0L));
        assertThat(combined.getFinalValue()).isEqualTo(GenericRowData.of(2L));
        assertThat(combined.getUpdatedValuesAfterEachRow())
                .isEqualTo(
                        Arrays.asList(
                                GenericRowData.of(0L),
                                GenericRowData.of(1L),
                                GenericRowData.of(2L)));
    }

    @Test
    public void testTwoBundledOneNonBundled() throws Exception {
        Combiner combiner =
                new BundledResultCombiner(
                                RowType.of(
                                        RowType.of(new BigIntType()),
                                        RowType.of(new VarCharType(10)),
                                        new BigIntType()),
                                RowType.of(new BigIntType(), new VarCharType(10), new BigIntType()))
                        .newCombiner();

        combiner.add(
                0,
                Optional.of(CompletableFuture.completedFuture(createBundledSum())),
                Optional.empty(),
                true,
                true,
                0,
                1);
        combiner.add(
                1,
                Optional.of(CompletableFuture.completedFuture(createBundledStringAppend())),
                Optional.empty(),
                true,
                true,
                1,
                2);
        combiner.add(2, Optional.empty(), Optional.of(createNonBundledSum(2)), true, false, 2, 3);
        BundledKeySegmentApplied combined = combiner.combine();
        assertThat(combined.getAccumulator())
                .isEqualTo(
                        GenericRowData.of(
                                GenericRowData.of(2L),
                                GenericRowData.of(StringData.fromString("abc")),
                                3L));
        assertThat(combined.getStartingValue())
                .isEqualTo(GenericRowData.of(0L, StringData.fromString("a"), 1L));
        assertThat(combined.getFinalValue())
                .isEqualTo(GenericRowData.of(2L, StringData.fromString("abc"), 3L));
        assertThat(combined.getUpdatedValuesAfterEachRow())
                .isEqualTo(
                        Arrays.asList(
                                GenericRowData.of(0L, StringData.fromString("a"), 1L),
                                GenericRowData.of(1L, StringData.fromString("ab"), 2L),
                                GenericRowData.of(2L, StringData.fromString("abc"), 3L)));
    }

    @Test
    public void testTwoBundledOneNonBundledDifferentOrder() throws Exception {
        Combiner combiner =
                new BundledResultCombiner(
                                RowType.of(
                                        RowType.of(new BigIntType()),
                                        new BigIntType(),
                                        RowType.of(new VarCharType(10))),
                                RowType.of(new BigIntType(), new BigIntType(), new VarCharType(10)))
                        .newCombiner();

        combiner.add(
                0,
                Optional.of(CompletableFuture.completedFuture(createBundledSum())),
                Optional.empty(),
                true,
                true,
                0,
                1);
        combiner.add(1, Optional.empty(), Optional.of(createNonBundledSum(1)), true, false, 1, 2);
        combiner.add(
                2,
                Optional.of(CompletableFuture.completedFuture(createBundledStringAppend())),
                Optional.empty(),
                true,
                true,
                2,
                3);
        BundledKeySegmentApplied combined = combiner.combine();
        assertThat(combined.getAccumulator())
                .isEqualTo(
                        GenericRowData.of(
                                GenericRowData.of(2L),
                                3L,
                                GenericRowData.of(StringData.fromString("abc"))));
        assertThat(combined.getStartingValue())
                .isEqualTo(GenericRowData.of(0L, 1L, StringData.fromString("a")));
        assertThat(combined.getFinalValue())
                .isEqualTo(GenericRowData.of(2L, 3L, StringData.fromString("abc")));
        assertThat(combined.getUpdatedValuesAfterEachRow())
                .isEqualTo(
                        Arrays.asList(
                                GenericRowData.of(0L, 1L, StringData.fromString("a")),
                                GenericRowData.of(1L, 2L, StringData.fromString("ab")),
                                GenericRowData.of(2L, 3L, StringData.fromString("abc"))));
    }

    @Test
    public void testOneBundledOneNonBundledWithMultiSizeAccumulator() throws Exception {
        // Note that the accumulator now has two entries for the non bundled
        Combiner combiner =
                new BundledResultCombiner(
                                RowType.of(
                                        RowType.of(new BigIntType()),
                                        new BigIntType(),
                                        new IntType()),
                                RowType.of(new BigIntType(), new BigIntType()))
                        .newCombiner();

        combiner.add(
                0,
                Optional.of(CompletableFuture.completedFuture(createBundledSum())),
                Optional.empty(),
                true,
                true,
                0,
                1);
        combiner.add(
                1,
                Optional.empty(),
                Optional.of(
                        new NonBundledSegmentResult(
                                GenericRowData.of(null, 2L, 6),
                                GenericRowData.of(null, 10L),
                                GenericRowData.of(null, 20L),
                                Arrays.asList(
                                        GenericRowData.of(null, 10L),
                                        GenericRowData.of(null, 15L),
                                        GenericRowData.of(null, 20L)))),
                true,
                false,
                1,
                3);
        BundledKeySegmentApplied combined = combiner.combine();
        assertThat(combined.getAccumulator())
                .isEqualTo(GenericRowData.of(GenericRowData.of(2L), 2L, 6));
        assertThat(combined.getStartingValue()).isEqualTo(GenericRowData.of(0L, 10L));
        assertThat(combined.getFinalValue()).isEqualTo(GenericRowData.of(2L, 20L));
        assertThat(combined.getUpdatedValuesAfterEachRow())
                .isEqualTo(
                        Arrays.asList(
                                GenericRowData.of(0L, 10L),
                                GenericRowData.of(1L, 15L),
                                GenericRowData.of(2L, 20L)));
    }

    private BundledKeySegmentApplied createBundledSum() {
        return new BundledKeySegmentApplied(
                GenericRowData.of(2L),
                GenericRowData.of(0L),
                GenericRowData.of(2L),
                Arrays.asList(GenericRowData.of(0L), GenericRowData.of(1L), GenericRowData.of(2L)));
    }

    private BundledKeySegmentApplied createBundledStringAppend() {
        return new BundledKeySegmentApplied(
                GenericRowData.of(StringData.fromString("abc")),
                GenericRowData.of(StringData.fromString("a")),
                GenericRowData.of(StringData.fromString("abc")),
                Arrays.asList(
                        GenericRowData.of(StringData.fromString("a")),
                        GenericRowData.of(StringData.fromString("ab")),
                        GenericRowData.of(StringData.fromString("abc"))));
    }

    private NonBundledSegmentResult createNonBundledSum(int index) {
        return new NonBundledSegmentResult(
                createRowDataWith(index, 3L),
                createRowDataWith(index, 1L),
                createRowDataWith(index, 3L),
                Arrays.asList(
                        createRowDataWith(index, 1L),
                        createRowDataWith(index, 2L),
                        createRowDataWith(index, 3L)));
    }

    private GenericRowData createRowDataWith(int index, Object obj) {
        Object[] fields = new Object[] {null, null, null};
        fields[index] = obj;
        return GenericRowData.of(fields);
    }
}
