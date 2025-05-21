/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.codegen.agg;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * For each {@link BundledKeySegmentApplied}, it combines with other corresponding bundled and
 * non-bundled results, producing a single {@link BundledKeySegmentApplied} of a combined result.
 * This allows an operator to utilize a uniform bundled API regardless of how many bundled or
 * non-bundled aggregate functions are used.
 *
 * <p>For example, imagine the following query: SELECT B1_SUM(a), B2_COUNT(b), SUM(c), COUNT(d) FROM
 * T;
 *
 * <p>Here, B1_SUM and B2_COUNT are bundled aggregate functions, and SUM and COUNT are non-bundled
 * aggregates. The bundled calls will be separate while both the non-bundled are produced using the
 * conventional {@link org.apache.flink.table.runtime.generated.AggsHandleFunction}. The result of
 * all three are then combined to a single {@link BundledKeySegmentApplied}.
 */
public class BundledResultCombiner implements Serializable {
    private static final long serialVersionUID = -3486860542451993040L;

    private final RowType accTypeInfo;
    private final RowType valueType;

    /**
     * Creates a new {@link Combiner} factory.
     *
     * @param accTypeInfo the accumulator type information
     * @param valueType the value type information
     */
    public BundledResultCombiner(RowType accTypeInfo, RowType valueType) {
        this.accTypeInfo = accTypeInfo;
        this.valueType = valueType;
    }

    /** Creates a new {@link Combiner} instance. */
    public Combiner newCombiner() {
        return new Combiner(accTypeInfo, valueType);
    }

    /**
     * Combines all kinds of aggregates into a single result conforming to the bundled interface, as
     * if it were not bundled at all.
     */
    public static class Combiner
            implements Serializable, SupplierWithException<BundledKeySegmentApplied, Exception> {

        private static final long serialVersionUID = -3348403465172218589L;

        private final RowType accTypeInfo;
        private final RowType valueType;

        private final List<UpdateMetadata> updates = new ArrayList<>();

        private Combiner(RowType accTypeInfo, RowType valueType) {
            this.accTypeInfo = accTypeInfo;
            this.valueType = valueType;
        }

        /**
         * Adds a new update to the combiner. Note that one of the two update types should be
         * present.
         *
         * @param index the index of the update
         * @param bundledDataKeySegmentUpdate the bundled data key segment update
         * @param nonBundledResult the non-bundled result
         * @param shouldIncludeValue whether the value is included in the result (or internal)
         * @param isBundled whether the result is bundled
         * @param accIndexStart the start index of the accumulator
         * @param accIndexEnd the end index of the accumulator
         */
        public void add(
                final int index,
                final Optional<CompletableFuture<BundledKeySegmentApplied>>
                        bundledDataKeySegmentUpdate,
                final Optional<NonBundledAggregateUtil.NonBundledSegmentResult> nonBundledResult,
                final boolean shouldIncludeValue,
                final boolean isBundled,
                final int accIndexStart,
                final int accIndexEnd) {
            final CompletableFuture<BundledKeySegmentApplied> updateToCombine;
            if (bundledDataKeySegmentUpdate.isPresent()) {
                updateToCombine = bundledDataKeySegmentUpdate.get();
            } else {
                Preconditions.checkArgument(nonBundledResult.isPresent());
                updateToCombine =
                        CompletableFuture.completedFuture(
                                nonBundledResult
                                        .get()
                                        .asBundledDataKeySegmentUpdate(
                                                index,
                                                shouldIncludeValue,
                                                indexList(accIndexStart, accIndexEnd),
                                                accTypeInfo,
                                                valueType));
            }
            updates.add(new UpdateMetadata(index, updateToCombine, shouldIncludeValue, isBundled));
        }

        /**
         * Combines all the updates added to the combiner.
         *
         * @return the combined updates
         */
        public BundledKeySegmentApplied combine() throws Exception {
            BundledKeySegmentApplied result =
                    BundledResultCombiner.combineUpdates(
                            accTypeInfo, valueType, updates.toArray(new UpdateMetadata[0]));
            updates.clear();
            return result;
        }

        @Override
        public BundledKeySegmentApplied get() throws Exception {
            return combine();
        }
    }

    private static BundledKeySegmentApplied combineUpdates(
            RowType accTypeInfo, RowType valueType, UpdateMetadata... updates) throws Exception {
        List<UpdateMetadata> ordered = new ArrayList<>(updates.length);
        for (int i = 0; i < updates.length; i++) {
            ordered.add(null);
        }
        for (UpdateMetadata updateMetadata : updates) {
            ordered.set(updateMetadata.index, updateMetadata);
        }
        Map<Integer, Boolean> shouldIncludeValues =
                ordered.stream().collect(Collectors.toMap(p -> p.index, p -> p.shouldIncludeValue));
        Map<Integer, Boolean> isBundled =
                ordered.stream().collect(Collectors.toMap(p -> p.index, p -> p.isBundled));
        List<BundledKeySegmentApplied> allUpdates =
                ordered.stream()
                        .map(
                                p -> {
                                    // This should only be invoked after the futures have all
                                    // completed, so calling join shouldn't block.
                                    return p.update.join();
                                })
                        .collect(Collectors.toList());
        return combineSegment(shouldIncludeValues, isBundled, accTypeInfo, valueType, allUpdates);
    }

    private static BundledKeySegmentApplied combineSegment(
            Map<Integer, Boolean> shouldIncludeValues,
            Map<Integer, Boolean> isBundled,
            RowType accumulatorType,
            RowType valueType,
            List<BundledKeySegmentApplied> ithEntries) {
        List<RowData> accs = new ArrayList<>();
        List<RowData> startingValues = new ArrayList<>();
        List<RowData> finalValues = new ArrayList<>();
        List<List<RowData>> updatedValuesAfterEachRow = new ArrayList<>();
        for (int i = 0; i < ithEntries.size(); i++) {
            BundledKeySegmentApplied update = ithEntries.get(i);
            // Non bundled accumulators are already wrapped in a row to contain them, so should not
            // create another layer.
            boolean avoidWrappingInRow = !isBundled.get(i);
            accs.add(
                    avoidWrappingInRow
                            ? update.getAccumulator()
                            : GenericRowData.of(update.getAccumulator()));

            if (shouldIncludeValues.get(i)) {
                RowData startingValue = update.getStartingValue();
                RowData finalValue = update.getFinalValue();

                startingValues.add(startingValue);
                finalValues.add(finalValue);

                if (updatedValuesAfterEachRow.isEmpty()) {
                    for (int j = 0; j < update.getUpdatedValuesAfterEachRow().size(); j++) {
                        updatedValuesAfterEachRow.add(new ArrayList<>(ithEntries.size()));
                        updatedValuesAfterEachRow
                                .get(j)
                                .add(update.getUpdatedValuesAfterEachRow().get(j));
                    }
                } else {
                    Preconditions.checkState(
                            updatedValuesAfterEachRow.size()
                                    == update.getUpdatedValuesAfterEachRow().size());
                    for (int j = 0; j < update.getUpdatedValuesAfterEachRow().size(); j++) {
                        updatedValuesAfterEachRow
                                .get(j)
                                .add(update.getUpdatedValuesAfterEachRow().get(j));
                    }
                }
            }
        }

        final List<RowData> updatedValuesAfterEachRowFinal =
                updatedValuesAfterEachRow.stream()
                        .map(list -> mergeAllFields(list, valueType))
                        .collect(Collectors.toList());

        return new BundledKeySegmentApplied(
                mergeAllFields(accs, accumulatorType),
                mergeAllFields(startingValues, valueType),
                mergeAllFields(finalValues, valueType),
                updatedValuesAfterEachRowFinal);
    }

    // Merges all fields from the given list of rows into a single row.
    private static GenericRowData mergeAllFields(List<RowData> rowData, RowType types) {
        int size = rowData.stream().mapToInt(RowData::getArity).sum();
        final Object[] fieldByPosition = new Object[size];
        int total = 0;
        for (RowData rd : rowData) {
            for (int pos = 0; pos < rd.getArity(); pos++) {
                final Object value =
                        RowData.createFieldGetter(types.getTypeAt(total), pos).getFieldOrNull(rd);
                fieldByPosition[total++] = value;
            }
        }
        return GenericRowData.of(fieldByPosition);
    }

    private static class UpdateMetadata {

        private final int index;
        private final CompletableFuture<BundledKeySegmentApplied> update;
        private final boolean shouldIncludeValue;
        private final boolean isBundled;

        public UpdateMetadata(
                int index,
                CompletableFuture<BundledKeySegmentApplied> update,
                boolean shouldIncludeValue,
                boolean isBundled) {
            this.index = index;
            this.update = update;
            this.shouldIncludeValue = shouldIncludeValue;
            this.isBundled = isBundled;
        }
    }

    private static List<Integer> indexList(int start, int end) {
        return java.util.stream.IntStream.range(start, end)
                .boxed()
                .collect(java.util.stream.Collectors.toList());
    }
}
