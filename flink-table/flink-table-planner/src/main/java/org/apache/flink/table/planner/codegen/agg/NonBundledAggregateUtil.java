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
import org.apache.flink.table.functions.agg.BundledKeySegment;
import org.apache.flink.table.functions.agg.BundledKeySegmentApplied;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Utility class for executing non-bundled aggregate functions. */
public class NonBundledAggregateUtil {

    /**
     * Executes the given non-bundled aggregate function calls for each segment in the list.
     *
     * @param handle The aggregate function to execute.
     * @param segment The segment to execute the function for.
     * @return The results of the function calls.
     */
    public static NonBundledSegmentResult executeAsBundle(
            AggsHandleFunction handle, BundledKeySegment segment) throws Exception {
        RowData acc = segment.getAccumulator();

        if (acc == null) {
            acc = handle.createAccumulators();
        }
        handle.setAccumulators(acc);
        RowData startingValues = handle.getValue();
        List<RowData> updatedValuesAfterEachRow = new ArrayList<>();
        for (org.apache.flink.table.data.RowData row : segment.getRows()) {
            if (org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg(row)) {
                handle.accumulate(row);
            } else {
                handle.retract(row);
            }
            if (segment.getUpdatedValuesAfterEachRow()) {
                updatedValuesAfterEachRow.add(handle.getValue());
            }
        }

        acc = handle.getAccumulators();

        org.apache.flink.table.data.RowData finalValues = handle.getValue();

        return new NonBundledSegmentResult(
                acc, startingValues, finalValues, updatedValuesAfterEachRow);
    }

    /** Result of all non-bundled aggregate function calls for this segment. */
    public static class NonBundledSegmentResult {
        private final RowData accumulator;
        private final RowData startingValue;
        private final RowData finalValue;
        private final List<RowData> updatedValuesAfterEachRow;

        public NonBundledSegmentResult(
                RowData accumulator,
                RowData startingValue,
                RowData finalValue,
                List<RowData> updatedValuesAfterEachRow) {
            this.accumulator = accumulator;
            this.startingValue = startingValue;
            this.finalValue = finalValue;
            this.updatedValuesAfterEachRow = updatedValuesAfterEachRow;
        }

        /**
         * Convert this result to a {@link BundledKeySegmentApplied} for the given call index. This
         * extracts the accumulator and values as though the call was done with a bundled call,
         * giving a view of them so that they can be combined with the bundled results.
         *
         * @param index The index of the call.
         * @param shouldIncludeValue Whether the value should be included or ignored.
         * @param accumulatorFields The fields to include in the view of the accumulator.
         * @param accTypeInfo The type information for the accumulator.
         * @param valueType The type information for the values.
         * @return The bundled data key segment update.
         */
        public BundledKeySegmentApplied asBundledDataKeySegmentUpdate(
                int index,
                boolean shouldIncludeValue,
                List<Integer> accumulatorFields,
                RowType accTypeInfo,
                RowType valueType) {
            return BundledKeySegmentApplied.of(
                    getAccumulatorViewForFields(accumulator, accumulatorFields, accTypeInfo),
                    shouldIncludeValue ? getValueForIndex(valueType, startingValue, index) : null,
                    shouldIncludeValue ? getValueForIndex(valueType, finalValue, index) : null,
                    shouldIncludeValue
                            ? updatedValuesAfterEachRow.stream()
                                    .map(v -> getValueForIndex(valueType, v, index))
                                    .collect(Collectors.toList())
                            : Collections.emptyList());
        }

        /** Wraps the accumulator fields in a row. */
        private RowData getAccumulatorViewForFields(
                RowData accumulator, List<Integer> accumulatorFields, RowType accumulatorType) {
            return GenericRowData.of(
                    accumulatorFields.stream()
                            .mapToInt(v -> v)
                            .mapToObj(
                                    v ->
                                            RowData.createFieldGetter(
                                                            accumulatorType.getTypeAt(v), v)
                                                    .getFieldOrNull(accumulator))
                            .toArray());
        }

        /** Extracts the value for the given index from the whole value. */
        private static RowData getValueForIndex(RowType valueType, RowData inputValue, int index) {
            return GenericRowData.of(
                    RowData.createFieldGetter(valueType.getTypeAt(index), index)
                            .getFieldOrNull(inputValue));
        }

        public RowData getAccumulator() {
            return accumulator;
        }

        public RowData getStartingValue() {
            return startingValue;
        }

        public RowData getFinalValue() {
            return finalValue;
        }

        public List<RowData> getUpdatedValuesAfterEachRow() {
            return updatedValuesAfterEachRow;
        }
    }
}
