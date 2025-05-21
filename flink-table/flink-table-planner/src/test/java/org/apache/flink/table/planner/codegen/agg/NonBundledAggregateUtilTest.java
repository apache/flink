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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.agg.BundledKeySegment;
import org.apache.flink.table.planner.codegen.agg.NonBundledAggregateUtil.NonBundledSegmentResult;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NonBundledAggregateUtil}. */
public class NonBundledAggregateUtilTest {

    @Test
    public void testAvg() throws Exception {
        // This simulates an AVG call as the second value and second and third acc entries.
        AggsHandleFunction handle = new AvgAggsHandleFunction(2, 1, 3, 1);
        NonBundledSegmentResult result =
                NonBundledAggregateUtil.executeAsBundle(
                        handle,
                        new BundledKeySegment(
                                null,
                                Arrays.asList(GenericRowData.of(2L), GenericRowData.of(4L)),
                                null,
                                false));
        assertThat(result.getAccumulator()).isEqualTo(GenericRowData.of(null, 2L, 6L));
        assertThat(result.getStartingValue()).isEqualTo(GenericRowData.of(null, null));
        assertThat(result.getFinalValue()).isEqualTo(GenericRowData.of(null, 3L));

        result =
                NonBundledAggregateUtil.executeAsBundle(
                        handle,
                        new BundledKeySegment(
                                null,
                                Arrays.asList(
                                        GenericRowData.of(7L),
                                        GenericRowData.of(9L),
                                        GenericRowData.ofKind(RowKind.DELETE, 7L)),
                                GenericRowData.of(null, 2L, 6L),
                                false));
        assertThat(result.getAccumulator()).isEqualTo(GenericRowData.of(null, 3L, 15L));
        assertThat(result.getStartingValue()).isEqualTo(GenericRowData.of(null, 3L));
        assertThat(result.getFinalValue()).isEqualTo(GenericRowData.of(null, 5L));
    }

    private static class AvgAggsHandleFunction implements AggsHandleFunction {

        private final int totalValues;
        private final int valueIndex;
        private final int totalAccFields;
        private final int accIndex;

        AvgAggsHandleFunction(int totalValues, int valueIndex, int totalAccFields, int accIndex) {
            this.totalValues = totalValues;
            this.valueIndex = valueIndex;
            this.totalAccFields = totalAccFields;
            this.accIndex = accIndex;
        }

        private GenericRowData acc;

        @Override
        public RowData getValue() throws Exception {
            Object[] fields = new Object[totalValues];
            if (!(acc == null || acc.getLong(accIndex) == 0)) {
                fields[valueIndex] = acc.getLong(accIndex + 1) / acc.getLong(accIndex);
            }
            return GenericRowData.of(fields);
        }

        @Override
        public void setWindowSize(int windowSize) {}

        @Override
        public void open(StateDataViewStore store) throws Exception {}

        @Override
        public void accumulate(RowData input) throws Exception {
            acc.setField(accIndex, acc.getLong(accIndex) + 1);
            acc.setField(accIndex + 1, acc.getLong(accIndex + 1) + input.getLong(0));
        }

        @Override
        public void retract(RowData input) throws Exception {
            acc.setField(accIndex, acc.getLong(accIndex) - 1);
            acc.setField(accIndex + 1, acc.getLong(accIndex + 1) - input.getLong(0));
        }

        @Override
        public void merge(RowData accumulators) throws Exception {}

        @Override
        public void setAccumulators(RowData accumulators) throws Exception {
            acc = (GenericRowData) accumulators;
        }

        @Override
        public void resetAccumulators() throws Exception {}

        @Override
        public RowData getAccumulators() throws Exception {
            return acc;
        }

        @Override
        public RowData createAccumulators() throws Exception {
            Object[] fields = new Object[totalAccFields];
            fields[accIndex] = 0L;
            fields[accIndex + 1] = 0L;
            return GenericRowData.of(fields);
        }

        @Override
        public void cleanup() throws Exception {}

        @Override
        public void close() throws Exception {}
    }
}
