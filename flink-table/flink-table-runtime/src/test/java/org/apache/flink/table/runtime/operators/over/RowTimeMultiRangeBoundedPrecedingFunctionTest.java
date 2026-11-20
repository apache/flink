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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateMapView;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowTimeMultiRangeBoundedPrecedingFunction}. */
class RowTimeMultiRangeBoundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    // input row: (key, value, rowtime); one aggregate value per window is appended
    private static final RowDataHarnessAssertor ASSERTOR =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType()
                    });

    private static final GeneratedAggsHandleFunction DISTINCT_COUNT_HANDLER =
            new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
                @Override
                public AggsHandleFunction newInstance(ClassLoader classLoader) {
                    return new DistinctCountViewAggsHandleFunction(1);
                }
            };

    @Test
    void testEachWindowRetractsAtItsOwnRange() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(aggsHandleFunction);
        testHarness.open();

        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 2L, 500L));
        testHarness.processElement(insertRecord("key", 4L, 1200L));
        testHarness.processWatermark(new Watermark(1500L));
        // two rows at one timestamp share one result
        testHarness.processElement(insertRecord("key", 8L, 2100L));
        testHarness.processElement(insertRecord("key", 32L, 2100L));
        testHarness.processElement(insertRecord("key", 16L, 3000L));
        testHarness.processWatermark(new Watermark(3000L));

        // windows: 1000 ms and 2000 ms; the row at 100 leaves the first window at 1200 but is
        // still summed by the second window at 2100, and leaves it at 3000
        List<Object> expected = new ArrayList<>();
        expected.add(insertRecord("key", 1L, 100L, 1L, 1L));
        expected.add(insertRecord("key", 2L, 500L, 3L, 3L));
        expected.add(insertRecord("key", 4L, 1200L, 6L, 7L));
        expected.add(new Watermark(1500L));
        expected.add(insertRecord("key", 8L, 2100L, 44L, 47L));
        expected.add(insertRecord("key", 32L, 2100L, 44L, 47L));
        expected.add(insertRecord("key", 16L, 3000L, 56L, 60L));
        expected.add(new Watermark(3000L));
        ASSERTOR.assertOutputEquals("result mismatch", expected, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testDataViewStatesAreSeparatedPerWindow() throws Exception {
        // both handlers ask for a data view state with the same name; if they shared it, the
        // first window's retraction would remove values the second window still counts
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(DISTINCT_COUNT_HANDLER);
        testHarness.open();

        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 2L, 500L));
        testHarness.processElement(insertRecord("key", 4L, 1200L));
        testHarness.processElement(insertRecord("key", 8L, 2100L));
        testHarness.processElement(insertRecord("key", 16L, 3000L));
        testHarness.processWatermark(new Watermark(3000L));

        List<Object> expected = new ArrayList<>();
        expected.add(insertRecord("key", 1L, 100L, 1L, 1L));
        expected.add(insertRecord("key", 2L, 500L, 2L, 2L));
        expected.add(insertRecord("key", 4L, 1200L, 2L, 3L));
        expected.add(insertRecord("key", 8L, 2100L, 2L, 4L));
        expected.add(insertRecord("key", 16L, 3000L, 2L, 3L));
        expected.add(new Watermark(3000L));
        ASSERTOR.assertOutputEquals("result mismatch", expected, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testStateCleanup() throws Exception {
        RowTimeMultiRangeBoundedPrecedingFunction<RowData> function =
                createFunction(aggsHandleFunction);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.open();

        AbstractKeyedStateBackend<?> stateBackend =
                (AbstractKeyedStateBackend<?>) operator.getKeyedStateBackend();
        assertThat(stateBackend.numKeyValueStateEntries()).isEqualTo(0);

        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));
        testHarness.processWatermark(new Watermark(1000L));
        assertThat(stateBackend.numKeyValueStateEntries()).isGreaterThan(0);

        // the cleanup timer is set from the largest range (2000 ms) by the first row:
        // 100 + 2000 * 1.5 + 1; a 1000 ms range would have fired it at 1601
        testHarness.processWatermark(new Watermark(3100L));
        assertThat(stateBackend.numKeyValueStateEntries()).isGreaterThan(0);
        testHarness.processWatermark(new Watermark(3101L));
        assertThat(stateBackend.numKeyValueStateEntries()).isEqualTo(0);
        testHarness.close();
    }

    @Test
    void testLateRecordMetrics() throws Exception {
        RowTimeMultiRangeBoundedPrecedingFunction<RowData> function =
                createFunction(aggsHandleFunction);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(new KeyedProcessOperator<>(function));
        testHarness.open();

        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));
        testHarness.processWatermark(new Watermark(500L));
        testHarness.processElement(insertRecord("key", 1L, 400L));

        assertThat(function.getCounter().getCount()).isEqualTo(1L);
        testHarness.close();
    }

    private RowTimeMultiRangeBoundedPrecedingFunction<RowData> createFunction(
            GeneratedAggsHandleFunction handler) {
        return new RowTimeMultiRangeBoundedPrecedingFunction<>(
                new GeneratedAggsHandleFunction[] {handler, handler},
                new LogicalType[][] {accTypes, accTypes},
                inputFieldTypes,
                new long[] {1000L, 2000L},
                2);
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            GeneratedAggsHandleFunction handler) throws Exception {
        return createTestHarness(new KeyedProcessOperator<>(createFunction(handler)));
    }

    /**
     * Counts the distinct values of one field, kept in a data view state named like the ones that
     * the code generator creates for the first aggregate of a handler.
     */
    private static class DistinctCountViewAggsHandleFunction implements AggsHandleFunction {

        private final int inputIndex;
        private StateMapView<?, Long, Long> view;

        DistinctCountViewAggsHandleFunction(int inputIndex) {
            this.inputIndex = inputIndex;
        }

        @Override
        public void open(StateDataViewStore store) throws Exception {
            view =
                    store.getStateMapView(
                            "agg0$distinct",
                            false,
                            LongSerializer.INSTANCE,
                            LongSerializer.INSTANCE);
        }

        @Override
        public void accumulate(RowData input) throws Exception {
            long value = input.getLong(inputIndex);
            Long count = view.get(value);
            view.put(value, count == null ? 1L : count + 1);
        }

        @Override
        public void retract(RowData input) throws Exception {
            long value = input.getLong(inputIndex);
            Long count = view.get(value);
            if (count == null || count <= 1) {
                view.remove(value);
            } else {
                view.put(value, count - 1);
            }
        }

        @Override
        public RowData getValue() throws Exception {
            long distinct = 0;
            for (Long ignored : view.keys()) {
                distinct++;
            }
            return GenericRowData.of(distinct);
        }

        @Override
        public void merge(RowData accumulator) {}

        @Override
        public void setAccumulators(RowData accumulator) {}

        @Override
        public void resetAccumulators() {}

        @Override
        public RowData getAccumulators() {
            return GenericRowData.of(0L);
        }

        @Override
        public RowData createAccumulators() {
            return GenericRowData.of(0L);
        }

        @Override
        public void setWindowSize(int windowSize) {}

        @Override
        public void cleanup() {
            view.clear();
        }

        @Override
        public void close() {}
    }
}
