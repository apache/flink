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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowTimeRangeBoundedPrecedingFunction}. */
class RowTimeRangeBoundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    @Test
    void testStateCleanup() throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 2000, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        AbstractKeyedStateBackend stateBackend =
                (AbstractKeyedStateBackend) operator.getKeyedStateBackend();

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("Initial state is not empty")
                .isEqualTo(0);

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(1000L));
        // at this moment we expect the function to have some records in state

        testHarness.processWatermark(new Watermark(4000L));
        // at this moment the function should have cleaned up states exclude lastTriggeringTsState

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("State has not been cleaned up")
                .isEqualTo(1);

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        ValueState<Long> lastTriggeringTsState =
                (ValueState<Long>)
                        stateBackend.getOrCreateKeyedState(
                                VoidNamespaceSerializer.INSTANCE, lastTriggeringTsDescriptor);
        assertThat(lastTriggeringTsState.value()).isEqualTo(500L);
    }

    @Test
    void testLateRecordMetrics() throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 2000, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        Counter counter = function.getCounter();

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(500L));

        // late record
        testHarness.processElement(insertRecord("key", 1L, 400L));

        assertThat(counter.getCount()).isEqualTo(1L);
    }

    @Test
    void testProcessElementAfterCleanUpStates() throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 2000, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        Counter counter = function.getCounter();

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        // process watermark and clean up states
        testHarness.processWatermark(new Watermark(1000L));
        testHarness.processWatermark(new Watermark(4000L));

        // late record
        testHarness.processElement(insertRecord("key", 1L, 400L));

        assertThat(counter.getCount()).isEqualTo(1L);
    }
}
