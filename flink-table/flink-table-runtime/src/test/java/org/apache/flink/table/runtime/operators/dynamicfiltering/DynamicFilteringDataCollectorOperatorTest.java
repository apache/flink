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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamicFilteringDataCollectorOperator}. */
class DynamicFilteringDataCollectorOperatorTest {

    @Test
    void testCollectDynamicFilteringData() throws Exception {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        List<Integer> indexes = Arrays.asList(0, 1, 3);
        MockOperatorEventGateway gateway = new MockOperatorEventGateway();

        DynamicFilteringDataCollectorOperator operator =
                new DynamicFilteringDataCollectorOperator(rowType, indexes, -1, gateway);
        ConcurrentLinkedQueue<Object> output;
        try (OneInputStreamOperatorTestHarness<RowData, Object> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            output = harness.getOutput();
            harness.setup();
            harness.open();

            for (long i = 0L; i < 3L; i++) {
                harness.processElement(rowData(1, 1L, 0, "a"), i);
            }
            harness.processElement(rowData(2, 1L, 0, null), 3L);

            // operator.finish is called when closing
        }

        assertThat(output).isEmpty();

        assertThat(gateway.getEventsSent()).hasSize(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent dynamicFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(dynamicFilteringEvent).isInstanceOf(DynamicFilteringEvent.class);

        DynamicFilteringData data = ((DynamicFilteringEvent) dynamicFilteringEvent).getData();
        assertThat(data.isFiltering()).isTrue();
        assertThat(data.getData()).hasSize(2);
        assertThat(data.contains(rowData(1, 1L, "a"))).isTrue();
        assertThat(data.contains(rowData(2, 1L, null))).isTrue();
    }

    @Test
    void testExceedsThreshold() throws Exception {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        List<Integer> indexes = Arrays.asList(0, 1, 3);
        MockOperatorEventGateway gateway = new MockOperatorEventGateway();
        // Can hold at most 2 rows
        int thresholds = 100;

        DynamicFilteringDataCollectorOperator operator =
                new DynamicFilteringDataCollectorOperator(rowType, indexes, thresholds, gateway);
        try (OneInputStreamOperatorTestHarness<RowData, Object> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup();
            harness.open();

            harness.processElement(rowData(1, 1L, 0, "a"), 1L);
            harness.processElement(rowData(2, 1L, 0, "b"), 2L);
            harness.processElement(rowData(3, 1L, 0, "c"), 3L);
        }

        assertThat(gateway.getEventsSent()).hasSize(1);
        OperatorEvent event = gateway.getEventsSent().get(0);
        assertThat(event).isInstanceOf(SourceEventWrapper.class);
        SourceEvent dynamicFilteringEvent = ((SourceEventWrapper) event).getSourceEvent();
        assertThat(dynamicFilteringEvent).isInstanceOf(DynamicFilteringEvent.class);
        DynamicFilteringData data = ((DynamicFilteringEvent) dynamicFilteringEvent).getData();
        assertThat(data.isFiltering()).isFalse();
    }

    private RowData rowData(Object... values) {
        GenericRowData rowData = new GenericRowData(values.length);
        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            value = value instanceof String ? new BinaryStringData((String) value) : value;
            rowData.setField(i, value);
        }
        return rowData;
    }
}
