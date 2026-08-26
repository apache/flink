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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OutputConversionOperator}. */
class OutputConversionOperatorTest {

    private static final int ROWTIME_INDEX = 1;

    @Test
    void testNullRowtimeMetadataIsEmittedWithoutTimestamp() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, Object> harness = createHarness(-1, true)) {
            harness.open();

            harness.processElement(new StreamRecord<>(row(null)));

            final List<StreamRecord<? extends Object>> output =
                    harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).hasTimestamp()).isFalse();
        }
    }

    @Test
    void testNullRowtimeFieldIsEmittedWithoutTimestamp() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, Object> harness =
                createHarness(ROWTIME_INDEX, false)) {
            harness.open();

            harness.processElement(new StreamRecord<>(row(null)));

            final List<StreamRecord<? extends Object>> output =
                    harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).hasTimestamp()).isFalse();
        }
    }

    @Test
    void testNullRowtimeMetadataDoesNotInheritPreviousTimestamp() throws Exception {
        assertNullRowtimeDoesNotInheritPreviousTimestamp(createHarness(-1, true));
    }

    @Test
    void testNullRowtimeFieldDoesNotInheritPreviousTimestamp() throws Exception {
        assertNullRowtimeDoesNotInheritPreviousTimestamp(createHarness(ROWTIME_INDEX, false));
    }

    private static void assertNullRowtimeDoesNotInheritPreviousTimestamp(
            OneInputStreamOperatorTestHarness<RowData, Object> harness) throws Exception {
        try (harness) {
            harness.open();

            harness.processElement(new StreamRecord<>(row(1000L)));
            harness.processElement(new StreamRecord<>(row(null)));

            final List<StreamRecord<? extends Object>> output =
                    harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getTimestamp()).isEqualTo(1000L);
            assertThat(output.get(1).hasTimestamp()).isFalse();
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, Object> createHarness(
            int rowtimeIndex, boolean consumeRowtimeMetadata) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new OutputConversionOperator(
                        null, new ToStringConverter(), rowtimeIndex, consumeRowtimeMetadata));
    }

    private static RowData row(Long rowtime) {
        return GenericRowData.of(
                StringData.fromString("payload"),
                rowtime == null ? null : TimestampData.fromEpochMillis(rowtime));
    }

    private static class ToStringConverter implements DataStructureConverter {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(RuntimeConverter.Context context) {}

        @Override
        public Object toExternal(Object internalStructure) {
            return String.valueOf(internalStructure);
        }
    }
}
