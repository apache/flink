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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests GEOGRAPHY values across operator checkpoint and restore paths. */
class GeographyStateRestoreTest {

    private static final byte[] POINT_1_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, 0, 64};

    private static final byte[] POINT_2_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 24, 64};

    private static final byte[] POINT_3_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 34, 64, 0, 0, 0, 0, 0, 0, 36, 64};

    private static final RowType MIXED_ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new BigIntType(false),
                        new VarCharType(),
                        new BooleanType(),
                        new TimestampType(3),
                        new VarCharType(),
                        new GeographyType()
                    },
                    new String[] {"id", "name", "active", "event_ts", "note", "geog"});

    @Test
    void testGeographyStateSurvivesCheckpointAndRestore() throws Exception {
        final RowData firstRow =
                rowOf(
                        1L,
                        "alpha",
                        true,
                        LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_000_000),
                        "before restore",
                        POINT_1_WKB);
        final RowData secondRow =
                rowOf(
                        1L,
                        "beta",
                        false,
                        LocalDateTime.of(2024, 2, 3, 4, 5, 6, 789_000_000),
                        null,
                        POINT_2_WKB);
        final RowData thirdRow =
                rowOf(
                        1L,
                        "gamma",
                        true,
                        LocalDateTime.of(2024, 3, 4, 5, 6, 7, 456_000_000),
                        "after restore",
                        POINT_3_WKB);

        final OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness =
                createHarness()) {
            harness.open();
            harness.processElement(new StreamRecord<>(firstRow));
            assertThat(harness.getOutput()).isEmpty();
            snapshot = harness.snapshot(1L, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> restoredHarness =
                createHarness()) {
            restoredHarness.initializeState(snapshot);
            restoredHarness.open();

            restoredHarness.processElement(new StreamRecord<>(secondRow));
            restoredHarness.processElement(new StreamRecord<>(thirdRow));

            final List<Object> restoredOutput =
                    TestHarnessUtil.getRawElementsFromOutput(restoredHarness.getOutput());
            assertThat(restoredOutput).hasSize(2);

            assertRowMatches(firstRow, (RowData) restoredOutput.get(0));
            assertRowMatches(secondRow, (RowData) restoredOutput.get(1));
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> createHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new RememberingGeographyProcessFunction()),
                row -> row.getLong(0),
                Types.LONG);
    }

    private static RowData rowOf(
            long id,
            String name,
            Boolean active,
            LocalDateTime eventTime,
            String note,
            byte[] geographyWkb) {
        return GenericRowData.of(
                id,
                name == null ? null : StringData.fromString(name),
                active,
                eventTime == null ? null : TimestampData.fromLocalDateTime(eventTime),
                note == null ? null : StringData.fromString(note),
                geographyWkb == null ? null : GeographyData.fromBytes(geographyWkb));
    }

    private static void assertRowMatches(RowData expected, RowData actual) {
        assertThat(actual.getLong(0)).isEqualTo(expected.getLong(0));
        assertThat(actual.isNullAt(1) ? null : actual.getString(1).toString())
                .isEqualTo(expected.isNullAt(1) ? null : expected.getString(1).toString());
        assertThat(actual.isNullAt(2) ? null : actual.getBoolean(2))
                .isEqualTo(expected.isNullAt(2) ? null : expected.getBoolean(2));
        assertThat(actual.isNullAt(3) ? null : actual.getTimestamp(3, 3))
                .isEqualTo(expected.isNullAt(3) ? null : expected.getTimestamp(3, 3));
        assertThat(actual.isNullAt(4) ? null : actual.getString(4).toString())
                .isEqualTo(expected.isNullAt(4) ? null : expected.getString(4).toString());
        assertThat(actual.isNullAt(5) ? null : actual.getGeography(5).toBytes())
                .isEqualTo(expected.isNullAt(5) ? null : expected.getGeography(5).toBytes());
    }

    private static final class RememberingGeographyProcessFunction
            extends KeyedProcessFunction<Long, RowData, RowData> {

        private transient ValueState<RowData> previousRowState;

        @Override
        public void open(OpenContext openContext) {
            previousRowState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "previous-row", InternalTypeInfo.of(MIXED_ROW_TYPE)));
        }

        @Override
        public void processElement(RowData value, Context ctx, Collector<RowData> out)
                throws Exception {
            final RowData previous = previousRowState.value();
            if (previous != null) {
                out.collect(previous);
            }
            previousRowState.update(value);
        }
    }
}
