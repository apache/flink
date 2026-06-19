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

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link InputSortBuffer}. */
class InputSortBufferTest {

    private static final int TIME_COLUMN = 0;

    private Map<Long, List<RowData>> backingMap;
    private MapState<Long, List<RowData>> mapState;
    private InternalTimerService<VoidNamespace> timerService;
    private Set<Long> registeredTimers;
    private List<RowData> emittedRows;
    private InputSortBuffer.SortedRowConsumer rowConsumer;

    @BeforeEach
    void setUp() {
        backingMap =
                new TreeMap<>(); // Use TreeMap to maintain sorted order for ordered backend tests
        mapState = createMockMapState();
        timerService = createMockTimerService();
        registeredTimers = new HashSet<>();
        emittedRows = new ArrayList<>();
        rowConsumer = emittedRows::add;
    }

    @Test
    void testBasicElementBuffering() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute
        buffer.updateWatermark(50);
        buffer.processElement(createRow(100, "A"));

        // Verify
        assertThat(backingMap).containsKey(100L);
        assertThat(backingMap.get(100L)).hasSize(1);
        assertThat(registeredTimers).contains(51L);
    }

    @Test
    void testLateEventFiltering() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute
        buffer.updateWatermark(100);
        buffer.processElement(createRow(50, "late"));

        // Verify: Row not stored
        assertThat(backingMap).doesNotContainKey(50L);
        assertThat(emittedRows).isEmpty();
    }

    @Test
    void testSortedEmissionOrderedBackend() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Process out-of-order timestamps
        buffer.updateWatermark(0);
        buffer.processElement(createRow(200, "C"));
        buffer.processElement(createRow(100, "A"));
        buffer.processElement(createRow(150, "B"));

        buffer.updateWatermark(200);
        buffer.onEventTime(createTimer(200));

        // Verify: Emitted in timestamp order
        assertThat(emittedRows).hasSize(3);
        assertThat(emittedRows.get(0).getLong(TIME_COLUMN)).isEqualTo(100);
        assertThat(emittedRows.get(1).getLong(TIME_COLUMN)).isEqualTo(150);
        assertThat(emittedRows.get(2).getLong(TIME_COLUMN)).isEqualTo(200);
        assertThat(backingMap).isEmpty();
        assertThat(registeredTimers).doesNotContain(200L); // Not re-registered
    }

    @Test
    void testSortedEmissionUnorderedBackend() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "hashmap");

        // Execute: Same as ordered backend test
        buffer.updateWatermark(0);
        buffer.processElement(createRow(200, "C"));
        buffer.processElement(createRow(100, "A"));
        buffer.processElement(createRow(150, "B"));

        buffer.updateWatermark(200);
        buffer.onEventTime(createTimer(200));

        // Verify: Same result (manual sort path)
        assertThat(emittedRows).hasSize(3);
        assertThat(emittedRows.get(0).getLong(TIME_COLUMN)).isEqualTo(100);
        assertThat(emittedRows.get(1).getLong(TIME_COLUMN)).isEqualTo(150);
        assertThat(emittedRows.get(2).getLong(TIME_COLUMN)).isEqualTo(200);
    }

    @Test
    void testSecondarySortingWithComparator() throws Exception {
        // Setup: Comparator that sorts by field[1] (StringData) ascending
        final RecordComparator comparator = createStringComparator();
        final InputSortBuffer buffer = createBuffer(comparator, "rocksdb");

        // Execute: Process rows with same timestamp but different IDs
        buffer.updateWatermark(0);
        buffer.processElement(createRow(100, "Z"));
        buffer.processElement(createRow(100, "A"));
        buffer.processElement(createRow(100, "M"));

        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        // Verify: Emitted in sorted order by ID
        assertThat(emittedRows).hasSize(3);
        assertThat(emittedRows.get(0).getString(1).toString()).isEqualTo("A");
        assertThat(emittedRows.get(1).getString(1).toString()).isEqualTo("M");
        assertThat(emittedRows.get(2).getString(1).toString()).isEqualTo("Z");
    }

    @Test
    void testNoComparatorInsertionOrder() throws Exception {
        // Setup: No comparator
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Multiple rows at same timestamp
        buffer.updateWatermark(0);
        final RowData row1 = createRow(100, "First");
        final RowData row2 = createRow(100, "Second");
        final RowData row3 = createRow(100, "Third");

        buffer.processElement(row1);
        buffer.processElement(row2);
        buffer.processElement(row3);

        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        // Verify: Emitted in insertion order
        assertThat(emittedRows).hasSize(3);
        assertThat(emittedRows.get(0)).isEqualTo(row1);
        assertThat(emittedRows.get(1)).isEqualTo(row2);
        assertThat(emittedRows.get(2)).isEqualTo(row3);
    }

    @Test
    void testTimerReregistrationForOutstandingData() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Set initial watermark, process data, then advance watermark
        buffer.updateWatermark(50);
        buffer.processElement(createRow(90, "A"));
        buffer.processElement(createRow(150, "B"));

        // Verify data is buffered
        assertThat(backingMap).containsKey(90L);
        assertThat(backingMap).containsKey(150L);

        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        // Verify: Row(90) emitted, row(150) remains
        assertThat(emittedRows).hasSize(1);
        assertThat(emittedRows.get(0).getLong(TIME_COLUMN)).isEqualTo(90);
        assertThat(backingMap).containsKey(150L);
        assertThat(backingMap).doesNotContainKey(90L); // Removed after emission
        // Timer registered twice initially at 51 (once for each processElement), then at 101 after
        // processing
        verify(timerService, times(2)).registerEventTimeTimer(eq(VoidNamespace.INSTANCE), eq(51L));
        verify(timerService).registerEventTimeTimer(eq(VoidNamespace.INSTANCE), eq(101L));
    }

    @Test
    void testNoTimerReregistrationWhenComplete() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: All data within watermark
        buffer.processElement(createRow(90, "A"));
        buffer.processElement(createRow(95, "B"));
        buffer.updateWatermark(100);

        buffer.onEventTime(createTimer(100));
        registeredTimers.clear(); // Clear initial registration

        // Verify: Both rows emitted, no re-registration
        assertThat(emittedRows).hasSize(2);
        assertThat(backingMap).isEmpty();
        assertThat(registeredTimers).isEmpty();
    }

    @Test
    void testEmptyStateOnTimerFire() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Fire timer with empty state
        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        // Verify: No crash, no emissions, no re-registration
        assertThat(emittedRows).isEmpty();
        assertThat(registeredTimers).doesNotContain(101L);
    }

    @Test
    void testMultipleRowsAtSameTimestamp() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: 5 rows at same timestamp
        buffer.updateWatermark(0);
        for (int i = 0; i < 5; i++) {
            buffer.processElement(createRow(100, "Row" + i));
        }
        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        // Verify: All 5 rows emitted
        assertThat(emittedRows).hasSize(5);
        assertThat(backingMap).isEmpty();
    }

    @Test
    void testMultipleTimestampsInSingleBatch() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Multiple timestamps below watermark
        buffer.updateWatermark(0);
        buffer.processElement(createRow(90, "A"));
        buffer.processElement(createRow(100, "B"));
        buffer.processElement(createRow(120, "C"));
        buffer.updateWatermark(150);
        buffer.onEventTime(createTimer(150));

        // Verify: All emitted in one call
        assertThat(emittedRows).hasSize(3);
        assertThat(emittedRows.get(0).getLong(TIME_COLUMN)).isEqualTo(90);
        assertThat(emittedRows.get(1).getLong(TIME_COLUMN)).isEqualTo(100);
        assertThat(emittedRows.get(2).getLong(TIME_COLUMN)).isEqualTo(120);
    }

    @Test
    void testIdempotentTimerRegistration() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Process multiple rows (each triggers registration)
        buffer.updateWatermark(50);
        buffer.processElement(createRow(100, "A"));
        buffer.processElement(createRow(101, "B"));
        buffer.processElement(createRow(102, "C"));

        // Verify: Timer registered 3 times (InternalTimerService deduplicates)
        verify(timerService, times(3)).registerEventTimeTimer(eq(VoidNamespace.INSTANCE), eq(51L));
    }

    @Test
    void testWatermarkUpdateWithoutElements() {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Only watermark updates
        buffer.updateWatermark(100);
        buffer.updateWatermark(200);

        // Verify: No state changes, no timers
        assertThat(backingMap).isEmpty();
        assertThat(registeredTimers).isEmpty();
    }

    @Test
    void testLateEventAfterWatermarkAdvance() throws Exception {
        // Setup
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Process, fire timer, then same timestamp again
        buffer.updateWatermark(0);
        buffer.processElement(createRow(100, "First"));

        buffer.updateWatermark(100);
        buffer.onEventTime(createTimer(100));

        emittedRows.clear();
        buffer.processElement(createRow(100, "Late"));

        // Verify: Second row dropped
        assertThat(emittedRows).isEmpty();
        assertThat(backingMap).doesNotContainKey(100L);
    }

    @Test
    void testInitialWatermarkState() throws Exception {
        // Setup: Initial state (triggeringWatermark = Long.MIN_VALUE)
        final InputSortBuffer buffer = createBuffer(null, "rocksdb");

        // Execute: Process row with time=0
        buffer.processElement(createRow(0, "Zero"));

        // Verify: Row accepted (0 > Long.MIN_VALUE)
        assertThat(backingMap).containsKey(0L);
        assertThat(registeredTimers).contains(Long.MIN_VALUE + 1);
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private InputSortBuffer createBuffer(RecordComparator comparator, String backendType) {
        final KeyedStateStore stateStore = mock(KeyedStateStore.class);
        when(stateStore.getMapState(any(MapStateDescriptor.class))).thenReturn(mapState);

        final KeyedStateBackend<RowData> backend = mock(KeyedStateBackend.class);
        when(backend.getBackendTypeIdentifier()).thenReturn(backendType);

        // Row type: (BIGINT timestamp, VARCHAR id)
        final RowType inputRowType = RowType.of(new BigIntType(), VarCharType.STRING_TYPE);

        final InputSortBuffer buffer =
                new InputSortBuffer(
                        0, // inputIdx
                        inputRowType,
                        TIME_COLUMN,
                        comparator,
                        stateStore,
                        rowConsumer,
                        StateTtlConfig.DISABLED,
                        backend);

        buffer.setTimerService(timerService);
        return buffer;
    }

    private RowData createRow(long timestamp, String id) {
        final GenericRowData row = new GenericRowData(2);
        row.setField(0, timestamp);
        row.setField(1, StringData.fromString(id));
        return row;
    }

    private InternalTimer<RowData, VoidNamespace> createTimer(long timestamp) {
        @SuppressWarnings("unchecked")
        final InternalTimer<RowData, VoidNamespace> timer = mock(InternalTimer.class);
        when(timer.getTimestamp()).thenReturn(timestamp);
        when(timer.getNamespace()).thenReturn(VoidNamespace.INSTANCE);
        return timer;
    }

    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    private MapState<Long, List<RowData>> createMockMapState() {
        final MapState<Long, List<RowData>> state = mock(MapState.class);

        try {
            when(state.get(anyLong()))
                    .thenAnswer(invocation -> backingMap.get(invocation.getArgument(0)));

            doAnswer(
                            invocation -> {
                                backingMap.put(
                                        invocation.getArgument(0), invocation.getArgument(1));
                                return null;
                            })
                    .when(state)
                    .put(anyLong(), any());

            when(state.keys()).thenAnswer(invocation -> backingMap.keySet());
            when(state.entries()).thenAnswer(invocation -> backingMap.entrySet());

            doAnswer(
                            invocation -> {
                                backingMap.remove(invocation.getArgument(0));
                                return null;
                            })
                    .when(state)
                    .remove(anyLong());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return state;
    }

    @SuppressWarnings("unchecked")
    private InternalTimerService<VoidNamespace> createMockTimerService() {
        final InternalTimerService<VoidNamespace> buffer = mock(InternalTimerService.class);
        doAnswer(
                        invocation -> {
                            registeredTimers.add(invocation.getArgument(1));
                            return null;
                        })
                .when(buffer)
                .registerEventTimeTimer(any(), anyLong());

        return buffer;
    }

    private RecordComparator createStringComparator() {
        return (row1, row2) -> {
            final String s1 = row1.getString(1).toString();
            final String s2 = row2.getString(1).toString();
            return s1.compareTo(s2);
        };
    }
}
