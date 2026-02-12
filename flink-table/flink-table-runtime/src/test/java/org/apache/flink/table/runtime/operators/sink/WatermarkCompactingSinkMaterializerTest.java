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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.delete;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insert;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfter;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WatermarkCompactingSinkMaterializer}. */
@ExtendWith(ParameterizedTestExtension.class)
class WatermarkCompactingSinkMaterializerTest {

    @Parameter ConflictBehavior behavior;

    @Parameter(1)
    SinkUpsertMaterializerStateBackend stateBackend;

    @Parameter(2)
    @Nullable
    Long initialWatermark;

    @Parameters(name = "behavior={0}, stateBackend={1}, initialWatermark={2}")
    public static Collection<Object[]> generateTestParameters() {
        List<Object[]> result = new ArrayList<>();
        for (ConflictBehavior behavior :
                new ConflictBehavior[] {ConflictBehavior.ERROR, ConflictBehavior.NOTHING}) {
            for (SinkUpsertMaterializerStateBackend backend :
                    SinkUpsertMaterializerStateBackend.values()) {
                for (Long initialWatermark : new Long[] {null, 50L}) {
                    result.add(new Object[] {behavior, backend, initialWatermark});
                }
            }
        }
        return result;
    }

    private static final int PRIMARY_KEY_INDEX = 1;
    private static final String PRIMARY_KEY_NAME = "pk";

    private static final LogicalType[] LOGICAL_TYPES =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};

    private static final GeneratedRecordEqualiser RECORD_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {
                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    private static final GeneratedRecordEqualiser UPSERT_KEY_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {
                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestUpsertKeyEqualiser();
                }
            };

    @TestTemplate
    void testBasicInsertWithWatermarkProgression() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert first record (watermark is MIN_VALUE)
            processElement(harness, insertRecord(1L, 1, "a1"));
            assertEmitsNothing(harness); // Buffered, waiting for watermark

            // Advance watermark to trigger compaction
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Update with same upsert key (this is the expected pattern for single-source updates)
            processElement(harness, updateAfterRecord(1L, 1, "a2"));
            assertEmitsNothing(harness);

            // Advance watermark again
            harness.processWatermark(200L);
            assertEmits(harness, updateAfter(1L, 1, "a2"));
        }
    }

    @TestTemplate
    void testDeleteAfterInsert() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert and compact
            processElement(harness, insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Delete and compact
            processElement(harness, deleteRecord(1L, 1, "a1"));
            harness.processWatermark(200L);
            assertEmits(harness, delete(1L, 1, "a1"));
        }
    }

    @TestTemplate
    void testInsertAndDeleteInSameWindow() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert and delete before watermark advances
            processElement(harness, insertRecord(1L, 1, "a1"));
            processElement(harness, deleteRecord(1L, 1, "a1"));

            // Compact - should emit nothing since insert and delete cancel out
            harness.processWatermark(100L);
            assertEmitsNothing(harness);
        }
    }

    @TestTemplate
    void testDoNothingKeepsFirstRecord() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert two records with different upsert keys but same primary key
            processElement(harness, insertRecord(1L, 1, "first"));
            processElement(harness, insertRecord(2L, 1, "second"));

            // Compact - should keep the first record
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "first"));
        }
    }

    @TestTemplate
    void testDoErrorThrowsOnConflict() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.ERROR);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert two records with different upsert keys but same primary key
            processElement(harness, insertRecord(1L, 1, "first"));
            processElement(harness, insertRecord(2L, 1, "second"));

            // Compact - should throw exception with key info
            assertThatThrownBy(() -> harness.processWatermark(100L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @TestTemplate
    void testDoErrorAllowsSameUpsertKey() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.ERROR);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert two records with same upsert key (updates to same source)
            processElement(harness, insertRecord(1L, 1, "v1"));
            processElement(harness, updateAfterRecord(1L, 1, "v2"));

            // Compact - should not throw, just keep the latest
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "v2"));
        }
    }

    @TestTemplate
    void testChangelogDisorderHandling() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Simulate changelog disorder from FLIP-558 example:
            // Records from different sources (different upsert keys: 1L and 2L) map to same PK (1)
            // Ideal order: +I(1,1,a1), -U(1,1,a1), +U(2,1,b1)
            // Disordered: +U(2,1,b1), +I(1,1,a1), -U(1,1,a1)

            // The +U from source 2 arrives first (upsert key = 2L)
            processElement(harness, updateAfterRecord(2L, 1, "b1"));
            // Then +I and -U from source 1 arrive (upsert key = 1L)
            processElement(harness, insertRecord(1L, 1, "a1"));
            processElement(harness, updateBeforeRecord(1L, 1, "a1"));

            // Net result: only (2L, 1, "b1") remains after cancellation, no conflict
            harness.processWatermark(100L);
            assertEmits(harness, insert(2L, 1, "b1"));
        }
    }

    @TestTemplate
    void testChangelogDisorderUpdateBeforeAfterInsert() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Simulate changelog disorder where UPDATE_AFTER arrives before UPDATE_BEFORE:
            // Ideal order: +I(1,1,a1), -U(1,1,a1), +U(1,1,a2)
            // Disordered: +I(1,1,a1), +U(1,1,a2), -U(1,1,a1)

            // INSERT the initial value
            processElement(harness, insertRecord(1L, 1, "a1"));
            // UPDATE_AFTER arrives before the UPDATE_BEFORE
            processElement(harness, updateAfterRecord(1L, 1, "a2"));
            // UPDATE_BEFORE (retraction of the original INSERT) arrives last
            processElement(harness, updateBeforeRecord(1L, 1, "a1"));

            // Net result: +I and -U cancel out, only +U(1,1,a2) remains
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a2"));
        }
    }

    @TestTemplate
    void testNoEmissionWhenValueUnchanged() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            openHarness(harness);

            // Insert and compact
            processElement(harness, insertRecord(1L, 1, "value"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "value"));

            // Insert same value again (same upsert key)
            processElement(harness, updateAfterRecord(1L, 1, "value"));
            harness.processWatermark(200L);
            // Should not emit since value is the same
            assertEmitsNothing(harness);
        }
    }

    /**
     * Tests that record timestamps are handled correctly when multiple inputs send records that
     * arrive out of timestamp order. This simulates the case where records from different upstream
     * tasks have different timestamps and arrive interleaved.
     *
     * <p>Input 1 uses upsert key = 1L, Input 2 uses upsert key = 2L. All records have same primary
     * key (1).
     *
     * <p>Sequence:
     *
     * <ol>
     *   <li>INSERT(input=1, t=2)
     *   <li>watermark=3 -> emits INSERT
     *   <li>UPDATE_BEFORE(input=1, t=4)
     *   <li>UPDATE_AFTER(input=1, t=6)
     *   <li>UPDATE_AFTER(input=2, t=4) - arrives after t=6 record but has smaller timestamp
     *   <li>watermark=5 -> compacts t<=5 records
     *   <li>UPDATE_BEFORE(input=2, t=6)
     *   <li>watermark=10 -> compacts remaining t=6 records
     * </ol>
     */
    @TestTemplate
    void testTwoUpstreamTasksWithDisorderedWatermarks() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            // INSERT from input 1 with timestamp 2
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 2L));
            assertEmitsNothing(harness);

            // watermark=3: compacts records with t<=3, emits INSERT(t=2)
            harness.processWatermark(3L);
            assertEmits(harness, insert(1L, 1, "v1"));

            // UPDATE_BEFORE from input 1 with timestamp 4
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_BEFORE, 1L, 1, "v1", 4L));
            assertEmitsNothing(harness);

            // UPDATE_AFTER from input 1 with timestamp 6
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v4", 6L));
            assertEmitsNothing(harness);

            // UPDATE_AFTER from input 2 with timestamp 4
            // This record arrives after the t=6 record but has a smaller timestamp
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 2L, 1, "v3", 4L));

            // watermark=5: compacts records with t<=5
            // Buffered: UPDATE_BEFORE(1L, t=4) and UPDATE_AFTER(2L, t=4) cancel out for input 1,
            // UPDATE_AFTER(2L, t=4) is emitted
            harness.processWatermark(5L);
            assertEmits(harness, updateAfter(2L, 1, "v3"));

            // UPDATE_BEFORE from input 2 with timestamp 6
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_BEFORE, 2L, 1, "v3", 6L));
            assertEmitsNothing(harness);

            // Final watermark to flush all remaining buffered records (t=6)
            // Buffered: UPDATE_AFTER(1L, t=6) and UPDATE_BEFORE(2L, t=6)
            // After compaction: UPDATE_AFTER(1L, "v4") remains as final value
            harness.processWatermark(10L);
            assertEmits(harness, updateAfter(1L, 1, "v4"));
        }
    }

    // --- Tests without upsert key ---

    @TestTemplate
    void testBasicInsertWithoutUpsertKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert first record
            processElement(harness, insertRecord(1L, 1, "a1"));
            assertEmitsNothing(harness);

            // Advance watermark to trigger compaction
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));
        }
    }

    @TestTemplate
    void testUpdateWithoutUpsertKeyNothingStrategy() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        // Without upsert key, UPDATE_AFTER on existing value causes conflict (two rows accumulate)
        // NOTHING strategy keeps the first value
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            processElement(harness, insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // UPDATE_AFTER with different value - creates conflict, NOTHING keeps first
            processElement(harness, updateAfterRecord(2L, 1, "a2"));
            harness.processWatermark(200L);
            // NOTHING keeps the previous value (1L, 1, "a1"), no emission since unchanged
            assertEmitsNothing(harness);
        }
    }

    @TestTemplate
    void testUpdateWithoutUpsertKeyErrorStrategy() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.ERROR);
        // Without upsert key, UPDATE_AFTER on existing value causes conflict (two rows accumulate)
        // ERROR strategy throws
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            processElement(harness, insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // UPDATE_AFTER with different value - creates conflict, ERROR throws with key info
            processElement(harness, updateAfterRecord(2L, 1, "a2"));
            assertThatThrownBy(() -> harness.processWatermark(200L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @TestTemplate
    void testDeleteAfterInsertWithoutUpsertKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert and compact
            processElement(harness, insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Delete with exact same row and compact
            processElement(harness, deleteRecord(1L, 1, "a1"));
            harness.processWatermark(200L);
            assertEmits(harness, delete(1L, 1, "a1"));
        }
    }

    @TestTemplate
    void testInsertAndDeleteInSameWindowWithoutUpsertKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert and delete with exact same row before watermark advances
            processElement(harness, insertRecord(1L, 1, "a1"));
            processElement(harness, deleteRecord(1L, 1, "a1"));

            // Compact - should emit nothing since insert and delete cancel out
            harness.processWatermark(100L);
            assertEmitsNothing(harness);
        }
    }

    @TestTemplate
    void testIdenticalInsertsWithoutUpsertKeyNothingKeepsFirst() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        // Without upsert key, even identical inserts are separate entries
        // NOTHING strategy just keeps the first one
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert two identical records (same full row)
            processElement(harness, insertRecord(1L, 1, "same"));
            processElement(harness, insertRecord(1L, 1, "same"));

            // Compact - NOTHING keeps first record
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "same"));
        }
    }

    @TestTemplate
    void testIdenticalInsertsWithoutUpsertKeyErrorThrows() throws Exception {
        Assumptions.assumeTrue(behavior == ConflictBehavior.ERROR);
        // Without upsert key, even identical inserts are separate entries
        // ERROR strategy throws because there are multiple records
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert two identical records (same full row)
            processElement(harness, insertRecord(1L, 1, "same"));
            processElement(harness, insertRecord(1L, 1, "same"));

            // Compact - ERROR throws because there are multiple pending records, includes key info
            assertThatThrownBy(() -> harness.processWatermark(100L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @TestTemplate
    void testInsertUpdateDeleteWithoutUpsertKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior, stateBackend)) {
            openHarness(harness);

            // Insert, then update_before + update_after sequence
            processElement(harness, insertRecord(1L, 1, "v1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "v1"));

            // Update: retract old value, insert new value
            processElement(harness, updateBeforeRecord(1L, 1, "v1"));
            processElement(harness, updateAfterRecord(2L, 1, "v2"));
            harness.processWatermark(200L);
            assertEmits(harness, updateAfter(2L, 1, "v2"));

            // Delete the current value
            processElement(harness, deleteRecord(2L, 1, "v2"));
            harness.processWatermark(300L);
            assertEmits(harness, delete(2L, 1, "v2"));
        }
    }

    // --- Restore Tests ---

    /**
     * Tests that buffered records at different timestamps before checkpoint are consolidated to
     * MIN_VALUE on restore and compacted on the first watermark.
     */
    @TestTemplate
    void testBufferedRecordsConsolidatedOnRestore() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        OperatorSubtaskState snapshot;

        // First harness: buffer records at different timestamps, then take snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            // Buffer records at different timestamps (simulating records before checkpoint)
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 1000L));
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v2", 2000L));
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v3", 3000L));

            // No watermark yet, so nothing emitted
            assertEmitsNothing(harness);

            // Take snapshot with buffered records
            snapshot = harness.snapshot(1L, 1L);
        }

        // Second harness: restore from snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(snapshot);
            harness.open();

            // After restore, watermarks restart from MIN_VALUE.
            // The buffered records should have been consolidated to MIN_VALUE.
            // First watermark (even a small one) should trigger compaction of all consolidated
            // records.
            harness.processWatermark(100L);

            // All records were from same upsert key, so only final value is emitted
            assertEmits(harness, insert(1L, 1, "v3"));
        }
    }

    /**
     * Tests that in-flight records from unaligned checkpoints (records with timestamps > MIN_VALUE
     * arriving before first watermark after restore) are correctly handled.
     */
    @TestTemplate
    void testInFlightRecordsAfterRestore() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        OperatorSubtaskState snapshot;

        // First harness: take empty snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();
            snapshot = harness.snapshot(1L, 1L);
        }

        // Second harness: restore and simulate in-flight records
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(snapshot);
            harness.open();

            // Simulate in-flight records that were checkpointed with their old timestamps.
            // These arrive after restore but before any watermark is received.
            // They have timestamps > MIN_VALUE from before the checkpoint.
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 5000L));
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v2", 5100L));

            // No watermark yet, nothing emitted
            assertEmitsNothing(harness);

            // First watermark after restore. Since currentWatermark was MIN_VALUE,
            // in-flight records should have been assigned to MIN_VALUE and will be compacted.
            harness.processWatermark(100L);

            // Both records had same upsert key, so only final value is emitted
            assertEmits(harness, insert(1L, 1, "v2"));
        }
    }

    /**
     * Tests restore with multiple keys having buffered records at different timestamps. Verifies
     * that each key's records are correctly consolidated and compacted.
     */
    @TestTemplate
    void testRestoreWithMultipleKeys() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        OperatorSubtaskState snapshot;

        // First harness: buffer records for multiple keys
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            // Key 1: multiple updates
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "k1v1", 1000L));
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "k1v2", 2000L));

            // Key 2: single insert
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 2L, 2, "k2v1", 1500L));

            // Key 3: insert then delete (should result in nothing)
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 3L, 3, "k3v1", 1000L));
            harness.processElement(recordWithTimestamp(RowKind.DELETE, 3L, 3, "k3v1", 2500L));

            snapshot = harness.snapshot(1L, 1L);
        }

        // Second harness: restore and verify
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(snapshot);
            harness.open();

            // First watermark compacts all consolidated records
            harness.processWatermark(100L);

            // Extract and verify results (order depends on key processing order)
            List<RowData> emitted = extractRecords(harness);
            assertThat(emitted).hasSize(2);
            // Key 1 should have final value "k1v2", Key 2 should have "k2v1", Key 3 cancelled out
            assertThat(emitted)
                    .anySatisfy(
                            row -> {
                                assertThat(row.getInt(1)).isEqualTo(1);
                                assertThat(row.getString(2).toString()).isEqualTo("k1v2");
                            })
                    .anySatisfy(
                            row -> {
                                assertThat(row.getInt(1)).isEqualTo(2);
                                assertThat(row.getString(2).toString()).isEqualTo("k2v1");
                            });
        }
    }

    /**
     * Tests that consolidation on restore compacts matching +I/-D pairs, so a new INSERT arriving
     * post-restore is not erroneously cancelled against a stale DELETE from the consolidated
     * buffer.
     */
    @TestTemplate
    void testConsolidationCompactsMatchingRecordsOnRestore() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        OperatorSubtaskState snapshot;

        // First harness: buffer +I and -D at different timestamps, then take snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            harness.processWatermark(3L);
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 3L));
            harness.processElement(recordWithTimestamp(RowKind.DELETE, 1L, 1, "v1", 4L));
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v2", 5L));

            assertEmitsNothing(harness);

            snapshot = harness.snapshot(1L, 1L);
        }

        // Second harness: restore, send a new INSERT, and verify it is emitted
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(snapshot);
            harness.open();

            // New INSERT with a different value to distinguish from the old one
            harness.processElement(
                    recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", Long.MIN_VALUE));

            harness.processWatermark(100L);

            // The consolidated +I/-D pair should have cancelled out on restore,
            // so only the new INSERT("v2") is emitted
            assertEmits(harness, insert(1L, 1, "v1"));
        }
    }

    // --- Timer Registration Tests ---

    /**
     * Tests that a record arriving with a timestamp equal to an already-processed watermark is
     * correctly buffered and included in the next compaction.
     *
     * <p>This verifies the timer registration at timestamp+1 behavior. If timers were registered at
     * exactly the watermark timestamp, a record arriving with that same timestamp would be missed
     * (the timer would have already fired). By registering timers at timestamp+1, we ensure all
     * records with timestamp T are processed before the timer fires when watermark reaches T+1.
     *
     * <p>Sequence:
     *
     * <ol>
     *   <li>Process record with timestamp T=99 → timer registered at 100
     *   <li>Process watermark T=100 → timer at 100 fires → compactAndEmit(99) compacts records ≤99
     *   <li>First record is emitted as INSERT
     *   <li>Process record with timestamp T=100 (equal to the watermark) → timer registered at 101
     *   <li>This record should be buffered, NOT missed
     *   <li>Process watermark T=200 → timer at 101 fires → compactAndEmit(100) compacts records
     *       ≤100
     *   <li>Second record should be emitted as UPDATE_AFTER
     * </ol>
     */
    @TestTemplate
    void testRecordWithTimestampEqualToWatermarkIsNotMissed() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            // Process record with timestamp 99 → timer registered at 100
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 99L));
            assertEmitsNothing(harness);

            // Process watermark 100 → timer at 100 fires → compacts records with timestamp ≤99
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "v1"));

            // Process record with timestamp 100 (equal to the watermark we just processed)
            // This should be buffered, not missed. Timer registered at 101.
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v2", 100L));
            assertEmitsNothing(harness);

            // Process watermark 200 → timer at 101 fires → compacts records with timestamp ≤100
            harness.processWatermark(200L);
            assertEmits(harness, updateAfter(1L, 1, "v2"));
        }
    }

    // --- Checkpoint ID Consolidation Tests ---

    /**
     * Tests that consolidation happens on first restore when storedId is null. After taking a
     * checkpoint with buffered records, restoring should consolidate the records.
     */
    @TestTemplate
    void testConsolidationOnFirstRestore() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        OperatorSubtaskState checkpoint1;

        // First run: buffer records and take checkpoint
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            // Buffer records at different timestamps
            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 1000L));
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v2", 2000L));

            assertEmitsNothing(harness);

            // Take checkpoint 1 - no consolidation marker stored yet
            checkpoint1 = harness.snapshot(1L, 1L);
        }

        // First restore from checkpoint 1: storedId is null, should consolidate
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(checkpoint1);
            harness.open();

            // First watermark triggers consolidation and compaction
            harness.processWatermark(100L);

            // Records consolidated and compacted - final value emitted
            assertEmits(harness, insert(1L, 1, "v2"));
        }
    }

    /**
     * Tests that restoring from a different checkpoint triggers re-consolidation. After restoring
     * from C1 and consolidating, if we take C2 and then restore from C2, the storedId (from C1
     * consolidation) won't match restoredId (C2), so we re-consolidate.
     */
    @TestTemplate
    void testConsolidationOnRestoreFromDifferentCheckpoint() throws Exception {
        Assumptions.assumeTrue(initialWatermark == null);
        Assumptions.assumeTrue(behavior == ConflictBehavior.NOTHING);
        OperatorSubtaskState checkpoint1;
        OperatorSubtaskState checkpoint2;

        // First run: buffer records and take checkpoint 1
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.open();

            harness.processElement(recordWithTimestamp(RowKind.INSERT, 1L, 1, "v1", 1000L));
            checkpoint1 = harness.snapshot(1L, 1L);
        }

        // First restore from C1, consolidate, then take checkpoint 2
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(checkpoint1);
            harness.open();

            // Trigger consolidation by processing a record for the key
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v2", 100L));
            harness.processWatermark(200L);

            // v1 and v2 compacted together
            assertEmits(harness, insert(1L, 1, "v2"));

            // Add more data and take checkpoint 2
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v3", 300L));
            checkpoint2 = harness.snapshot(2L, 2L);
        }

        // Restore from checkpoint 2: storedId=1 (from C1 consolidation), restoredId=2
        // Since they differ, should consolidate again
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior, stateBackend)) {
            harness.initializeState(checkpoint2);
            harness.open();

            // Add new record to trigger consolidation check for the key
            // The v3 record was buffered at timestamp 300, which gets consolidated to MIN_VALUE
            harness.processElement(recordWithTimestamp(RowKind.UPDATE_AFTER, 1L, 1, "v4", 50L));
            harness.processWatermark(100L);

            // v3 and v4 consolidated together, v4 wins as it was added after
            assertEmits(harness, updateAfter(1L, 1, "v4"));
        }
    }

    // --- Helper Methods ---

    private void openHarness(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness)
            throws Exception {
        harness.open();
        if (initialWatermark != null) {
            harness.processWatermark(initialWatermark);
        }
    }

    private void processElement(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
            StreamRecord<RowData> record)
            throws Exception {
        if (initialWatermark != null && !record.hasTimestamp()) {
            harness.processElement(new StreamRecord<>(record.getValue(), initialWatermark + 1));
        } else {
            harness.processElement(record);
        }
    }

    private StreamRecord<RowData> recordWithTimestamp(
            RowKind kind, long upsertKey, int pk, String value, long timestamp) {
        return new StreamRecord<>(rowOfKind(kind, upsertKey, pk, value), timestamp);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            ConflictBehavior behavior, SinkUpsertMaterializerStateBackend stateBackend)
            throws Exception {
        RowType keyType = RowType.of(new LogicalType[] {LOGICAL_TYPES[PRIMARY_KEY_INDEX]});
        WatermarkCompactingSinkMaterializer operator =
                WatermarkCompactingSinkMaterializer.create(
                        StateTtlConfig.DISABLED,
                        toStrategy(behavior),
                        RowType.of(LOGICAL_TYPES),
                        RECORD_EQUALISER,
                        UPSERT_KEY_EQUALISER,
                        new int[] {0}, // upsert key is first column
                        keyType,
                        new String[] {PRIMARY_KEY_NAME});

        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                        new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES)
                                .getProducedType());
        testHarness.setStateBackend(stateBackend.create(false));
        return testHarness;
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>
            createHarnessWithoutUpsertKey(
                    ConflictBehavior behavior, SinkUpsertMaterializerStateBackend stateBackend)
                    throws Exception {
        RowType keyType = RowType.of(new LogicalType[] {LOGICAL_TYPES[PRIMARY_KEY_INDEX]});
        WatermarkCompactingSinkMaterializer operator =
                WatermarkCompactingSinkMaterializer.create(
                        StateTtlConfig.DISABLED,
                        toStrategy(behavior),
                        RowType.of(LOGICAL_TYPES),
                        RECORD_EQUALISER,
                        null, // no upsert key equaliser
                        null, // no upsert key
                        keyType,
                        new String[] {PRIMARY_KEY_NAME});

        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                        new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES)
                                .getProducedType());
        testHarness.setStateBackend(stateBackend.create(false));
        return testHarness;
    }

    private static InsertConflictStrategy toStrategy(ConflictBehavior behavior) {
        switch (behavior) {
            case ERROR:
                return InsertConflictStrategy.error();
            case NOTHING:
                return InsertConflictStrategy.nothing();
            case DEDUPLICATE:
                return InsertConflictStrategy.deduplicate();
            default:
                throw new IllegalArgumentException("Unknown behavior: " + behavior);
        }
    }

    private void assertEmitsNothing(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        assertThat(extractRecords(harness)).isEmpty();
    }

    private void assertEmits(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
            RowData... expected) {
        List<RowData> emitted = extractRecords(harness);
        assertThat(emitted).containsExactly(expected);
    }

    private List<RowData> extractRecords(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[LOGICAL_TYPES.length];
        for (int i = 0; i < LOGICAL_TYPES.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(LOGICAL_TYPES[i], i);
        }

        final List<RowData> rows = new ArrayList<>();
        Object o;
        while ((o = harness.getOutput().poll()) != null) {
            // Skip watermarks, only process StreamRecords
            if (o instanceof StreamRecord) {
                RowData value = (RowData) ((StreamRecord<?>) o).getValue();
                Object[] row = new Object[LOGICAL_TYPES.length];
                for (int i = 0; i < LOGICAL_TYPES.length; i++) {
                    row[i] = fieldGetters[i].getFieldOrNull(value);
                }
                GenericRowData newRow = GenericRowData.of(row);
                newRow.setRowKind(value.getRowKind());
                rows.add(newRow);
            }
        }
        return rows;
    }

    /** Test equaliser that compares all fields. */
    private static class TestRecordEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(0) == row2.getLong(0)
                    && row1.getInt(1) == row2.getInt(1)
                    && Objects.equals(row1.getString(2), row2.getString(2));
        }
    }

    /** Test equaliser that only compares the upsert key (first column). */
    private static class TestUpsertKeyEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getLong(0) == row2.getLong(0);
        }
    }
}
