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
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
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
class WatermarkCompactingSinkMaterializerTest {

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

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testBasicInsertWithWatermarkProgression(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Insert first record (watermark is MIN_VALUE)
            harness.processElement(insertRecord(1L, 1, "a1"));
            assertEmitsNothing(harness); // Buffered, waiting for watermark

            // Advance watermark to trigger compaction
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Update with same upsert key (this is the expected pattern for single-source updates)
            harness.processElement(updateAfterRecord(1L, 1, "a2"));
            assertEmitsNothing(harness);

            // Advance watermark again
            harness.processWatermark(200L);
            assertEmits(harness, updateAfter(1L, 1, "a2"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testDeleteAfterInsert(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Insert and compact
            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Delete and compact
            harness.processElement(deleteRecord(1L, 1, "a1"));
            harness.processWatermark(200L);
            assertEmits(harness, delete(1L, 1, "a1"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testInsertAndDeleteInSameWindow(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Insert and delete before watermark advances
            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processElement(deleteRecord(1L, 1, "a1"));

            // Compact - should emit nothing since insert and delete cancel out
            harness.processWatermark(100L);
            assertEmitsNothing(harness);
        }
    }

    @Test
    void testDoNothingKeepsFirstRecord() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(ConflictBehavior.NOTHING)) {
            harness.open();

            // Insert two records with different upsert keys but same primary key
            harness.processElement(insertRecord(1L, 1, "first"));
            harness.processElement(insertRecord(2L, 1, "second"));

            // Compact - should keep the first record
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "first"));
        }
    }

    @Test
    void testDoErrorThrowsOnConflict() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(ConflictBehavior.ERROR)) {
            harness.open();

            // Insert two records with different upsert keys but same primary key
            harness.processElement(insertRecord(1L, 1, "first"));
            harness.processElement(insertRecord(2L, 1, "second"));

            // Compact - should throw exception with key info
            assertThatThrownBy(() -> harness.processWatermark(100L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @Test
    void testDoErrorAllowsSameUpsertKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(ConflictBehavior.ERROR)) {
            harness.open();

            // Insert two records with same upsert key (updates to same source)
            harness.processElement(insertRecord(1L, 1, "v1"));
            harness.processElement(updateAfterRecord(1L, 1, "v2"));

            // Compact - should not throw, just keep the latest
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "v2"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testChangelogDisorderHandling(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Simulate changelog disorder from FLIP-558 example:
            // Records from different sources (different upsert keys: 1L and 2L) map to same PK (1)
            // Ideal order: +I(1,1,a1), -U(1,1,a1), +U(2,1,b1)
            // Disordered: +U(2,1,b1), +I(1,1,a1), -U(1,1,a1)

            // The +U from source 2 arrives first (upsert key = 2L)
            harness.processElement(updateAfterRecord(2L, 1, "b1"));
            // Then +I and -U from source 1 arrive (upsert key = 1L)
            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processElement(updateBeforeRecord(1L, 1, "a1"));

            // Net result: only (2L, 1, "b1") remains after cancellation, no conflict
            harness.processWatermark(100L);
            assertEmits(harness, insert(2L, 1, "b1"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testChangelogDisorderUpdateBeforeAfterInsert(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Simulate changelog disorder where UPDATE_AFTER arrives before UPDATE_BEFORE:
            // Ideal order: +I(1,1,a1), -U(1,1,a1), +U(1,1,a2)
            // Disordered: +I(1,1,a1), +U(1,1,a2), -U(1,1,a1)

            // INSERT the initial value
            harness.processElement(insertRecord(1L, 1, "a1"));
            // UPDATE_AFTER arrives before the UPDATE_BEFORE
            harness.processElement(updateAfterRecord(1L, 1, "a2"));
            // UPDATE_BEFORE (retraction of the original INSERT) arrives last
            harness.processElement(updateBeforeRecord(1L, 1, "a1"));

            // Net result: +I and -U cancel out, only +U(1,1,a2) remains
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a2"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testNoEmissionWhenValueUnchanged(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();

            // Insert and compact
            harness.processElement(insertRecord(1L, 1, "value"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "value"));

            // Insert same value again (same upsert key)
            harness.processElement(updateAfterRecord(1L, 1, "value"));
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
    @Test
    void testTwoUpstreamTasksWithDisorderedWatermarks() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(ConflictBehavior.NOTHING)) {
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

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testBasicInsertWithoutUpsertKey(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior)) {
            harness.open();

            // Insert first record
            harness.processElement(insertRecord(1L, 1, "a1"));
            assertEmitsNothing(harness);

            // Advance watermark to trigger compaction
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));
        }
    }

    @Test
    void testUpdateWithoutUpsertKeyNothingStrategy() throws Exception {
        // Without upsert key, UPDATE_AFTER on existing value causes conflict (two rows accumulate)
        // NOTHING strategy keeps the first value
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(ConflictBehavior.NOTHING)) {
            harness.open();

            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // UPDATE_AFTER with different value - creates conflict, NOTHING keeps first
            harness.processElement(updateAfterRecord(2L, 1, "a2"));
            harness.processWatermark(200L);
            // NOTHING keeps the previous value (1L, 1, "a1"), no emission since unchanged
            assertEmitsNothing(harness);
        }
    }

    @Test
    void testUpdateWithoutUpsertKeyErrorStrategy() throws Exception {
        // Without upsert key, UPDATE_AFTER on existing value causes conflict (two rows accumulate)
        // ERROR strategy throws
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(ConflictBehavior.ERROR)) {
            harness.open();

            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // UPDATE_AFTER with different value - creates conflict, ERROR throws with key info
            harness.processElement(updateAfterRecord(2L, 1, "a2"));
            assertThatThrownBy(() -> harness.processWatermark(200L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testDeleteAfterInsertWithoutUpsertKey(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior)) {
            harness.open();

            // Insert and compact
            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "a1"));

            // Delete with exact same row and compact
            harness.processElement(deleteRecord(1L, 1, "a1"));
            harness.processWatermark(200L);
            assertEmits(harness, delete(1L, 1, "a1"));
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testInsertAndDeleteInSameWindowWithoutUpsertKey(ConflictBehavior behavior)
            throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior)) {
            harness.open();

            // Insert and delete with exact same row before watermark advances
            harness.processElement(insertRecord(1L, 1, "a1"));
            harness.processElement(deleteRecord(1L, 1, "a1"));

            // Compact - should emit nothing since insert and delete cancel out
            harness.processWatermark(100L);
            assertEmitsNothing(harness);
        }
    }

    @Test
    void testIdenticalInsertsWithoutUpsertKeyNothingKeepsFirst() throws Exception {
        // Without upsert key, even identical inserts are separate entries
        // NOTHING strategy just keeps the first one
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(ConflictBehavior.NOTHING)) {
            harness.open();

            // Insert two identical records (same full row)
            harness.processElement(insertRecord(1L, 1, "same"));
            harness.processElement(insertRecord(1L, 1, "same"));

            // Compact - NOTHING keeps first record
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "same"));
        }
    }

    @Test
    void testIdenticalInsertsWithoutUpsertKeyErrorThrows() throws Exception {
        // Without upsert key, even identical inserts are separate entries
        // ERROR strategy throws because there are multiple records
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(ConflictBehavior.ERROR)) {
            harness.open();

            // Insert two identical records (same full row)
            harness.processElement(insertRecord(1L, 1, "same"));
            harness.processElement(insertRecord(1L, 1, "same"));

            // Compact - ERROR throws because there are multiple pending records, includes key info
            assertThatThrownBy(() -> harness.processWatermark(100L))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Primary key constraint violation")
                    .hasMessageContaining("[pk=1]");
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testInsertUpdateDeleteWithoutUpsertKey(ConflictBehavior behavior) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarnessWithoutUpsertKey(behavior)) {
            harness.open();

            // Insert, then update_before + update_after sequence
            harness.processElement(insertRecord(1L, 1, "v1"));
            harness.processWatermark(100L);
            assertEmits(harness, insert(1L, 1, "v1"));

            // Update: retract old value, insert new value
            harness.processElement(updateBeforeRecord(1L, 1, "v1"));
            harness.processElement(updateAfterRecord(2L, 1, "v2"));
            harness.processWatermark(200L);
            assertEmits(harness, updateAfter(2L, 1, "v2"));

            // Delete the current value
            harness.processElement(deleteRecord(2L, 1, "v2"));
            harness.processWatermark(300L);
            assertEmits(harness, delete(2L, 1, "v2"));
        }
    }

    // --- Restore Tests ---

    /**
     * Tests that buffered records at different timestamps before checkpoint are consolidated to
     * MIN_VALUE on restore and compacted on the first watermark.
     */
    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testBufferedRecordsConsolidatedOnRestore(ConflictBehavior behavior) throws Exception {
        OperatorSubtaskState snapshot;

        // First harness: buffer records at different timestamps, then take snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
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
                createHarness(behavior)) {
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
    @ParameterizedTest
    @EnumSource(
            value = ConflictBehavior.class,
            names = {"ERROR", "NOTHING"})
    void testInFlightRecordsAfterRestore(ConflictBehavior behavior) throws Exception {
        OperatorSubtaskState snapshot;

        // First harness: take empty snapshot
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
            harness.open();
            snapshot = harness.snapshot(1L, 1L);
        }

        // Second harness: restore and simulate in-flight records
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(behavior)) {
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
    @Test
    void testRestoreWithMultipleKeys() throws Exception {
        OperatorSubtaskState snapshot;

        // First harness: buffer records for multiple keys
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness =
                createHarness(ConflictBehavior.NOTHING)) {
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
                createHarness(ConflictBehavior.NOTHING)) {
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

    // --- Helper Methods ---

    private StreamRecord<RowData> recordWithTimestamp(
            RowKind kind, long upsertKey, int pk, String value, long timestamp) {
        return new StreamRecord<>(rowOfKind(kind, upsertKey, pk, value), timestamp);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            ConflictBehavior behavior) throws Exception {
        RowType keyType = RowType.of(new LogicalType[] {LOGICAL_TYPES[PRIMARY_KEY_INDEX]});
        WatermarkCompactingSinkMaterializer operator =
                WatermarkCompactingSinkMaterializer.create(
                        toStrategy(behavior),
                        RowType.of(LOGICAL_TYPES),
                        RECORD_EQUALISER,
                        UPSERT_KEY_EQUALISER,
                        new int[] {0}, // upsert key is first column
                        keyType,
                        new String[] {PRIMARY_KEY_NAME});

        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES),
                HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES)
                        .getProducedType());
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>
            createHarnessWithoutUpsertKey(ConflictBehavior behavior) throws Exception {
        RowType keyType = RowType.of(new LogicalType[] {LOGICAL_TYPES[PRIMARY_KEY_INDEX]});
        WatermarkCompactingSinkMaterializer operator =
                WatermarkCompactingSinkMaterializer.create(
                        toStrategy(behavior),
                        RowType.of(LOGICAL_TYPES),
                        RECORD_EQUALISER,
                        null, // no upsert key equaliser
                        null, // no upsert key
                        keyType,
                        new String[] {PRIMARY_KEY_NAME});

        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES),
                HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {PRIMARY_KEY_INDEX}, LOGICAL_TYPES)
                        .getProducedType());
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
