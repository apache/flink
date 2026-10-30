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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.function.Predicate;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StreamingSemiAntiJoinOperator}. */
class StreamingSemiAntiJoinOperatorTest extends StreamingJoinOperatorTestBase {
    @Override
    protected StreamingSemiAntiJoinOperator createJoinOperator(TestInfo testInfo) {
        Long[] ttl = STATE_RETENTION_TIME_EXTRACTOR.apply(testInfo.getTags());
        return new StreamingSemiAntiJoinOperator(
                ANTI_JOIN_CHECKER.test(testInfo.getDisplayName()),
                leftTypeInfo,
                rightTypeInfo,
                joinCondition,
                leftInputSpec,
                rightInputSpec,
                null,
                null,
                false,
                new boolean[] {true},
                ttl[0],
                ttl[1]);
    }

    @Override
    protected RowType getOutputType() {
        return leftTypeInfo.toRowType();
    }

    /**
     * The equivalent SQL as follows.
     *
     * <p>{@code SELECT a.order_id, a.line_order_id, a.shipping_address FROM orders a WHERE
     * a.line_order_id IN (SELECT b.line_order_id FROM line_orders b)}
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=1000")
    @Test
    void testLeftSemiJoinWithDifferentStateRetentionTime() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.setStateTtlProcessingTime(3001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(deleteRecord("LineOrd#2", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        // the left side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "SHIP"));
        assertor.shouldEmitNothing(testHarness);

        // the right side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(5001);
        testHarness.processElement1(
                updateAfterRecord("Ord#1", "LineOrd#1", "7238 Marsh St., Birmingham, AL 35209"));
        assertor.shouldEmitNothing(testHarness);
    }

    /**
     * The equivalent SQL is the same as {@link #testLeftSemiJoinWithDifferentStateRetentionTime()}.
     * The only difference is that the state retention is disabled.
     */
    @Test
    void testLeftSemiJoinWithStateRetentionDisabled() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.setStateTtlProcessingTime(3001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(deleteRecord("LineOrd#2", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        // the right side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(5001);
        testHarness.processElement1(
                updateAfterRecord("Ord#1", "LineOrd#1", "7238 Marsh St., Birmingham, AL 35209"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#1",
                        "7238 Marsh St., Birmingham, AL 35209"));
    }

    /**
     * The equivalent SQL as follows.
     *
     * <p>{@code SELECT a.order_id, a.line_order_id, a.shipping_address FROM orders a WHERE
     * a.line_order_id NOT IN (SELECT b.line_order_id FROM line_orders b)}
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=1000")
    @Test
    void testLeftAntiJoinWithDifferentStateRetentionTime() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.setStateTtlProcessingTime(3001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        // left side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "RAIL"));
        assertor.shouldEmitNothing(testHarness);

        // right side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(5001);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#1", "23 W. River Avenue, Port Orange, FL 32127"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#1",
                        "23 W. River Avenue, Port Orange, FL 32127"));
    }

    /**
     * The equivalent SQL is the same as {@link #testLeftAntiJoinWithDifferentStateRetentionTime()}.
     * The only difference is that the state retention is disabled.
     */
    @Test
    void testLeftAntiJoinWithStateRetentionTimeDisabled() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.setStateTtlProcessingTime(3001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "RAIL"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));

        testHarness.setStateTtlProcessingTime(5001);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#1", "23 W. River Avenue, Port Orange, FL 32127"));
        assertor.shouldEmitNothing(testHarness);
    }

    @Test
    void testLeftSemiJoinSkipsNullKeys() throws Exception {
        assertSkipsNullKeys(false);
    }

    /** An anti join emits and retracts a left row with a null key without storing it. */
    @Test
    void testLeftAntiJoinSkipsNullKeys() throws Exception {
        assertSkipsNullKeys(true);
    }

    private void assertSkipsNullKeys(boolean isAntiJoin) throws Exception {
        String address = "3 Bellevue Drive, Pottstown, PA 19464";
        testHarness.processElement1(insertRecord("Ord#1", null, address));
        if (isAntiJoin) {
            assertor.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, "Ord#1", null, address));
        } else {
            assertor.shouldEmitNothing(testHarness);
        }
        testHarness.processElement2(insertRecord(null, "AIR"));
        assertor.shouldEmitNothing(testHarness);
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        testHarness.processElement1(deleteRecord("Ord#1", null, address));
        if (isAntiJoin) {
            assertor.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, "Ord#1", null, address));
        } else {
            assertor.shouldEmitNothing(testHarness);
        }
        testHarness.processElement2(deleteRecord(null, "AIR"));
        assertor.shouldEmitNothing(testHarness);
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        assertThat(testHarness.numKeyedStateEntries()).isOne();
    }

    /** A right row replaced without UPDATE_BEFORE is counted once, so its DELETE removes it. */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testRightUpsertIsCountedOnce(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightInputSpec, joinCondition, false);
        assertRightUpsertIsCountedOnce(isAntiJoin, "TRUCK");
    }

    /** The same holds if the unique key of the right side contains more than the join key. */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testRightUpsertWithUniqueKeyIsCountedOnce(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightHasUniqueKeySpec(), joinCondition, false);
        assertRightUpsertIsCountedOnce(isAntiJoin, "AIR");
    }

    private void assertRightUpsertIsCountedOnce(boolean isAntiJoin, String newShipMode)
            throws Exception {
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.getOutput().clear();

        testHarness.processElement2(updateAfterRecord("LineOrd#1", newShipMode));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(deleteRecord("LineOrd#1", newShipMode));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#1", ADDRESS)));
    }

    /** A right row replaced without UPDATE_BEFORE that no longer matches removes its match. */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testRightUpsertRemovesLostMatch(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightInputSpec, knownValuesNonEquiCondition(), true);
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.getOutput().clear();

        testHarness.processElement2(updateAfterRecord("LineOrd#1", UNKNOWN));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#1", ADDRESS)));

        testHarness.processElement2(updateAfterRecord("LineOrd#1", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.UPDATE_AFTER, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS)));
    }

    /** A DELETE of only the key of a right row is joined with the stored row. */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testRightKeyOnlyDeleteRemovesMatch(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightInputSpec, knownValuesNonEquiCondition(), true);
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.getOutput().clear();

        testHarness.processElement2(deleteRecord("LineOrd#1", null));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#1", ADDRESS)));
    }

    /**
     * A left row replaced without UPDATE_BEFORE retracts the old version if it was emitted but the
     * new version is not.
     */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testLeftUpsertRetractsOldVersion(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightInputSpec, knownValuesNonEquiCondition(), true);
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#1", ADDRESS),
                        null));

        testHarness.processElement1(updateAfterRecord("Ord#1", "LineOrd#1", UNKNOWN));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.UPDATE_AFTER, "Ord#1", "LineOrd#1", UNKNOWN)));

        testHarness.processElement1(updateAfterRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.UPDATE_AFTER, "Ord#1", "LineOrd#1", NEW_ADDRESS),
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", UNKNOWN)));
    }

    /**
     * A DELETE of only the key of a left row is joined with the stored row, so it is emitted with
     * all fields if the row was emitted, and not at all otherwise.
     */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testLeftKeyOnlyDelete(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(isAntiJoin, rightInputSpec, knownValuesNonEquiCondition(), true);
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.processElement1(insertRecord("Ord#2", "LineOrd#2", ADDRESS));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.INSERT, "Ord#2", "LineOrd#2", ADDRESS)));

        testHarness.processElement1(deleteRecord("Ord#1", "LineOrd#1", null));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        null));

        testHarness.processElement1(deleteRecord("Ord#2", "LineOrd#2", null));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        null,
                        rowOfKind(RowKind.DELETE, "Ord#2", "LineOrd#2", ADDRESS)));
    }

    /**
     * Whether the replaced left row was emitted is decided by its stored number of matches, so it
     * is retracted even if its matches expired.
     */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testLeftUpsertRetractsOldVersionAfterMatchExpired(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(
                isAntiJoin,
                leftInputSpec,
                rightInputSpec,
                knownValuesNonEquiCondition(),
                true,
                4000L,
                1000L);
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.getOutput().clear();

        testHarness.setStateTtlProcessingTime(2001);
        testHarness.processElement1(updateAfterRecord("Ord#1", "LineOrd#1", UNKNOWN));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.UPDATE_AFTER, "Ord#1", "LineOrd#1", UNKNOWN)));
    }

    /**
     * Left rows whose unique key is wider than the join key are updated and deleted by their stored
     * version.
     */
    @ParameterizedTest(name = "isAntiJoin = {0}")
    @ValueSource(booleans = {false, true})
    void testLeftUpsertsWithUniqueKeyWiderThanJoinKey(boolean isAntiJoin) throws Exception {
        useSemiAntiJoinOperator(
                isAntiJoin,
                leftHasUniqueKeySpec(),
                rightInputSpec,
                knownValuesNonEquiCondition(),
                true,
                0L,
                0L);
        testHarness.processElement2(insertRecord("LineOrd#1", "AIR"));
        testHarness.processElement1(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        testHarness.processElement1(insertRecord("Ord#2", "LineOrd#1", ADDRESS));
        testHarness.getOutput().clear();

        testHarness.processElement1(updateAfterRecord("Ord#1", "LineOrd#1", UNKNOWN));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", ADDRESS),
                        rowOfKind(RowKind.UPDATE_AFTER, "Ord#1", "LineOrd#1", UNKNOWN)));

        testHarness.processElement1(deleteRecord("Ord#2", "LineOrd#1", null));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        rowOfKind(RowKind.DELETE, "Ord#2", "LineOrd#1", ADDRESS),
                        null));

        testHarness.processElement1(deleteRecord("Ord#1", "LineOrd#1", null));
        assertor.shouldEmit(
                testHarness,
                emitted(
                        isAntiJoin,
                        null,
                        rowOfKind(RowKind.DELETE, "Ord#1", "LineOrd#1", UNKNOWN)));
    }

    private void useSemiAntiJoinOperator(
            boolean isAntiJoin,
            JoinInputSideSpec rightSpec,
            GeneratedJoinCondition condition,
            boolean hasNonEquiCondition)
            throws Exception {
        useSemiAntiJoinOperator(
                isAntiJoin, leftInputSpec, rightSpec, condition, hasNonEquiCondition, 0L, 0L);
    }

    private void useSemiAntiJoinOperator(
            boolean isAntiJoin,
            JoinInputSideSpec leftSpec,
            JoinInputSideSpec rightSpec,
            GeneratedJoinCondition condition,
            boolean hasNonEquiCondition,
            long leftStateRetentionTime,
            long rightStateRetentionTime)
            throws Exception {
        testHarness.close();
        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new StreamingSemiAntiJoinOperator(
                                isAntiJoin,
                                leftTypeInfo,
                                rightTypeInfo,
                                condition,
                                leftSpec,
                                rightSpec,
                                UPSERT_WITH_KEY_ONLY_DELETES,
                                UPSERT_WITH_KEY_ONLY_DELETES,
                                hasNonEquiCondition,
                                new boolean[] {true},
                                leftStateRetentionTime,
                                rightStateRetentionTime),
                        leftKeySelector,
                        rightKeySelector,
                        joinKeyTypeInfo);
        testHarness.open();
    }

    /** Returns the row that the semi or the anti join emits, if any. */
    private static RowData[] emitted(
            boolean isAntiJoin, @Nullable RowData semiJoinRow, @Nullable RowData antiJoinRow) {
        final RowData row = isAntiJoin ? antiJoinRow : semiJoinRow;
        return row == null ? new RowData[0] : new RowData[] {row};
    }

    private static final String ADDRESS = "3 Bellevue Drive, Pottstown, PA 19464";
    private static final String NEW_ADDRESS = "68 Manor Station Street, Honolulu, HI 96815";

    private static final Predicate<String> ANTI_JOIN_CHECKER =
            (testDisplayName) -> testDisplayName.contains("Anti");
}
