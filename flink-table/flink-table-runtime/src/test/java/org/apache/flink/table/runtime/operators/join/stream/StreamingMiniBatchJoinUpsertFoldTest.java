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

import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountCoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.table.runtime.operators.join.stream.ChangelogOracle.row;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;

/**
 * A mini-batch join must not fold an UPDATE_AFTER and a DELETE of an upsert input into nothing.
 *
 * <p>The bundles fold "+U followed by -D" of the same key into nothing, which is right for a
 * retract input, where the -U before the +U retracts the stored version. An upsert input has no -U:
 * if the old version is stored from an earlier bundle, the folded DELETE is lost and the old
 * version and its joined rows stay forever.
 *
 * <p>An upsert input whose unique key contains the join key is joined without ChangelogNormalize if
 * the consumer of the join only needs UPDATE_AFTER, e.g. an upsert sink.
 *
 * <p>Schema: orders(order_id, line_order_id, shipping_address), line_orders(line_order_id,
 * line_order_ship_mode), joined ON orders.line_order_id = line_orders.line_order_id. Orders are
 * keyed by line_order_id (= join key) or by (order_id, line_order_id) (wider than the join key);
 * line_orders by line_order_id.
 */
class StreamingMiniBatchJoinUpsertFoldTest extends StreamingJoinOperatorTestBase {

    private static final String ADDRESS = "3 Bellevue Drive, Pottstown, PA 19464";
    private static final String NEW_ADDRESS = "68 Manor Station Street, Honolulu, HI 96815";

    private ChangelogOracle oracle;
    private long checkpointId;

    @Override
    protected TwoInputStreamOperator<RowData, RowData, RowData> createJoinOperator(
            TestInfo testInfo) {
        return miniBatchJoin(FlinkJoinType.INNER, leftInputSpec);
    }

    @Override
    protected RowType getOutputType() {
        return RowType.of(
                Stream.concat(
                                leftTypeInfo.toRowType().getChildren().stream(),
                                rightTypeInfo.toRowType().getChildren().stream())
                        .toArray(LogicalType[]::new),
                Stream.concat(
                                leftTypeInfo.toRowType().getFieldNames().stream(),
                                rightTypeInfo.toRowType().getFieldNames().stream())
                        .toArray(String[]::new));
    }

    static Stream<Arguments> joinTypesAndKeys() {
        return Stream.of(
                Arguments.of(FlinkJoinType.INNER, false),
                Arguments.of(FlinkJoinType.LEFT, false),
                Arguments.of(FlinkJoinType.INNER, true),
                Arguments.of(FlinkJoinType.LEFT, true));
    }

    /**
     * +I(k, v0) is flushed, then +U(k, v1) and -D(k) arrive in one bundle: k must be gone, also for
     * a later match.
     */
    @ParameterizedTest(name = "{0} join, unique key wider than join key = {1}")
    @MethodSource("joinTypesAndKeys")
    void testUpdateAfterAndDeleteOfLeftInOneBundle(FlinkJoinType joinType, boolean widerKey)
            throws Exception {
        use(joinType, widerKey);
        right(insertRecord("LineOrd#1", "AIR"));
        left(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        flush();

        left(updateAfterRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        left(deleteRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        flush();
        oracle.assertResult();

        // the deleted order must not join a later version of the line order
        right(updateAfterRecord("LineOrd#1", "SHIP"));
        flush();
        oracle.assertResult();
    }

    /** Same as above, with a key-only DELETE as an upsert source sends it. */
    @ParameterizedTest(name = "{0} join, unique key wider than join key = {1}")
    @MethodSource("joinTypesAndKeys")
    void testUpdateAfterAndKeyOnlyDeleteOfLeftInOneBundle(FlinkJoinType joinType, boolean widerKey)
            throws Exception {
        use(joinType, widerKey);
        right(insertRecord("LineOrd#1", "AIR"));
        left(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        flush();

        left(updateAfterRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        left(deleteRecord(widerKey ? "Ord#1" : null, "LineOrd#1", null));
        flush();
        oracle.assertResult();
    }

    /** The right side is updated and deleted in one bundle: the left row is null padded again. */
    @ParameterizedTest(name = "{0} join, unique key wider than join key = {1}")
    @MethodSource("joinTypesAndKeys")
    void testUpdateAfterAndDeleteOfRightInOneBundle(FlinkJoinType joinType, boolean widerKey)
            throws Exception {
        use(joinType, widerKey);
        left(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        right(insertRecord("LineOrd#1", "AIR"));
        flush();

        right(updateAfterRecord("LineOrd#1", "SHIP"));
        right(deleteRecord("LineOrd#1", "SHIP"));
        flush();
        if (joinType == FlinkJoinType.LEFT) {
            oracle.assertResult(row("Ord#1", "LineOrd#1", ADDRESS, null, null));
        } else {
            oracle.assertResult();
        }
    }

    /** Control: without the UPDATE_AFTER, the DELETE alone is applied. */
    @ParameterizedTest(name = "{0} join, unique key wider than join key = {1}")
    @MethodSource("joinTypesAndKeys")
    void testDeleteOfLeftInNextBundle(FlinkJoinType joinType, boolean widerKey) throws Exception {
        use(joinType, widerKey);
        right(insertRecord("LineOrd#1", "AIR"));
        left(insertRecord("Ord#1", "LineOrd#1", ADDRESS));
        flush();

        left(updateAfterRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        flush();
        left(deleteRecord("Ord#1", "LineOrd#1", NEW_ADDRESS));
        flush();
        oracle.assertResult();
    }

    // ------------------------------------------------------------------------------------------

    private void left(StreamRecord<RowData> record) throws Exception {
        testHarness.processElement1(record);
        oracle.consume(testHarness.getOutput());
    }

    private void right(StreamRecord<RowData> record) throws Exception {
        testHarness.processElement2(record);
        oracle.consume(testHarness.getOutput());
    }

    /** Ends the bundle, as a checkpoint does. */
    private void flush() throws Exception {
        testHarness.prepareSnapshotPreBarrier(++checkpointId);
        oracle.consume(testHarness.getOutput());
    }

    private void use(FlinkJoinType joinType, boolean widerKey) throws Exception {
        final JoinInputSideSpec leftSpec;
        if (widerKey) {
            final RowDataKeySelector uniqueKeySelector =
                    HandwrittenSelectorUtil.getRowDataSelector(
                            new int[] {0, 1},
                            leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
            leftSpec =
                    JoinInputSideSpec.withUniqueKey(
                            uniqueKeySelector.getProducedType(), uniqueKeySelector);
        } else {
            leftSpec = leftInputSpec;
        }
        testHarness.close();
        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        miniBatchJoin(joinType, leftSpec),
                        leftKeySelector,
                        rightKeySelector,
                        joinKeyTypeInfo);
        testHarness.open();
        // the upsert sink is keyed by the unique key of the orders, which contains the join key
        oracle = new ChangelogOracle(getOutputType(), 0, 1);
    }

    /** A bundle only ends with {@link #flush()}. */
    private MiniBatchStreamingJoinOperator miniBatchJoin(
            FlinkJoinType joinType, JoinInputSideSpec leftSpec) {
        return MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
                joinType,
                leftTypeInfo,
                rightTypeInfo,
                joinCondition,
                leftSpec,
                rightInputSpec,
                joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL,
                joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL,
                UPSERT_WITH_KEY_ONLY_DELETES,
                UPSERT_WITH_KEY_ONLY_DELETES,
                false,
                new boolean[] {true},
                0L,
                0L,
                new CountCoBundleTrigger<>(1000));
    }
}
