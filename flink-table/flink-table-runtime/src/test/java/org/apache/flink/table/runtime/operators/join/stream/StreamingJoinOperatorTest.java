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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.AsyncStateStreamingJoinOperator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** Harness tests for {@link StreamingJoinOperator}. */
@ExtendWith(ParameterizedTestExtension.class)
class StreamingJoinOperatorTest extends StreamingJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false, true);
    }

    @Parameter private boolean enableAsyncState;

    @Override
    protected TwoInputStreamOperator<RowData, RowData, RowData> createJoinOperator(
            TestInfo testInfo) {
        Boolean[] joinTypeSpec =
                JOIN_TYPE_EXTRACTOR.apply(
                        testInfo.getTestMethod()
                                .map(Method::getName)
                                .orElse(testInfo.getDisplayName()));
        Long[] ttl = STATE_RETENTION_TIME_EXTRACTOR.apply(testInfo.getTags());
        if (!enableAsyncState) {
            return new StreamingJoinOperator(
                    leftTypeInfo,
                    rightTypeInfo,
                    joinCondition,
                    leftInputSpec,
                    rightInputSpec,
                    joinTypeSpec[0],
                    joinTypeSpec[1],
                    new boolean[] {true},
                    ttl[0],
                    ttl[1]);
        } else {
            return new AsyncStateStreamingJoinOperator(
                    leftTypeInfo,
                    rightTypeInfo,
                    joinCondition,
                    leftInputSpec,
                    rightInputSpec,
                    joinTypeSpec[0],
                    joinTypeSpec[1],
                    new boolean[] {true},
                    ttl[0],
                    ttl[1]);
        }
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

    /**
     * The equivalent SQL as follows.
     *
     * <p>{@code SELECT a.order_id, a.line_order_id, a.shipping_address, b.line_order_id,
     * b.line_order_ship_mode FROM orders a JOIN line_orders b ON a.line_order_id = b.line_order_id}
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=1000")
    @TestTemplate
    void testInnerJoinWithDifferentStateRetentionTime() throws Exception {
        // async state op is not supported to set ttl yet
        assumeFalse(enableAsyncState);

        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        // the right side state of LineOrd#2 has expired
        testHarness.setStateTtlProcessingTime(3000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        // the left side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(deleteRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        // the left side state of LineOrd#2 has expired
        testHarness.setStateTtlProcessingTime(7000);
        testHarness.processElement2(insertRecord("LineOrd#2", "RAIL"));
        assertor.shouldEmitNothing(testHarness);
    }

    /**
     * The equivalent SQL is same with {@link #testInnerJoinWithDifferentStateRetentionTime}. The
     * only difference is that the state retention is disabled.
     */
    @TestTemplate
    void testInnerJoinWithStateRetentionDisabled() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        testHarness.setStateTtlProcessingTime(3000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "AIR"));

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#1",
                        "TRUCK"));

        testHarness.processElement2(deleteRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        testHarness.setStateTtlProcessingTime(7000);
        testHarness.processElement2(insertRecord("LineOrd#2", "RAIL"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "RAIL"));
    }

    /**
     * The equivalent SQL is same with testInnerJoinWithDifferentStateRetentionTime. The only
     * difference is that the left and right state retention time are same.
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=4000")
    @TestTemplate
    void testInnerJoinWithSameStateRetentionTime() throws Exception {
        // async state op is not supported to set ttl yet
        assumeFalse(enableAsyncState);

        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        // extend the expired time to 8000 for LineOrd#2
        testHarness.setStateTtlProcessingTime(4000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "AIR"));

        // the state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        // the expired time for left and right state of LineOrd#2 is 8000
        testHarness.setStateTtlProcessingTime(7999);
        testHarness.processElement2(updateAfterRecord("LineOrd#2", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "TRUCK"));

        testHarness.setStateTtlProcessingTime(8000);
        testHarness.processElement2(updateAfterRecord("LineOrd#2", "RAIL"));
        assertor.shouldEmitNothing(testHarness);
    }

    /**
     * The equivalent SQL as follows.
     *
     * <p>{@code SELECT a.order_id, a.line_order_id, a.shipping_address, b.line_order_id,
     * b.line_order_ship_mode FROM orders a LEFT JOIN line_orders b ON a.line_order_id =
     * b.line_order_id}
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=1000")
    @TestTemplate
    void testLeftOuterJoinWithDifferentStateRetentionTime() throws Exception {
        // async state op is not supported to set ttl yet
        assumeFalse(enableAsyncState);

        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null));

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null));

        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        // the right side state of LineOrd#2 has expired
        testHarness.setStateTtlProcessingTime(3000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        null,
                        null));

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        // the left side state of LineOrd#1 has expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(deleteRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        null,
                        null));

        // the left side state of LineOrd#2 has expired
        testHarness.setStateTtlProcessingTime(8001);
        testHarness.processElement2(insertRecord("LineOrd#2", "RAIL"));
        assertor.shouldEmitNothing(testHarness);
    }

    /**
     * The equivalent SQL is the same as {@link
     * #testLeftOuterJoinWithDifferentStateRetentionTime()}. The only difference is that the state
     * retention is disabled.
     */
    @TestTemplate
    void testLeftOuterJoinWithStateRetentionDisabled() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null));

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null));

        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        testHarness.setStateTtlProcessingTime(3000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "AIR"));

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#1",
                        "TRUCK"));

        testHarness.setStateTtlProcessingTime(8001);
        testHarness.processElement2(deleteRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));
    }

    /**
     * The equivalent SQL as follows.
     *
     * <p>{@code SELECT a.order_id, a.line_order_id, a.shipping_address, b.line_order_id,
     * b.line_order_ship_mode FROM orders a RIGHT JOIN line_orders b ON a.line_order_id =
     * b.line_order_id}
     */
    @Tag("leftStateRetentionTime=4000")
    @Tag("rightStateRetentionTime=1000")
    @TestTemplate
    void testRightOuterJoinWithDifferentStateRetentionTime() throws Exception {
        // async state op is not supported to set ttl yet
        assumeFalse(enableAsyncState);

        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        // left side state is expired
        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness, rowOfKind(RowKind.INSERT, null, null, null, "LineOrd#2", "AIR"));

        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmit(
                testHarness, rowOfKind(RowKind.INSERT, null, null, null, "LineOrd#1", "TRUCK"));

        // the right side state has expired
        testHarness.setStateTtlProcessingTime(5001);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(updateAfterRecord("LineOrd#2", "SHIP"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "SHIP"));

        testHarness.processElement2(updateAfterRecord("LineOrd#1", "RAIL"));
        assertor.shouldEmit(
                testHarness, rowOfKind(RowKind.INSERT, null, null, null, "LineOrd#1", "RAIL"));

        testHarness.setStateTtlProcessingTime(6000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#1", "3 North Winchester Drive, Haines City, FL 33844"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, null, null, null, "LineOrd#1", "RAIL"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 North Winchester Drive, Haines City, FL 33844",
                        "LineOrd#1",
                        "RAIL"));

        // right side state has expired
        testHarness.setStateTtlProcessingTime(7000);
        testHarness.processElement1(
                deleteRecord(
                        "Ord#1", "LineOrd#1", "3 North Winchester Drive, Haines City, FL 33844"));
        assertor.shouldEmitNothing(testHarness);
    }

    /**
     * The equivalent SQL is the same as {@link
     * #testRightOuterJoinWithDifferentStateRetentionTime()}. The only difference is that the state
     * retention is disabled.
     */
    @TestTemplate
    void testRightOuterJoinWithDStateRetentionDisabled() throws Exception {
        testHarness.setStateTtlProcessingTime(1);
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.setStateTtlProcessingTime(4001);
        testHarness.processElement2(insertRecord("LineOrd#2", "AIR"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#2",
                        "AIR"));

        testHarness.setStateTtlProcessingTime(10000);
        testHarness.processElement2(insertRecord("LineOrd#1", "TRUCK"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "LineOrd#1",
                        "TRUCK"));

        testHarness.setStateTtlProcessingTime(20000);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#1", "LineOrd#2", "68 Manor Station Street, Honolulu, HI 96815"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "68 Manor Station Street, Honolulu, HI 96815",
                        "LineOrd#2",
                        "AIR"));
    }

    private static final Function<String, Boolean[]> JOIN_TYPE_EXTRACTOR =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return new Boolean[] {false, false};
                } else if (testDisplayName.contains("LeftOuterJoin")) {
                    return new Boolean[] {true, false};
                } else {
                    return new Boolean[] {false, true};
                }
            };
}
