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

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.function.Predicate;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;

/** Test for {@link StreamingSemiAntiJoinOperator}. */
public class StreamingSemiAntiJoinOperatorTest extends StreamingJoinOperatorTestBase {
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
    public void testLeftSemiJoinWithDifferentStateRetentionTime() throws Exception {
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
        assertor.shouldEmitNothing(testHarness);

        // numOfAssociations is reduced to 1, retract the record
        testHarness.processElement2(deleteRecord("LineOrd#2", "AIR"));
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
    public void testLeftSemiJoinWithStateRetentionDisabled() throws Exception {
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
        assertor.shouldEmitNothing(testHarness);

        // numOfAssociations is reduced to 1, retract the record
        testHarness.processElement2(deleteRecord("LineOrd#2", "AIR"));
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
    public void testLeftAntiJoinWithDifferentStateRetentionTime() throws Exception {
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
    public void testLeftAntiJoinWithStateRetentionTimeDisabled() throws Exception {
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

    private static final Predicate<String> ANTI_JOIN_CHECKER =
            (testDisplayName) -> testDisplayName.contains("Anti");
}
