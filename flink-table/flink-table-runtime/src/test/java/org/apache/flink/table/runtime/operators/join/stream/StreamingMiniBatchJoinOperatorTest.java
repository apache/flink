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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountCoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Test for StreamingMiniBatchJoinOperatorTest. */
public final class StreamingMiniBatchJoinOperatorTest extends StreamingJoinOperatorTestBase {

    private RowDataKeySelector leftUniqueKeySelector;
    private RowDataKeySelector rightUniqueKeySelector;

    @BeforeEach
    public void beforeEach(TestInfo testInfo) throws Exception {
        rightTypeInfo =
                InternalTypeInfo.of(
                        RowType.of(
                                new LogicalType[] {
                                    new CharType(false, 20),
                                    new CharType(false, 20),
                                    new CharType(true, 10)
                                },
                                new String[] {
                                    "order_id#", "line_order_id0", "line_order_ship_mode"
                                }));

        rightKeySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {1},
                        rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
        super.beforeEach(testInfo);
    }

    @Tag("miniBatchSize=3")
    @Test
    public void testLeftJoinWithLeftArriveFirst() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=1")
    @Test
    public void testLeftJoinWithLeftArriveFirstNoMiniBatch() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I

        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=3")
    @Test
    public void testRightJoinWithRightArriveFirst() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=1")
    @Test
    public void testRightJoinWithRightArriveFirstWithNoMiniBatch() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I

        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(RowKind.DELETE, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=3")
    @Test
    public void testFullJoinWithRightArriveFirst() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I

        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        null,
                        null,
                        null),
                rowOfKind(RowKind.DELETE, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=1")
    @Test
    public void testFullJoinWithRightArriveFirstWithNoMiniBatch() throws Exception {
        // joinKey is LineOrd
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right

        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#2", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I

        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(RowKind.DELETE, null, null, null, "Ord#X", "LineOrd#2", "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        null,
                        null,
                        null));
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testInnerJoinJoinKeyContainsUniqueKeyWithoutFold() throws Exception {
        // joinKey is LineOrd
        testHarness.setStateTtlProcessingTime(1);
        // basic test for that the mini-batch process could be triggerred normally
        testHarness.processElement1(
                insertRecord(
                        "Ord#1",
                        "LineOrd#1",
                        "1 Bellevue Drive, Pottstown, PA 19464")); // left +I x
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement1(
                insertRecord(
                        "Ord#1",
                        "LineOrd#2",
                        "2 Bellevue Drive, Pottstown, PA 19464")); // left +I xx
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#1", "TRUCK")); // right +I x
        assertor.shouldEmitNothing(testHarness);

        // exactly reach to the mini-batch size
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right +I   xx
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "1 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#Y",
                        "LineOrd#1",
                        "TRUCK"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "2 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#X",
                        "LineOrd#2",
                        "AIR"));
    }

    @Tag("miniBatchSize=18")
    @Test
    public void testInnerJoinWithJoinKeyContainsUniqueKeyWithinBatch() throws Exception {
        // joinKey is LineOrd
        // left fold  || right fold
        // +I +U / +U +U / +U -D ||  +I -D / +U -U / +I -U
        testHarness.processElement1(
                insertRecord(
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464")); // left +I  x
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464")); // left +U x | +I +U
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#7", "RAILWAY")); // right +I  a
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464")); // left +U  y
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#x5",
                        "LineOrd#5",
                        "x3 Bellevue Drive, Pottstown, PAxx 19464")); // left +U   y |  +U  +U
        testHarness.processElement1(
                insertRecord(
                        "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464")); // left +I
        testHarness.processElement2(
                deleteRecord("Ord#X", "LineOrd#7", "RAILWAY")); // right -D  a ｜ +I -D
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464")); // left +U
        testHarness.processElement1(
                deleteRecord(
                        "Ord#3",
                        "LineOrd#x3",
                        "14y0 Bellevue Drive, Pottstown, PJyy 19464")); // left -D  | +U -D
        testHarness.processElement2(
                updateAfterRecord("Ord#X", "LineOrd#7", "AIR")); // right +U    b
        testHarness.processElement1(
                insertRecord(
                        "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464")); // left +I
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(
                updateBeforeRecord("Ord#X", "LineOrd#7", "AIR")); // right -U    b  | +U -U
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right +I     c
        testHarness.processElement2(
                updateBeforeRecord("Ord#X", "LineOrd#2", "AIR")); // right -U    c  | +I -U
        // right state is empty
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#1", "AIR")); // right +I
        testHarness.processElement2(updateBeforeRecord("Ord#Y", "LineOrd#5", "TRUCK")); // right -U
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#6", "RAILWAY")); // right +I
        testHarness.processElement2(
                updateBeforeRecord(
                        "Ord#Z", "LineOrd#4", "RAILWAY")); // no effect to state  // right -U
        // left state  |  right state
        // "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"     |  "Ord#X",
        // "LineOrd#1", "AIR"
        // "Ord#x5", "LineOrd#5", "x3 Bellevue Drive, Pottstown, PAxx 19464" |  "Ord#Y",
        // "LineOrd#6", "RAILWAY"
        // "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464"
        // "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#Y",
                        "LineOrd#6",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#X",
                        "LineOrd#1",
                        "AIR"));
    }

    @Tag("miniBatchSize=10")
    @Test
    public void testInnerJoinWithJoinKeyContainsUniqueKeyCrossBatches() throws Exception {
        // joinKey is LineOrd

        testHarness.processElement1(
                insertRecord(
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464")); // left +I   z
        testHarness.processElement1(
                insertRecord(
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464")); // left +I   z  | +I +I
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#4", "TRUCK")); // right +I    y
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord(
                        "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464")); // left +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464")); // left +I    x
        testHarness.processElement1(
                updateBeforeRecord(
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464")); // left -U  x  |  +I -U
        testHarness.processElement1(
                updateBeforeRecord(
                        "Ord#9", "LineOrd#9", "11 Bellevue Drive, Pottstown, PI 19464")); // left -U
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#9", "AIR")); // right +I
        testHarness.processElement2(updateAfterRecord("Ord#xyz", "LineOrd#1", "SHIP")); // right +U
        testHarness.processElement2(
                deleteRecord("Ord#Y", "LineOrd#4", "TRUCK")); // right -D    y | +I -D
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#9",
                        "LineOrd#9",
                        "11 Bellevue Drive, Pottstown, PI 19464",
                        "Ord#X",
                        "LineOrd#9",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#xyz",
                        "LineOrd#1",
                        "SHIP"));
        // +---------------------------------------------------------------+---------------------------------+
        // |                            left state                         |           right state
        //         |
        // |---------------------------------------------------------------|---------------------------------|
        // | "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464" | "Ord#X", "LineOrd#9",
        // "AIR"     |
        // | "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464" | "Ord#xyz", "LineOrd#1",
        // "SHIP"  |
        // +---------------------------------------------------------------+---------------------------------+

        // second join:
        // 1.left stream state(last batch defined) join new input from right stream.
        // 2.right stream state(current and last batch defined) join new input from left stream.

        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#adjust",
                        "LineOrd#4",
                        "14 Bellevue Drive, Pottstown, PJ 19464")); // left +U x
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#18",
                        "LineOrd#9",
                        "22 Bellevue Drive, Pottstown, PK 19464")); // left +U
        testHarness.processElement2(deleteRecord("Ord#X", "LineOrd#x3", "AIR")); // right -D
        testHarness.processElement2(updateBeforeRecord("Ord#xyz", "LineOrd#1", "SHIP")); // right -U
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#4", "TRUCK")); // right +I
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#14",
                        "LineOrd#4",
                        "18 Bellevue Drive, Pottstown, PL 19464")); // left +U  x | +U +U
        testHarness.processElement1(
                deleteRecord(
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464")); // left -D
        testHarness.processElement1(
                insertRecord(
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464")); // left +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#10",
                        "LineOrd#100y",
                        "14y0 Bellevue Drive, Pottstown, PJyy 19464")); // left +I
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(updateAfterRecord("Ord#101", "LineOrd#x3", "AIR")); // right +U

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#Y",
                        "LineOrd#4",
                        "TRUCK"),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        "Ord#xyz",
                        "LineOrd#1",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464",
                        "Ord#101",
                        "LineOrd#x3",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#x3",
                        "x5 Bellevue Drive, Pottstown, PCxx 19464",
                        "Ord#101",
                        "LineOrd#x3",
                        "AIR"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#18",
                        "LineOrd#9",
                        "22 Bellevue Drive, Pottstown, PK 19464",
                        "Ord#X",
                        "LineOrd#9",
                        "AIR"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#14",
                        "LineOrd#4",
                        "18 Bellevue Drive, Pottstown, PL 19464",
                        "Ord#Y",
                        "LineOrd#4",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=13")
    @Test
    public void testInnerJoinWithHasUniqueKeyWithinBatch() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        // +I +U / +I -U / +I -D / +U -D /+U +U
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1",
                                "LineOrd#1",
                                "3 Bellevue Drive, Pottstown, PA 19464"), // 2  +I -U
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "5 Bellevue Drive, Pottstown, PC 19464"), // 1  +I +U
                        updateAfterRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "xxx Bellevue Drive, Pottstown, PJ 19464"), // 1
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 3  +I -D
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"), // 3
                        updateBeforeRecord(
                                "Ord#12",
                                "LineOrd#4",
                                "6 Bellevue Drive, Pottstown, PD 19464"), // no effect
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PC 19464"), // 4   +U -D
                        deleteRecord(
                                "Ord#9",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PC 19464")); // 4
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        // +-------------------------------------------------------------------+
        // |                            left state                             |
        // |-------------------------------------------------------------------|
        // | "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"     |
        // | "Ord#3", "LineOrd#10", "xxx Bellevue Drive, Pottstown, PJ 19464"  |
        // | "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"     |
        // +-------------------------------------------------------------------+
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#5", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#6",
                        "LineOrd#5",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"));
    }

    @Tag("miniBatchSize=8")
    @Test
    public void testInnerJoinWithHasUniqueKeyCrossBatches() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        // fold +I/+U +U (same and different jks)
        testHarness.processElement1(
                updateBeforeRecord(
                        "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464")); // left -U
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464")); // left  +U
        testHarness.processElement2(insertRecord("Ord#5", "LineOrd#5", "SHIP")); // right  +I
        testHarness.processElement1(
                insertRecord(
                        "Ord#4",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PC 19464")); // left +I  x
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464")); // left +U  x | +I +U
        testHarness.processElement2(
                updateAfterRecord("Ord#22", "LineOrd#4", "SHIP")); // right  +U    join
        testHarness.processElement2(insertRecord("Ord#23", "LineOrd#10", "AIR")); // right +I
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#4",
                        "LineOrd#4",
                        "xxx Bellevue Drive, Pottstown, PJ 19464")); // left +U x | +I +U +U  join
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#4",
                        "LineOrd#4",
                        "xxx Bellevue Drive, Pottstown, PJ 19464",
                        "Ord#22",
                        "LineOrd#4",
                        "SHIP"));

        // +-----------------------------------------------------------------+----------------------------------+
        // |                              left state                         |           right state
        //            |
        // |-----------------------------------------------------------------|----------------------------------|
        // | "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"   | "Ord#5", "LineOrd#5",
        // "SHIP"     |
        // | "Ord#4", "LineOrd#4", "xxx Bellevue Drive, Pottstown, PJ 19464" | "Ord#22",
        // "LineOrd#4", "SHIP"    |
        // |                                                                 | "Ord#23",
        // "LineOrd#10", "AIR"    |
        // +-----------------------------------------------------------------+----------------------------------+

        // fold +I/+U -U/D (same and different jks)
        testHarness.processElement1(
                insertRecord(
                        "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464")); // left  +I
        testHarness.processElement1(
                updateBeforeRecord(
                        "Ord#6",
                        "LineOrd#6",
                        "8 Bellevue Drive, Pottstown, PF 19464")); // left   -U
        testHarness.processElement2(insertRecord("Ord#21", "LineOrd#5", "RAILWAY")); //  right +I  x
        testHarness.processElement2(
                insertRecord("Ord#1", "LineOrd#5", "TRUCK")); //   right +I  x | +I +I
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464")); // left +U
        testHarness.processElement1(
                deleteRecord(
                        "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464")); // left -D
        testHarness.processElement2(
                updateBeforeRecord("Ord#5", "LineOrd#5", "SHIP")); // right -U   x | +I +I -U
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement2(updateBeforeRecord("Ord#22", "LineOrd#6", "AIR")); // right -U

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#21",
                        "LineOrd#5",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#1",
                        "LineOrd#5",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=20")
    @Test
    public void testInnerJoinWithNoUniqueKeyWithinBatch() throws Exception {
        // joinKey is LineOrd
        // +I -U / +I -D / -U +U / -D +I
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x    +I -D
                        deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PD 19464"), // x   +I -U
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x   -U +U
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        updateAfterRecord(
                                "Ord#18", "LineOrd#18", "22 Bellevue Drive, Pottstown, PK 19464"),
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x   -D +I
                        insertRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464") // 4x
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "AIR"),
                        updateAfterRecord("Ord#1", "LineOrd#2", "SHIP"),
                        updateBeforeRecord("Ord#1", "LineOrd#2", "RAILWAY"),
                        insertRecord("Ord#1", "LineOrd#2", "RAILWAY"),
                        deleteRecord("Ord#6", "LineOrd#6", "RAILWAY"),
                        insertRecord("Ord#6", "LineOrd#6", "RAILWAY"));

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"),
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP",
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464"));
    }

    @Tag("miniBatchSize=6")
    @Test
    public void testInnerJoinWithNoUniqueKeyCrossBatches() throws Exception {
        // joinKey is LineOrd
        // completely duplicate records
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#1", "LineOrd#1", "AIR"));
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement1(
                deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement2(updateAfterRecord("Ord#1", "LineOrd#2", "SHIP"));
        assertor.shouldEmitNothing(testHarness);
        testHarness.processElement1(
                insertRecord("Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"));
        // left state   |    right state
        // "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#1", "LineOrd#1",
        // "AIR"
        // "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#1", "LineOrd#2",
        // "SHIP"
        // "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"  |
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"));
        testHarness.processElement1(
                updateBeforeRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement1(
                updateAfterRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        testHarness.processElement2(insertRecord("Ord#1", "LineOrd#3", "AIR"));
        testHarness.processElement1(
                deleteRecord("Ord#6", "LineOrd#1", "8 Bellevue Drive, Pottstown, PF 19464"));
        testHarness.processElement2(deleteRecord("Ord#1", "LineOrd#2", "SHIP"));
        // right state
        // "Ord#1", "LineOrd#1", "AIR"
        // "Ord#1", "LineOrd#3", "AIR"
        testHarness.processElement1(
                insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#3",
                        "5 Bellevue Drive, Pottstown, PD 19464",
                        "Ord#1",
                        "LineOrd#3",
                        "AIR"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#1",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#6",
                        "LineOrd#1",
                        "8 Bellevue Drive, Pottstown, PF 19464",
                        "Ord#1",
                        "LineOrd#1",
                        "AIR"));
    }

    /** Outer join only emits INSERT or DELETE Msg. */
    @Tag("miniBatchSize=10")
    @Test
    public void testLeftJoinWithJoinKeyContainsUniqueKey() throws Exception {
        // joinKey is LineOrd
        // left fold  || right fold
        // +I +U / +U +U / +U -D ||  +I -D / +U -U / +I -U
        testHarness.processElement1(
                insertRecord(
                        "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464")); // left +I
        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464")); // left +U

        testHarness.processElement1(
                insertRecord(
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464")); // left  +I

        testHarness.processElement1(
                updateAfterRecord(
                        "Ord#3", "LineOrd#x3", "x5 Bellevue Drive, Pottstown, PCxx 19464")); // left
        testHarness.processElement1(
                deleteRecord(
                        "Ord#3",
                        "LineOrd#x3",
                        "14y0 Bellevue Drive, Pottstown, PJyy 19464")); // left

        testHarness.processElement1(
                insertRecord(
                        "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464")); // left
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#2", "AIR")); // right
        // right state is empty
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#1", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#Y", "LineOrd#6", "RAILWAY")); // right
        // left state  |  right state
        // "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#X","LineOrd#1",
        // "AIR"
        // "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464" |  "Ord#Y","LineOrd#6",
        // "RAILWAY"
        // "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#Y",
                        "LineOrd#6",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#4",
                        "LineOrd#4",
                        "6 Bellevue Drive, Pottstown, PD 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#X",
                        "LineOrd#1",
                        "AIR"));
        // left state  |  right state
        // "Ord#2", "LineOrd#1", "4 Bellevue Drive, Pottstown, PB 19464"  |  "Ord#X","LineOrd#1",
        // "AIR"
        // "Ord#i", "LineOrd#6", "i6 Bellevue Drive, Pottstown, Pi 19464" |  "Ord#Y","LineOrd#6",
        // "RAILWAY"
        // "Ord#4", "LineOrd#4", "6 Bellevue Drive, Pottstown, PD 19464"
        testHarness.processElement2(
                updateBeforeRecord("Ord#Y", "LineOrd#6", "RAILWAY")); // right -U
        testHarness.processElement2(updateAfterRecord("Ord#UU", "LineOrd#6", "SHIP")); // right  +U
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#3", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#2", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#7", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#8", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#9", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#10", "AIR")); // right
        testHarness.processElement2(insertRecord("Ord#X", "LineOrd#11", "AIR")); // right
        testHarness.processElement2(updateBeforeRecord("Ord#X", "LineOrd#1", "AIR")); // right

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#Y",
                        "LineOrd#6",
                        "RAILWAY"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#i",
                        "LineOrd#6",
                        "i6 Bellevue Drive, Pottstown, Pi 19464",
                        "Ord#UU",
                        "LineOrd#6",
                        "SHIP"),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#X",
                        "LineOrd#1",
                        "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#1",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null));
    }

    /** Outer join only emits INSERT or DELETE Msg. */
    @Tag("miniBatchSize=4")
    @Test
    public void testLeftJoinWithHasUniqueKey() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        // +I +U / +I -U / +I -D /+U +U
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1",
                                "LineOrd#1",
                                "3 Bellevue Drive, Pottstown, PA 19464"), // 2  +I -U
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "5 Bellevue Drive, Pottstown, PC 19464"), // 1  +I +U
                        updateAfterRecord(
                                "Ord#3",
                                "LineOrd#10",
                                "xxx Bellevue Drive, Pottstown, PJ 19464"), // 1
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 3 +I -D
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"), // 3
                        updateAfterRecord(
                                "Ord#6",
                                "LineOrd#7",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 5   +U +U
                        updateAfterRecord(
                                "Ord#6",
                                "LineOrd#7",
                                "9 Bellevue Drive, Pottstown, PF 19464")); // 5

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#3",
                        "LineOrd#10",
                        "xxx Bellevue Drive, Pottstown, PJ 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null));
        // +----------------------------------------------------------------+
        // |                       left state                               |
        // |----------------------------------------------------------------|
        // | "Ord#2","LineOrd#2","4 Bellevue Drive, Pottstown, PB 19464"    |
        // | "Ord#3","LineOrd#10","xxx Bellevue Drive, Pottstown, PJ 19464" |
        // | "Ord#5","LineOrd#5","7 Bellevue Drive, Pottstown, PE 19464"    |
        // | "Ord#6","LineOrd#7","9 Bellevue Drive, Pottstown, PF 19464"    |
        // +----------------------------------------------------------------+

        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#4", "AIR"),
                        updateBeforeRecord("Ord#5", "LineOrd#2", "SHIP"), // -U +U pattern
                        updateAfterRecord("Ord#5", "LineOrd#2", "TRUCK"), // -U +U pattern
                        updateAfterRecord("Ord#7", "LineOrd#0", "AIR"),
                        updateAfterRecord("Ord#8", "LineOrd#11", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#6",
                        "LineOrd#7",
                        "9 Bellevue Drive, Pottstown, PF 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinHasUniqueKeyRetAndAcc() throws Exception {
        // this case would create buffer of JoinHasUniqueKey
        testLeftJoinWithUpdate();
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinJoinKeyContainsUniqueKeyRetAndAcc() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithUpdate();
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinHasUniqueKeyWithoutRetract() throws Exception {
        // this case would create buffer of JoinHasUniqueKey
        testLeftJoinWithoutRetract();
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinJoinKeyContainsUniqueKeyWithoutRetract() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithoutRetract();
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinJoinKeyContainsUniqueKeyWithoutAcc() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithoutAcc();
    }

    @Tag("miniBatchSize=2")
    @Test
    public void testLeftJoinHasUniqueKeyWithoutAcc() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithoutAcc();
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testLeftJoinHasUniqueKeyWithUpdateMultipleCases() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithUpdateRecordsMultipleCases();
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testLeftJoinJoinKeyContainsUniqueKeyWithUpdateMultipleCases() throws Exception {
        // this case would create buffer of JoinKeyContainsUniqueKey
        testLeftJoinWithUpdateRecordsMultipleCases();
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testRightJoinWithHasUniqueKey() throws Exception {
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1",
                                "LineOrd#1",
                                "3 Bellevue Drive, Pottstown, PA 19464"), // 2  +I -U
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 3 +I -D
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "7 Bellevue Drive, Pottstown, PE 19464")); // 3

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmitNothing(testHarness);
        // +----------------------------------------------------------------+
        // |                       left state                               |
        // |----------------------------------------------------------------|
        // | "Ord#2","LineOrd#2","4 Bellevue Drive, Pottstown, PB 19464"    |
        // | "Ord#5","LineOrd#5","7 Bellevue Drive, Pottstown, PE 19464"    |
        // +----------------------------------------------------------------+
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#4", "AIR"),
                        updateBeforeRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#5", "LineOrd#2", "TRUCK"),
                        updateAfterRecord("Ord#7", "LineOrd#0", "AIR"),
                        updateAfterRecord("Ord#8", "LineOrd#11", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#6", "LineOrd#4", "AIR"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#7", "LineOrd#0", "AIR"),
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#8", "LineOrd#11", "AIR"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=4")
    @Test
    public void testFullJoinWithHasUniqueKey() throws Exception {
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord(
                                "Ord#1",
                                "LineOrd#1",
                                "3 Bellevue Drive, Pottstown, PA 19464"), // 2  +I -U
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 3 +I -D
                        updateBeforeRecord(
                                "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"), // 2
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#5",
                                "7 Bellevue Drive, Pottstown, PE 19464")); // 3

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#6",
                        "LineOrd#5",
                        "8 Bellevue Drive, Pottstown, PF 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null));
        // +----------------------------------------------------------------+
        // |                       left state                               |
        // |----------------------------------------------------------------|
        // | "Ord#2","LineOrd#2","4 Bellevue Drive, Pottstown, PB 19464"    |
        // | "Ord#5","LineOrd#5","7 Bellevue Drive, Pottstown, PE 19464"    |
        // +----------------------------------------------------------------+

        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#6", "LineOrd#4", "AIR"),
                        updateBeforeRecord("Ord#5", "LineOrd#2", "SHIP"), // -U +U pattern
                        updateAfterRecord("Ord#5", "LineOrd#2", "TRUCK"), // -U +U pattern
                        updateAfterRecord("Ord#7", "LineOrd#0", "AIR"),
                        updateAfterRecord("Ord#8", "LineOrd#11", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#6", "LineOrd#4", "AIR"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#6",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null),
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#7", "LineOrd#0", "AIR"),
                rowOfKind(RowKind.INSERT, null, null, null, "Ord#8", "LineOrd#11", "AIR"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#5",
                        "LineOrd#2",
                        "TRUCK"));
    }

    @Tag("miniBatchSize=15")
    @Test
    public void testLeftJoinWithNoUniqueKey() throws Exception {
        // joinKey is LineOrd
        // +I -U / +I -D / -U +U / -D +I
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"),
                        insertRecord("Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        insertRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x    +I -D
                        deleteRecord("Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464"),
                        insertRecord(
                                "Ord#3",
                                "LineOrd#3",
                                "5 Bellevue Drive, Pottstown, PD 19464"), // x  +I -U
                        updateBeforeRecord(
                                "Ord#3", "LineOrd#3", "5 Bellevue Drive, Pottstown, PD 19464"), // x
                        updateBeforeRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x   -U +U
                        updateAfterRecord(
                                "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464"),
                        deleteRecord(
                                "Ord#1",
                                "LineOrd#2",
                                "4 Bellevue Drive, Pottstown, PB 19464"), // 2x
                        updateAfterRecord(
                                "Ord#9",
                                "LineOrd#9",
                                "11 Bellevue Drive, Pottstown, PI 19464"), // 3x
                        deleteRecord(
                                "Ord#6",
                                "LineOrd#6",
                                "8 Bellevue Drive, Pottstown, PF 19464"), // 4x   -D +I
                        insertRecord(
                                "Ord#6", "LineOrd#6", "8 Bellevue Drive, Pottstown, PF 19464") // 4x
                        );

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        // +------------------------------------------------------------------+
        // |                       right state                                |
        // |------------------------------------------------------------------|
        // | "Ord#1", "LineOrd#1", "3 Bellevue Drive, Pottstown, PA 19464"    |
        // | "Ord#1", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"    |
        // | "Ord#10", "LineOrd#10", "14 Bellevue Drive, Pottstown, PJ 19464" |
        // +------------------------------------------------------------------+
        records =
                Arrays.asList(
                        insertRecord("Ord#1", "LineOrd#1", "AIR"),
                        updateAfterRecord("Ord#1", "LineOrd#3", "SHIP"),
                        deleteRecord("Ord#6", "LineOrd#6", "RAILWAY"));

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(RowKind.DELETE, "Ord#6", "LineOrd#6", "RAILWAY", null, null, null),
                rowOfKind(RowKind.INSERT, "Ord#1", "LineOrd#3", "SHIP", null, null, null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#1",
                        "LineOrd#1",
                        "AIR",
                        "Ord#1",
                        "LineOrd#1",
                        "3 Bellevue Drive, Pottstown, PA 19464"));
    }

    /** Special for the pair of retract and accumulate. */
    private void testLeftJoinWithUpdate() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        List<StreamRecord<RowData>> records =
                Collections.singletonList(
                        insertRecord(
                                "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmitNothing(testHarness);
        records =
                Arrays.asList(
                        insertRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateBeforeRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#2", "LineOrd#2", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.UPDATE_BEFORE,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "SHIP"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "AIR"));
    }

    private void testLeftJoinWithoutRetract() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateAfterRecord(
                                "Ord#2", "LineOrd#2", "5 Bellevue Drive, Pottstown, PC 19464"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "5 Bellevue Drive, Pottstown, PC 19464",
                        null,
                        null,
                        null));
        records =
                Arrays.asList(
                        updateAfterRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateAfterRecord("Ord#2", "LineOrd#2", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "5 Bellevue Drive, Pottstown, PC 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "5 Bellevue Drive, Pottstown, PC 19464",
                        "Ord#2",
                        "LineOrd#2",
                        "AIR"));
    }

    private void testLeftJoinWithoutAcc() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        updateBeforeRecord(
                                "Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        deleteRecord(
                                "Ord#2", "LineOrd#2", "5 Bellevue Drive, Pottstown, PC 19464"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#2",
                        "LineOrd#2",
                        "5 Bellevue Drive, Pottstown, PC 19464",
                        null,
                        null,
                        null));
        records =
                Arrays.asList(
                        deleteRecord("Ord#2", "LineOrd#2", "SHIP"),
                        updateBeforeRecord("Ord#2", "LineOrd#2", "AIR"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmitNothing(testHarness);
    }

    private void testLeftJoinWithUpdateRecordsMultipleCases() throws Exception {
        // joinKey is LineOrd and uniqueKey is Ord
        List<StreamRecord<RowData>> records =
                Arrays.asList(
                        insertRecord("Ord#2", "LineOrd#2", "4 Bellevue Drive, Pottstown, PB 19464"),
                        updateAfterRecord(
                                "Ord#5", "LineOrd#5", "7 Bellevue Drive, Pottstown, PE 19464"),
                        insertRecord("Ord#0", "LineOrd#4", "5 Bellevue Drive, Pottstown, PB 19464"),
                        updateAfterRecord(
                                "Ord#4", "LineOrd#0", "6 Bellevue Drive, Pottstown, PB 19464"));

        for (StreamRecord<RowData> row : records) {
            testHarness.processElement1(row);
        }

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#4",
                        "LineOrd#0",
                        "6 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#0",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#2",
                        "LineOrd#2",
                        "4 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null));

        // only +U and the joinKey changes
        records =
                Arrays.asList(
                        insertRecord("Ord#6", "LineOrd#4", "AIR"),
                        // +I 6 joinKey=4
                        updateAfterRecord("Ord#6", "LineOrd#4", "AIR"),
                        // +U 6 joinKey=4
                        updateAfterRecord("Ord#6", "LineOrd#5", "AIR"),
                        // +U 6 joinKey=5  not expected record
                        updateAfterRecord("Ord#6", "LineOrd#4", "TRUCK")
                        // +U 6 joinKey=4
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#0",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PB 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#0",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#6",
                        "LineOrd#4",
                        "TRUCK"),
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        null,
                        null,
                        null),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#5",
                        "LineOrd#5",
                        "7 Bellevue Drive, Pottstown, PE 19464",
                        "Ord#6",
                        "LineOrd#5",
                        "AIR"));

        // -D +I  update records
        records =
                Arrays.asList(
                        deleteRecord("Ord#6", "LineOrd#4", "TRUCK"),
                        // +I 6 joinKey=4
                        insertRecord("Ord#6", "LineOrd#4", "TRUCK2"),
                        // +U 6 joinKey=4
                        deleteRecord("Ord#6", "LineOrd#4", "TRUCK3"),
                        // +U 6 joinKey=5  not expected record
                        insertRecord("Ord#6", "LineOrd#4", "AIR")
                        // +U 6 joinKey=4
                        );
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }

        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "Ord#0",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#6",
                        "LineOrd#4",
                        "TRUCK"),
                rowOfKind(
                        RowKind.INSERT,
                        "Ord#0",
                        "LineOrd#4",
                        "5 Bellevue Drive, Pottstown, PB 19464",
                        "Ord#6",
                        "LineOrd#4",
                        "AIR"));

        // -U +U disOrder
        records =
                Arrays.asList(
                        insertRecord("Ord#5", "LineOrd#2", "SHIP"), // +I 5
                        updateAfterRecord("Ord#5", "LineOrd#2", "TRUCK"), // +U 5 disorder
                        updateBeforeRecord("Ord#5", "LineOrd#2", "SHIP"), // -U 5 disorder
                        updateAfterRecord("Ord#9", "LineOrd#7", "TRUCK"));
        for (StreamRecord<RowData> row : records) {
            testHarness.processElement2(row);
        }
        assertor.shouldEmitNothing(testHarness);
    }

    @Override
    public MiniBatchStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
        RowDataKeySelector[] keySelectors = ukSelectorExtractor.apply(testInfo.getDisplayName());
        leftUniqueKeySelector = keySelectors[0];
        rightUniqueKeySelector = keySelectors[1];
        JoinInputSideSpec[] inputSideSpecs = inputSpecExtractor.apply(testInfo.getDisplayName());
        Boolean[] isOuter = joinTypeExtractor.apply(testInfo.getDisplayName());
        FlinkJoinType joinType = flinkJoinTypeExtractor.apply(testInfo.getDisplayName());
        int batchSize = miniBatchSizeExtractor.apply(testInfo.getTags());
        Long[] ttl = STATE_RETENTION_TIME_EXTRACTOR.apply(testInfo.getTags());

        return MiniBatchStreamingJoinOperator.newMiniBatchStreamJoinOperator(
                joinType,
                leftTypeInfo,
                rightTypeInfo,
                joinCondition,
                inputSideSpecs[0],
                inputSideSpecs[1],
                isOuter[0],
                isOuter[1],
                new boolean[] {true},
                ttl[0],
                ttl[0],
                new CountCoBundleTrigger<>(batchSize));
    }

    @Override
    public RowType getOutputType() {
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

    private final Function<String, JoinInputSideSpec[]> inputSpecExtractor =
            (testDisplayName) -> {
                if (testDisplayName.contains("JoinKeyContainsUniqueKey")) {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                leftTypeInfo, leftUniqueKeySelector),
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                rightTypeInfo, rightUniqueKeySelector)
                    };
                } else if (testDisplayName.contains("HasUniqueKey")) {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withUniqueKey(leftTypeInfo, leftUniqueKeySelector),
                        JoinInputSideSpec.withUniqueKey(rightTypeInfo, rightUniqueKeySelector)
                    };
                } else {
                    return new JoinInputSideSpec[] {
                        JoinInputSideSpec.withoutUniqueKey(), JoinInputSideSpec.withoutUniqueKey()
                    };
                }
            };

    private final Function<String, RowDataKeySelector[]> ukSelectorExtractor =
            (testDisplayName) -> {
                if (testDisplayName.contains("JoinKeyContainsUniqueKey")) {
                    return new RowDataKeySelector[] {
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {1},
                                leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0])),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {1},
                                rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]))
                    };
                } else if (testDisplayName.contains("HasUniqueKey")) {
                    return new RowDataKeySelector[] {
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {0},
                                leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0])),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {0},
                                rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]))
                    };
                } else {
                    return new RowDataKeySelector[] {null, null};
                }
            };

    private final Function<Set<String>, Integer> miniBatchSizeExtractor =
            (tags) -> {
                int size = 5;
                if (tags.isEmpty()) {
                    return size; // default
                }
                for (String tag : tags) {
                    String[] splits = tag.split("=");
                    int value = Integer.parseInt(splits[1].trim());
                    if (splits[0].trim().startsWith("miniBatchSize")) {
                        size = value;
                        break;
                    }
                }
                return size;
            };

    private final Function<String, Boolean[]> joinTypeExtractor =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return new Boolean[] {false, false};
                } else if (testDisplayName.contains("LeftJoin")) {
                    return new Boolean[] {true, false};
                } else if (testDisplayName.contains("RightJoin")) {
                    return new Boolean[] {false, true};
                } else {
                    return new Boolean[] {true, true};
                }
            };

    private final Function<String, FlinkJoinType> flinkJoinTypeExtractor =
            (testDisplayName) -> {
                if (testDisplayName.contains("InnerJoin")) {
                    return FlinkJoinType.INNER;
                } else if (testDisplayName.contains("LeftJoin")) {
                    return FlinkJoinType.LEFT;
                } else if (testDisplayName.contains("RightJoin")) {
                    return FlinkJoinType.RIGHT;
                } else {
                    return FlinkJoinType.FULL;
                }
            };
}
