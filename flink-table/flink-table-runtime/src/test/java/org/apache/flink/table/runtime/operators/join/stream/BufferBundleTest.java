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
import org.apache.flink.table.runtime.operators.join.stream.bundle.BufferBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasNoUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.JoinKeyContainsUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for MiniBatch buffer only which verify the logic of folding in MiniBatch. */
public class BufferBundleTest {

    private BufferBundle<?> buffer;

    private final InternalTypeInfo<RowData> inputTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new CharType(false, 20),
                                new CharType(false, 20),
                                VarCharType.STRING_TYPE
                            },
                            new String[] {"order_id", "line_order_id", "shipping_address"}));

    private final RowDataKeySelector joinKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0},
                    inputTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    /**
     * For case of JoinKeyContainsUniqueKey : JoinKey & UniqueKey = order_id. For case of
     * InputSideHasUniqueKey : JoinKey = order_id & UniqueKey = line_order_id. For case of
     * InputSideHasNoUniqueKey : JoinKey = order_id.
     */
    private final RowDataKeySelector uniqueKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {1},
                    inputTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    private final JoinInputSideSpec inputSideHasUniqueKey =
            JoinInputSideSpec.withUniqueKey(inputTypeInfo, uniqueKeySelector);

    @AfterEach
    public void cleanup() {
        buffer.clear();
    }

    @ParameterizedTest(name = "joinKeyContainsUniqueKey: {0}")
    @ValueSource(booleans = {true, false})
    public void testAccumulateAccumulatePattern(boolean joinKeyContainsUniqueKey) throws Exception {
        buffer =
                joinKeyContainsUniqueKey
                        ? new JoinKeyContainsUniqueKeyBundle()
                        : new InputSideHasUniqueKeyBundle();
        testAccumulateWithAccumulatePatternInternal();
    }

    @ParameterizedTest(name = "joinKeyContainsUniqueKey: {0}")
    @ValueSource(booleans = {true, false})
    public void testAccumulateRetractPattern(boolean joinKeyContainsUniqueKey) throws Exception {
        buffer =
                joinKeyContainsUniqueKey
                        ? new JoinKeyContainsUniqueKeyBundle()
                        : new InputSideHasUniqueKeyBundle();
        testAccumulateWithRetractPatternInternal();
    }

    @ParameterizedTest(name = "joinKeyContainsUniqueKey: {0}")
    @ValueSource(booleans = {true, false})
    public void testRetractAccumulatePattern(boolean joinKeyContainsUniqueKey) throws Exception {
        buffer =
                joinKeyContainsUniqueKey
                        ? new JoinKeyContainsUniqueKeyBundle()
                        : new InputSideHasUniqueKeyBundle();
        testRetractWithAccumulatePatternInternal();
    }

    @ParameterizedTest(name = "joinKeyContainsUniqueKey: {0}")
    @ValueSource(booleans = {true, false})
    public void testRetractRetractPattern(boolean joinKeyContainsUniqueKey) throws Exception {
        buffer =
                joinKeyContainsUniqueKey
                        ? new JoinKeyContainsUniqueKeyBundle()
                        : new InputSideHasUniqueKeyBundle();
        testRetractWithRetractPatternInternal();
    }

    @ParameterizedTest(name = "joinKeyContainsUniqueKey: {0}")
    @ValueSource(booleans = {true, false})
    public void testPatternCombination(boolean joinKeyContainsUniqueKey) throws Exception {
        buffer =
                joinKeyContainsUniqueKey
                        ? new JoinKeyContainsUniqueKeyBundle()
                        : new InputSideHasUniqueKeyBundle();
        testPatternCombinationInternal();
    }

    @Test
    public void testInputSideHasNoUniqueKey() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |    -U/-D                 |        +U/+I               |       Clear both           |
        // |--------------------------|----------------------------|----------------------------|
        // |    +I/+U                 |        -U/-D               |       Clear both           |
        // +--------------------------+----------------------------+----------------------------+
        buffer = new InputSideHasNoUniqueKeyBundle();
        List<RowData> records =
                Stream.of(
                                deleteRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "11 Bellevue Drive, Pottstown, PI 19464"), // z
                                insertRecord(
                                        "Ord#1",
                                        "LineOrd#2",
                                        "4 Bellevue Drive, Pottstown, PB 19464"), // xx
                                updateBeforeRecord(
                                        "Ord#90",
                                        "LineOrd#90",
                                        "110 Bellevue Drive, Pottstown, PI 19464"), // yyyy
                                insertRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "5 Bellevue Drive, Pottstown, PD 19464"), // x
                                updateBeforeRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "11 Bellevue Drive, Pottstown, PI 19464"), // y
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#1",
                                        "3 Bellevue Drive, Pottstown, PA 19464"), // s
                                updateAfterRecord(
                                        "Ord#10",
                                        "LineOrd#10",
                                        "14 Bellevue Drive, Pottstown, PJ 19464"), // zz
                                insertRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "11 Bellevue Drive, Pottstown, PI 19464"), // y  -U +I
                                deleteRecord(
                                        "Ord#1",
                                        "LineOrd#2",
                                        "4 Bellevue Drive, Pottstown, PB 19464"), // xx   +I -D
                                updateAfterRecord(
                                        "Ord#90",
                                        "LineOrd#90",
                                        "110 Bellevue Drive, Pottstown, PI 19464"), // yyyy  -U +U
                                updateBeforeRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "5 Bellevue Drive, Pottstown, PD 19464"), // x  +I -U
                                deleteRecord(
                                        "Ord#3",
                                        "LineOrd#1",
                                        "3 Bellevue Drive, Pottstown, PA 19464"), // s +U -D
                                deleteRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "8 Bellevue Drive, Pottstown, PF 19464"), // yy
                                updateBeforeRecord(
                                        "Ord#10",
                                        "LineOrd#10",
                                        "14 Bellevue Drive, Pottstown, PJ 19464"), // zz +U -U
                                insertRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "8 Bellevue Drive, Pottstown, PF 19464"), // yy   -D +I
                                updateAfterRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "11 Bellevue Drive, Pottstown, PI 19464") // z -D +U
                                )
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);
        assertRecords(Collections.emptyList(), records.size());
    }

    private void testAccumulateWithAccumulatePatternInternal() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |    +I/+U                 |        +U/+I               |    Only keep the last      |
        // |                          |                            |       (+U/+I) record       |
        // +--------------------------+----------------------------+----------------------------+
        List<RowData> records =
                Stream.of(
                                insertRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                updateAfterRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                updateAfterRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);

        List<RowData> expected =
                Stream.of(
                                updateAfterRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        assertRecords(expected, records.size());
        assertJoinKeys(expected);
    }

    private void testAccumulateWithRetractPatternInternal() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |    +I/+U                 |        -U/-D               |        Clear both          |
        // +--------------------------+----------------------------+----------------------------+
        List<RowData> records =
                Stream.of(
                                insertRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#7",
                                        "LineOrd#7",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#8",
                                        "LineOrd#8",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                updateAfterRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                updateBeforeRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateBeforeRecord(
                                        "Ord#8",
                                        "LineOrd#8",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                deleteRecord(
                                        "Ord#7",
                                        "LineOrd#7",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);
        assertRecords(Collections.emptyList(), records.size());
        assertJoinKeys(Collections.emptyList());
    }

    private void testRetractWithAccumulatePatternInternal() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |         -U/-D            |        +I/+U               |        Clear both          |
        // +--------------------------+----------------------------+----------------------------+
        List<RowData> records =
                Stream.of(
                                updateBeforeRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateBeforeRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                deleteRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                updateAfterRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "6 Bellevue Drive, Pottstown, PK 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);

        List<RowData> expected =
                Stream.of(
                                updateBeforeRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                insertRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                deleteRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                deleteRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                updateAfterRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                updateBeforeRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                updateAfterRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        assertRecords(expected, records.size());
        assertJoinKeys(expected);
    }

    private void testRetractWithRetractPatternInternal() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |         -U/-D            |        -U/-D               |        keep  last          |
        // +--------------------------+----------------------------+----------------------------+
        List<RowData> records =
                Stream.of(
                                updateBeforeRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                deleteRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                updateBeforeRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "4 Bellevue Drive, Pottstown, PB 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);

        List<RowData> expected =
                Stream.of(
                                deleteRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                updateBeforeRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "4 Bellevue Drive, Pottstown, PB 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());

        assertRecords(expected, records.size());
        assertJoinKeys(expected);
    }

    private void testPatternCombinationInternal() throws Exception {
        // +--------------------------+----------------------------+----------------------------+
        // |   Before the last        |       Last record          |          Result            |
        // |--------------------------|----------------------------|----------------------------|
        // |         +I/+U            |        +U/+I               |    Only keep the last      |
        // |                          |                            |       (+U/+I) record       |
        // |--------------------------|----------------------------|----------------------------|
        // |         +I/+U            |        -U/-D               |       Clear both           |
        // |--------------------------|----------------------------|----------------------------|
        // |         -U/-D            |        -U/-D               |        keep  last          |
        // +--------------------------+----------------------------+----------------------------+
        List<RowData> records =
                Stream.of(
                                updateAfterRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                insertRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#7",
                                        "LineOrd#7",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#8",
                                        "LineOrd#8",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                updateAfterRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                updateBeforeRecord(
                                        "Ord#6",
                                        "LineOrd#6",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateAfterRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                updateBeforeRecord(
                                        "Ord#8",
                                        "LineOrd#8",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                updateBeforeRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                deleteRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#7",
                                        "LineOrd#7",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#9",
                                        "LineOrd#9",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                updateBeforeRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                deleteRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                updateAfterRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                updateBeforeRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                insertRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                insertRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateAfterRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                deleteRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                deleteRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                updateBeforeRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "4 Bellevue Drive, Pottstown, PB 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        addRecordsToBuffer(records);
        List<RowData> result =
                Stream.of(
                                updateAfterRecord(
                                        "Ord#1",
                                        "LineOrd#1",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                updateBeforeRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "7 Bellevue Drive, Pottstown, PL 19464"),
                                insertRecord(
                                        "Ord#14",
                                        "LineOrd#14",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                deleteRecord(
                                        "Ord#18",
                                        "LineOrd#18",
                                        "3 Bellevue Drive, Pottstown, PA 19464"),
                                updateAfterRecord(
                                        "Ord#3",
                                        "LineOrd#3",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                deleteRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "9 Bellevue Drive, Pottstown, PJyy 19464"),
                                insertRecord(
                                        "Ord#16",
                                        "LineOrd#16",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                deleteRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"),
                                updateAfterRecord(
                                        "Ord#17",
                                        "LineOrd#17",
                                        "6 Bellevue Drive, Pottstown, PK 19464"),
                                insertRecord(
                                        "Ord#2",
                                        "LineOrd#2",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                updateBeforeRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "8 Bellevue Drive, Pottstown, PCxx 19464"),
                                updateAfterRecord(
                                        "Ord#15",
                                        "LineOrd#15",
                                        "5 Bellevue Drive, Pottstown, PJ 19464"),
                                updateBeforeRecord(
                                        "Ord#19",
                                        "LineOrd#19",
                                        "4 Bellevue Drive, Pottstown, PB 19464"),
                                insertRecord(
                                        "Ord#4",
                                        "LineOrd#4",
                                        "10 Bellevue Drive, Pottstown, PJyy 19464"))
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList());
        assertRecords(result, records.size());
        assertJoinKeys(result);
    }

    private void assertRecords(List<RowData> expectedRecords, int inputSize) throws Exception {
        List<RowData> actual =
                buffer.getRecords().values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        Assertions.assertEquals(buffer.reducedSize(), inputSize - expectedRecords.size());
        Assertions.assertTrue(CollectionUtils.isEqualCollection(actual, expectedRecords));
    }

    private void assertJoinKeys(List<RowData> expectedRecords) throws Exception {
        if (buffer instanceof InputSideHasUniqueKeyBundle) {
            Map<RowData, Map<RowData, List<RowData>>> map = new HashMap<>();

            for (RowData record : expectedRecords) {
                RowData joinKey = joinKeySelector.getKey(record);
                RowData uniqueKey = uniqueKeySelector.getKey(record);
                map.computeIfAbsent(joinKey, k -> new HashMap<>())
                        .computeIfAbsent(uniqueKey, k -> new ArrayList<>())
                        .add(record);
            }
            for (RowData joinKey : buffer.getJoinKeys()) {
                Assertions.assertEquals(buffer.getRecordsWithJoinKey(joinKey), map.get(joinKey));
            }
        }
    }

    private void addRecordsToBuffer(List<RowData> input) throws Exception {
        for (RowData record : input) {
            RowData joinKey = joinKeySelector.getKey(record);
            RowData uniqueKey = null;
            if (buffer instanceof InputSideHasUniqueKeyBundle) {
                assertNotNull(inputSideHasUniqueKey.getUniqueKeySelector());
                uniqueKey = inputSideHasUniqueKey.getUniqueKeySelector().getKey(record);
            }
            buffer.addRecord(joinKey, uniqueKey, record);
        }
    }
}
