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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;

/** Test for {@link SinkUpsertMaterializer}. */
public class SinkUpsertMaterializerTest {

    private final StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(1000);
    private final LogicalType[] types =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};
    private final RowDataSerializer serializer = new RowDataSerializer(types);
    private final RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {1}, types);
    private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(types);

    private final GeneratedRecordEqualiser equaliser =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    private final GeneratedRecordEqualiser upsertKeyEqualiser =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestUpsertKeyEqualiser();
                }
            };

    @Test
    public void test() throws Exception {
        SinkUpsertMaterializer materializer =
                new SinkUpsertMaterializer(
                        ttlConfig, serializer, equaliser, upsertKeyEqualiser, null);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        materializer, keySelector, keySelector.getProducedType());

        testHarness.open();

        testHarness.setStateTtlProcessingTime(1);

        testHarness.processElement(insertRecord(1L, 1, "a1"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        testHarness.processElement(insertRecord(2L, 1, "a2"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2"));

        testHarness.processElement(insertRecord(3L, 1, "a3"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        testHarness.processElement(deleteRecord(2L, 1, "a2"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement(deleteRecord(3L, 1, "a3"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a1"));

        testHarness.processElement(deleteRecord(1L, 1, "a1"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, 1L, 1, "a1"));

        testHarness.processElement(insertRecord(4L, 1, "a4"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        testHarness.setStateTtlProcessingTime(1002);

        testHarness.processElement(deleteRecord(4L, 1, "a4"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.close();
    }

    @Test
    public void testInputHasUpsertKeyWithNonDeterministicColumn() throws Exception {
        SinkUpsertMaterializer materializer =
                new SinkUpsertMaterializer(
                        ttlConfig, serializer, equaliser, upsertKeyEqualiser, new int[] {0});
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        materializer, keySelector, keySelector.getProducedType());

        testHarness.open();

        testHarness.setStateTtlProcessingTime(1);

        testHarness.processElement(insertRecord(1L, 1, "a1"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        testHarness.processElement(updateAfterRecord(1L, 1, "a11"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a11"));

        testHarness.processElement(insertRecord(3L, 1, "a3"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        testHarness.processElement(deleteRecord(1L, 1, "a111"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement(deleteRecord(3L, 1, "a33"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, 3L, 1, "a33"));

        testHarness.processElement(insertRecord(4L, 1, "a4"));
        assertor.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        testHarness.setStateTtlProcessingTime(1002);

        testHarness.processElement(deleteRecord(4L, 1, "a4"));
        assertor.shouldEmitNothing(testHarness);

        testHarness.close();
    }

    private static class TestRecordEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(0) == row2.getLong(0)
                    && row1.getInt(1) == row2.getInt(1)
                    && row1.getString(2).equals(row2.getString(2));
        }
    }

    private static class TestUpsertKeyEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind() && row1.getLong(0) == row2.getLong(0);
        }
    }
}
