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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;

/** Test for {@link SinkUpsertMaterializer}. */
public class SinkUpsertMaterializerTest {

    private final StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(1000);
    private final LogicalType[] types = new LogicalType[] {new IntType(), new VarCharType()};
    private final RowDataSerializer serializer = new RowDataSerializer(types);
    private final RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(new int[0], types);
    private final GeneratedRecordEqualiser equaliser =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    @Test
    public void test() throws Exception {
        SinkUpsertMaterializer materializer =
                new SinkUpsertMaterializer(ttlConfig, serializer, equaliser);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        materializer, keySelector, keySelector.getProducedType());

        testHarness.open();

        testHarness.setStateTtlProcessingTime(1);

        testHarness.processElement(insertRecord(1, "a1"));
        Assert.assertEquals(Collections.singletonList(row(1, "a1")), toRows(testHarness));

        testHarness.processElement(insertRecord(1, "a2"));
        Assert.assertEquals(Collections.singletonList(row(1, "a2")), toRows(testHarness));

        testHarness.processElement(insertRecord(1, "a3"));
        Assert.assertEquals(Collections.singletonList(row(1, "a3")), toRows(testHarness));

        testHarness.processElement(deleteRecord(1, "a2"));
        Assert.assertEquals(Collections.emptyList(), toRows(testHarness));

        testHarness.processElement(deleteRecord(1, "a3"));
        Assert.assertEquals(Collections.singletonList(row(1, "a1")), toRows(testHarness));

        testHarness.processElement(deleteRecord(1, "a1"));
        RowData deleteRow = row(1, "a1");
        deleteRow.setRowKind(RowKind.DELETE);
        Assert.assertEquals(Collections.singletonList(deleteRow), toRows(testHarness));

        testHarness.processElement(insertRecord(1, "a4"));
        Assert.assertEquals(Collections.singletonList(row(1, "a4")), toRows(testHarness));

        testHarness.setStateTtlProcessingTime(1002);

        testHarness.processElement(deleteRecord(1, "a4"));
        Assert.assertEquals(Collections.emptyList(), toRows(testHarness));

        testHarness.close();
    }

    private List<RowData> toRows(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        Object o;
        List<RowData> ret = new ArrayList<>();
        while ((o = harness.getOutput().poll()) != null) {
            RowData value = (RowData) ((StreamRecord) o).getValue();
            GenericRowData newRow = GenericRowData.of(value.getInt(0), value.getString(1));
            newRow.setRowKind(value.getRowKind());
            ret.add(newRow);
        }
        return ret;
    }

    private static class TestRecordEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getInt(0) == row2.getInt(0) && row1.getString(1).equals(row2.getString(1));
        }
    }
}
