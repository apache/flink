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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link BufferedUpsertSinkFunction}. */
@RunWith(Parameterized.class)
public class BufferedUpsertSinkFunctionTest {

    @Parameterized.Parameters(name = "object reuse = {0}")
    public static Object[] enableObjectReuse() {
        return new Boolean[] {true, false};
    }

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT().notNull()),
                    Column.physical("title", DataTypes.STRING().notNull()),
                    Column.physical("author", DataTypes.STRING()),
                    Column.physical("price", DataTypes.DOUBLE()),
                    Column.physical("qty", DataTypes.INT()),
                    Column.physical("ts", DataTypes.TIMESTAMP_LTZ(3)));

    private static final int keyIndices = 0;
    private static final int TIMESTAMP_INDICES = 5;
    private static final SinkBufferFlushMode BUFFER_FLUSH_MODE =
            new SinkBufferFlushMode(4, Long.MAX_VALUE);

    public static final RowData[] TEST_DATA = {
        GenericRowData.ofKind(
                INSERT,
                1001,
                StringData.fromString("Java public for dummies"),
                StringData.fromString("Tan Ah Teck"),
                11.11,
                11,
                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z"))),
        GenericRowData.ofKind(
                INSERT,
                1002,
                StringData.fromString("More Java for dummies"),
                StringData.fromString("Tan Ah Teck"),
                22.22,
                22,
                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z"))),
        GenericRowData.ofKind(
                INSERT,
                1004,
                StringData.fromString("A Cup of Java"),
                StringData.fromString("Kumar"),
                44.44,
                44,
                TimestampData.fromInstant(Instant.parse("2021-03-30T17:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java"),
                StringData.fromString("Kevin Jones"),
                55.55,
                55,
                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java 1.4"),
                StringData.fromString("Kevin Jones"),
                66.66,
                66,
                TimestampData.fromInstant(Instant.parse("2021-03-30T19:00:00Z"))),
        GenericRowData.ofKind(
                UPDATE_AFTER,
                1004,
                StringData.fromString("A Teaspoon of Java 1.5"),
                StringData.fromString("Kevin Jones"),
                77.77,
                77,
                TimestampData.fromInstant(Instant.parse("2021-03-30T20:00:00Z"))),
        GenericRowData.ofKind(
                DELETE,
                1004,
                StringData.fromString("A Teaspoon of Java 1.8"),
                StringData.fromString("Kevin Jones"),
                null,
                1010,
                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))
    };

    private final boolean enableObjectReuse;

    public BufferedUpsertSinkFunctionTest(boolean enableObjectReuse) {
        this.enableObjectReuse = enableObjectReuse;
    }

    @Test
    public void testWriteData() throws Exception {
        MockedSinkFunction sinkFunction = new MockedSinkFunction();
        BufferedUpsertSinkFunction bufferedSink = createBufferedSink(sinkFunction);

        // write 3 records which doesn't trigger batch size
        writeData(bufferedSink, new ReusableIterator(0, 3));
        assertTrue(sinkFunction.rowDataCollectors.isEmpty());

        // write one more record, and should flush the buffer
        writeData(bufferedSink, new ReusableIterator(3, 1));

        HashMap<Integer, List<RowData>> expected = new HashMap<>();
        expected.put(
                1001,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1001,
                                StringData.fromString("Java public for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                11.11,
                                11,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z")))));
        expected.put(
                1002,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1002,
                                StringData.fromString("More Java for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                22.22,
                                22,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z")))));
        expected.put(
                1004,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1004,
                                StringData.fromString("A Teaspoon of Java"),
                                StringData.fromString("Kevin Jones"),
                                55.55,
                                55,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z")))));

        compareCompactedResult(expected, sinkFunction.rowDataCollectors);

        sinkFunction.rowDataCollectors.clear();
        // write remaining data, and they are still buffered
        writeData(bufferedSink, new ReusableIterator(4, 3));
        assertTrue(sinkFunction.rowDataCollectors.isEmpty());
    }

    @Test
    public void testFlushDataWhenCheckpointing() throws Exception {
        MockedSinkFunction sinkFunction = new MockedSinkFunction();
        BufferedUpsertSinkFunction bufferedFunction = createBufferedSink(sinkFunction);
        // write all data, there should be 3 records are still buffered
        writeData(bufferedFunction, new ReusableIterator(0, TEST_DATA.length));
        // snapshot should flush the buffer
        bufferedFunction.snapshotState(null);

        HashMap<Integer, List<RowData>> expected = new HashMap<>();
        expected.put(
                1001,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1001,
                                StringData.fromString("Java public for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                11.11,
                                11,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T15:00:00Z")))));
        expected.put(
                1002,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1002,
                                StringData.fromString("More Java for dummies"),
                                StringData.fromString("Tan Ah Teck"),
                                22.22,
                                22,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T16:00:00Z")))));
        expected.put(
                1004,
                Arrays.asList(
                        GenericRowData.ofKind(
                                UPDATE_AFTER,
                                1004,
                                StringData.fromString("A Teaspoon of Java"),
                                StringData.fromString("Kevin Jones"),
                                55.55,
                                55,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z"))),
                        GenericRowData.ofKind(
                                DELETE,
                                1004,
                                StringData.fromString("A Teaspoon of Java 1.8"),
                                StringData.fromString("Kevin Jones"),
                                null,
                                1010,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))));

        compareCompactedResult(expected, sinkFunction.rowDataCollectors);
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private BufferedUpsertSinkFunction createBufferedSink(MockedSinkFunction sinkFunction)
            throws Exception {
        TypeInformation<RowData> typeInformation =
                (TypeInformation<RowData>)
                        new SinkRuntimeProviderContext(false)
                                .createTypeInformation(SCHEMA.toPhysicalRowDataType());

        BufferedUpsertSinkFunction bufferedSinkFunction =
                new BufferedUpsertSinkFunction(
                        sinkFunction,
                        SCHEMA.toPhysicalRowDataType(),
                        new int[] {keyIndices},
                        typeInformation,
                        BUFFER_FLUSH_MODE);
        bufferedSinkFunction.open(new Configuration());
        return bufferedSinkFunction;
    }

    private void writeData(BufferedUpsertSinkFunction sink, Iterator<RowData> iterator)
            throws Exception {
        while (iterator.hasNext()) {
            RowData next = iterator.next();
            long rowtime = next.getTimestamp(TIMESTAMP_INDICES, 3).getMillisecond();
            sink.invoke(next, SinkContextUtil.forTimestamp(rowtime));
        }
    }

    private void compareCompactedResult(
            Map<Integer, List<RowData>> expected, List<RowData> actual) {
        Map<Integer, List<RowData>> actualMap = new HashMap<>();

        for (RowData rowData : actual) {
            Integer id = rowData.getInt(keyIndices);
            actualMap.computeIfAbsent(id, key -> new ArrayList<>()).add(rowData);
        }

        assertEquals(expected.size(), actualMap.size());
        for (Integer id : expected.keySet()) {
            assertEquals(expected.get(id), actualMap.get(id));
        }
    }

    // --------------------------------------------------------------------------------------------

    private class MockedSinkFunction extends RichSinkFunction<RowData>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;
        private final RuntimeContext context = new MockStreamingRuntimeContext(true, 1, 1);
        transient List<RowData> rowDataCollectors;

        MockedSinkFunction() {
            if (enableObjectReuse) {
                context.getExecutionConfig().enableObjectReuse();
            }
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return context;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            rowDataCollectors = new ArrayList<>();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // do nothing
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // do nothing
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // do nothing
        }

        @Override
        public void invoke(RowData value, Context context) {
            assertEquals(
                    value.getTimestamp(TIMESTAMP_INDICES, 3).toInstant(),
                    Instant.ofEpochMilli(context.timestamp()));
            rowDataCollectors.add(value);
        }
    }

    private class ReusableIterator implements Iterator<RowData> {

        private final RowDataSerializer serializer =
                InternalTypeInfo.of(SCHEMA.toSinkRowDataType().getLogicalType()).toRowSerializer();
        private final RowData reusedRow = new GenericRowData(SCHEMA.getColumnCount());

        private int begin;
        private final int end;

        ReusableIterator(int begin, int size) {
            this.begin = begin;
            this.end = begin + size;
        }

        @Override
        public boolean hasNext() {
            return begin < end;
        }

        @Override
        public RowData next() {
            if (enableObjectReuse) {
                return serializer.copy(TEST_DATA[begin++], reusedRow);
            } else {
                return TEST_DATA[begin++];
            }
        }
    }
}
