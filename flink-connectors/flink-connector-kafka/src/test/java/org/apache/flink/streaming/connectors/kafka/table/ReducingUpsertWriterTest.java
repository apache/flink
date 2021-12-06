/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
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

import java.io.IOException;
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

/** Tests for {@link ReducingUpsertWriter}. */
@RunWith(Parameterized.class)
public class ReducingUpsertWriterTest {
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
                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z"))),
        GenericRowData.ofKind(
                DELETE,
                1005,
                StringData.fromString("A Teaspoon of Java 1.8"),
                StringData.fromString("Kevin Jones"),
                null,
                1010,
                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))
    };

    private final boolean enableObjectReuse;

    public ReducingUpsertWriterTest(boolean enableObjectReuse) {
        this.enableObjectReuse = enableObjectReuse;
    }

    @Test
    public void testWriteData() throws Exception {
        final MockedSinkWriter writer = new MockedSinkWriter();
        final ReducingUpsertWriter<?> bufferedWriter = createBufferedWriter(writer);

        // write 4 records which doesn't trigger batch size
        writeData(bufferedWriter, new ReusableIterator(0, 4));
        assertTrue(writer.rowDataCollectors.isEmpty());

        // write one more record, and should flush the buffer
        writeData(bufferedWriter, new ReusableIterator(7, 1));

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

        expected.put(
                1005,
                Collections.singletonList(
                        GenericRowData.ofKind(
                                DELETE,
                                1005,
                                StringData.fromString("A Teaspoon of Java 1.8"),
                                StringData.fromString("Kevin Jones"),
                                null,
                                1010,
                                TimestampData.fromInstant(Instant.parse("2021-03-30T21:00:00Z")))));

        compareCompactedResult(expected, writer.rowDataCollectors);

        writer.rowDataCollectors.clear();
        // write remaining data, and they are still buffered
        writeData(bufferedWriter, new ReusableIterator(4, 3));
        assertTrue(writer.rowDataCollectors.isEmpty());
    }

    @Test
    public void testFlushDataWhenCheckpointing() throws Exception {
        final MockedSinkWriter writer = new MockedSinkWriter();
        final ReducingUpsertWriter<?> bufferedWriter = createBufferedWriter(writer);
        // write all data, there should be 3 records are still buffered
        writeData(bufferedWriter, new ReusableIterator(0, 4));
        // snapshot should flush the buffer
        bufferedWriter.prepareCommit(true);

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
                                TimestampData.fromInstant(Instant.parse("2021-03-30T18:00:00Z")))));

        compareCompactedResult(expected, writer.rowDataCollectors);
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

    private void writeData(ReducingUpsertWriter<?> writer, Iterator<RowData> iterator)
            throws Exception {
        while (iterator.hasNext()) {
            RowData next = iterator.next();
            long rowtime = next.getTimestamp(TIMESTAMP_INDICES, 3).getMillisecond();
            writer.write(
                    next,
                    new SinkWriter.Context() {
                        @Override
                        public long currentWatermark() {
                            throw new UnsupportedOperationException("Not implemented.");
                        }

                        @Override
                        public Long timestamp() {
                            return rowtime;
                        }
                    });
        }
    }

    @SuppressWarnings("unchecked")
    private ReducingUpsertWriter<?> createBufferedWriter(MockedSinkWriter sinkWriter) {
        TypeInformation<RowData> typeInformation =
                (TypeInformation<RowData>)
                        new SinkRuntimeProviderContext(false)
                                .createTypeInformation(SCHEMA.toPhysicalRowDataType());
        return new ReducingUpsertWriter<>(
                sinkWriter,
                SCHEMA.toPhysicalRowDataType(),
                new int[] {keyIndices},
                BUFFER_FLUSH_MODE,
                new Sink.ProcessingTimeService() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return 0;
                    }

                    @Override
                    public void registerProcessingTimer(
                            long time, ProcessingTimeCallback processingTimerCallback) {}
                },
                enableObjectReuse
                        ? typeInformation.createSerializer(new ExecutionConfig())::copy
                        : r -> r);
    }

    private static class MockedSinkWriter implements SinkWriter<RowData, Void, Void> {

        transient List<RowData> rowDataCollectors;

        MockedSinkWriter() {
            rowDataCollectors = new ArrayList<>();
        }

        @Override
        public void write(RowData element, Context context)
                throws IOException, InterruptedException {
            assertEquals(
                    element.getTimestamp(TIMESTAMP_INDICES, 3).toInstant(),
                    Instant.ofEpochMilli(context.timestamp()));
            rowDataCollectors.add(element);
        }

        @Override
        public List<Void> prepareCommit(boolean flush) throws IOException, InterruptedException {
            return null;
        }

        @Override
        public List<Void> snapshotState() throws IOException {
            return null;
        }

        @Override
        public void close() throws Exception {}
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
