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

package org.apache.flink.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Testing Sinks that collects test output data for validation. */
public class TestingSinks {

    /** TestAppendingSink for testing. */
    public static class TestAppendingSink implements DynamicTableSink {
        private final DataType rowDataType;

        public TestAppendingSink(DataType rowDataType) {
            this.rowDataType = rowDataType;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            final DataStructureConverter converter =
                    context.createDataStructureConverter(rowDataType);
            return new DataStreamSinkProvider() {

                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    return dataStream.addSink(new RowSink(converter));
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new TestAppendingSink(rowDataType);
        }

        @Override
        public String asSummaryString() {
            return String.format("TestingAppendSink(%s)", DataType.getFields(rowDataType));
        }
    }

    /** RowSink for testing. */
    static class RowSink implements SinkFunction<RowData> {
        private final DynamicTableSink.DataStructureConverter converter;

        public RowSink(DynamicTableSink.DataStructureConverter converter) {
            this.converter = converter;
        }

        @Override
        public void invoke(RowData value, Context context) {
            RowKind rowKind = value.getRowKind();
            Row data = (Row) converter.toExternal(value);
            if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
                RowCollector.addValue(Tuple2.of(true, data));
            } else {
                RowCollector.addValue(Tuple2.of(false, data));
            }
        }
    }

    /** RowCollector for testing. */
    public static class RowCollector {
        private static final List<Tuple2<Boolean, Row>> SINK = new ArrayList<>();

        public static void addValue(Tuple2<Boolean, Row> value) {
            final Tuple2<Boolean, Row> copy = new Tuple2<>(value.f0, value.f1);
            synchronized (SINK) {
                SINK.add(copy);
            }
        }

        public static List<Tuple2<Boolean, Row>> getAndClearValues() {
            final List<Tuple2<Boolean, Row>> out = new ArrayList<>(SINK);
            SINK.clear();
            return out;
        }

        public static List<String> retractResults(List<Tuple2<Boolean, Row>> results) {
            Map<String, Integer> retractedResult = new HashMap<>();
            results.forEach(
                    v -> {
                        int cnt = retractedResult.getOrDefault(v.f1.toString(), 0);
                        if (v.f0) {
                            retractedResult.put(v.f1.toString(), cnt + 1);
                        } else {
                            retractedResult.put(v.f1.toString(), cnt - 1);
                        }
                    });

            if (retractedResult.entrySet().stream().allMatch(entry -> entry.getValue() < 0)) {
                throw new AssertionError(
                        "Received retracted rows which have not been " + "accumulated.");
            }
            List<String> retractedString = new ArrayList<>();
            retractedResult.forEach(
                    (k, v) -> {
                        for (int i = 0; i < v; i++) {
                            retractedString.add(k);
                        }
                    });

            return retractedString;
        }

        public static List<String> upsertResults(List<Tuple2<Boolean, Row>> results, int[] keys) {
            Map<Row, String> upsertResult = new HashMap<>();
            results.forEach(
                    v -> {
                        Row key = Row.project(v.f1, keys);
                        if (v.f0) {
                            upsertResult.put(key, v.f1.toString());
                        } else {
                            upsertResult.remove(key);
                        }
                    });
            return new ArrayList<>(upsertResult.values());
        }
    }
}
