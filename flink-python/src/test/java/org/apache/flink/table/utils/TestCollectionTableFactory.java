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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Testing CollectionTableFactory that creates collection DynamicTableSource and DynamicTableSink.
 */
public class TestCollectionTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static boolean isStreaming = true;
    private static final LinkedList<Row> SOURCE_DATA = new LinkedList<>();
    private static final LinkedList<Row> DIM_DATA = new LinkedList<>();
    private static final LinkedList<Row> RESULT = new LinkedList<>();

    private static long emitIntervalMS = -1L;

    public static void initData(List<Row> sourceData, List<Row> dimData, Long emitInterval) {
        SOURCE_DATA.addAll(sourceData);
        DIM_DATA.addAll(dimData);
        emitIntervalMS = emitInterval == null ? -1L : emitInterval;
    }

    public static void reset() {
        RESULT.clear();
        SOURCE_DATA.clear();
        DIM_DATA.clear();
        emitIntervalMS = -1L;
    }

    public static CollectionTableSource getCollectionSource(
            ResolvedCatalogTable catalogTable, boolean isStreaming) {
        String parallelismProp = catalogTable.getOptions().getOrDefault("parallelism", null);
        Optional<Integer> parallelism;
        if (parallelismProp == null) {
            parallelism = Optional.empty();
        } else {
            parallelism = Optional.of(Integer.parseInt(parallelismProp));
        }
        return new CollectionTableSource(
                emitIntervalMS,
                catalogTable.getResolvedSchema().toSourceRowDataType(),
                isStreaming,
                parallelism);
    }

    public static CollectionTableSink getCollectionSink(ResolvedCatalogTable catalogTable) {
        return new CollectionTableSink(catalogTable.getResolvedSchema().toSinkRowDataType());
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return getCollectionSource(
                context.getCatalogTable(), TestCollectionTableFactory.isStreaming);
    }

    @Override
    public String factoryIdentifier() {
        return "COLLECTION";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return getCollectionSink(context.getCatalogTable());
    }

    static class CollectionTableSource implements ScanTableSource, LookupTableSource {

        private final Long emitIntervalMS;
        private final DataType rowType;
        private final boolean isStreaming;
        private final Optional<Integer> parallelism;

        public CollectionTableSource(
                Long emitIntervalMS,
                DataType rowType,
                boolean isStreaming,
                Optional<Integer> parallelism) {
            this.emitIntervalMS = emitIntervalMS;
            this.rowType = rowType;
            this.isStreaming = isStreaming;
            this.parallelism = parallelism;
        }

        @Override
        public DynamicTableSource copy() {
            return new CollectionTableSource(emitIntervalMS, rowType, isStreaming, parallelism);
        }

        @Override
        public String asSummaryString() {
            return "CollectionTableSource";
        }

        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
            int[] lookupIndices = Arrays.stream(context.getKeys()).mapToInt(k -> k[0]).toArray();
            return TableFunctionProvider.of(new TemporalTableFetcher(DIM_DATA, lookupIndices));
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            TypeInformation<RowData> type = runtimeProviderContext.createTypeInformation(rowType);
            TypeSerializer<RowData> serializer = type.createSerializer(new ExecutionConfig());
            DataStructureConverter converter =
                    runtimeProviderContext.createDataStructureConverter(rowType);
            List<RowData> rowData =
                    SOURCE_DATA.stream()
                            .map(row -> (RowData) converter.toInternal(row))
                            .collect(Collectors.toList());

            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(
                        ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                    DataStreamSource<RowData> dataStream =
                            execEnv.createInput(
                                    new TestCollectionInputFormat<>(
                                            emitIntervalMS, rowData, serializer),
                                    type);
                    parallelism.ifPresent(dataStream::setParallelism);
                    return dataStream;
                }

                @Override
                public boolean isBounded() {
                    return !isStreaming;
                }
            };
        }
    }

    static class CollectionTableSink implements DynamicTableSink {

        private final DataType outputType;

        public CollectionTableSink(DataType outputType) {
            this.outputType = outputType;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            TypeInformation<Row> typeInformation = context.createTypeInformation(outputType);
            DataStructureConverter converter = context.createDataStructureConverter(outputType);
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    return dataStream
                            .addSink(new UnsafeMemorySinkFunction(typeInformation, converter))
                            .setParallelism(1);
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new CollectionTableSink(outputType);
        }

        @Override
        public String asSummaryString() {
            return String.format("CollectionTableSink(%s)", outputType);
        }
    }

    static class TestCollectionInputFormat<T> extends CollectionInputFormat<T> {
        private final long emitIntervalMs;

        public TestCollectionInputFormat(
                long emitIntervalMs, Collection<T> dataSet, TypeSerializer<T> serializer) {
            super(dataSet, serializer);
            this.emitIntervalMs = emitIntervalMs;
        }

        @Override
        public boolean reachedEnd() throws IOException {
            if (emitIntervalMs > 0) {
                try {
                    Thread.sleep(emitIntervalMs);
                } catch (Exception ignored) {

                }
            }
            return super.reachedEnd();
        }
    }

    static class TemporalTableFetcher extends TableFunction<Row> {
        private final LinkedList<Row> dimData;
        private final int[] keyes;

        public TemporalTableFetcher(LinkedList<Row> dimData, int[] keyes) {
            this.dimData = dimData;
            this.keyes = keyes;
        }

        public void eval(Object... values) {
            for (Row data : dimData) {
                boolean matched = true;
                int idx = 0;
                while (matched && idx < keyes.length) {
                    Object dimField = data.getField(keyes[idx]);
                    Object inputField = values[idx];
                    if (dimField != null) {
                        matched = dimField.equals(inputField);
                    } else {
                        matched = inputField == null;
                    }
                    idx += 1;
                }

                if (matched) {
                    Row ret = new Row(data.getArity());
                    for (int i = 0; i < data.getArity(); i++) {
                        ret.setField(i, data.getField(i));
                    }
                    collect(ret);
                }
            }
        }
    }

    static class UnsafeMemorySinkFunction extends RichSinkFunction<RowData> {
        private TypeSerializer<Row> serializer;

        private final TypeInformation<Row> outputType;
        private final DynamicTableSink.DataStructureConverter converter;

        public UnsafeMemorySinkFunction(
                TypeInformation<Row> outputType,
                DynamicTableSink.DataStructureConverter converter) {
            this.outputType = outputType;
            this.converter = converter;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            serializer = outputType.createSerializer(new ExecutionConfig());
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            RESULT.add(serializer.copy((Row) converter.toExternal(value)));
        }
    }
}
