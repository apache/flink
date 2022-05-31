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

package org.apache.flink.table.legacyutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory for the testing sinks. */
public class TestCollectionTableFactory
        implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {

    private static boolean isStreaming = true;

    private static final List<Row> SOURCE_DATA = new LinkedList<>();
    private static final List<Row> DIM_DATA = new LinkedList<>();
    private static final List<Row> RESULT = new LinkedList<>();

    private long emitIntervalMS = -1L;

    @Override
    public TableSource<Row> createTableSource(Map<String, String> properties) {
        return getCollectionSource(properties, isStreaming);
    }

    @Override
    public TableSink<Row> createTableSink(Map<String, String> properties) {
        return getCollectionSink(properties);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return getCollectionSource(properties, true);
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        return getCollectionSink(properties);
    }

    private CollectionTableSource getCollectionSource(
            Map<String, String> props, boolean isStreaming) {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(props);
        final TableSchema schema = properties.getTableSchema(Schema.SCHEMA);
        final Optional<Integer> parallelism = properties.getOptionalInt("parallelism");
        return new CollectionTableSource(emitIntervalMS, schema, isStreaming, parallelism);
    }

    private CollectionTableSink getCollectionSink(Map<String, String> props) {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(props);
        final TableSchema schema = properties.getTableSchema(Schema.SCHEMA);
        return new CollectionTableSink((RowTypeInfo) schema.toRowType());
    }

    @Override
    public Map<String, String> requiredContext() {
        final HashMap<String, String> context = new HashMap<>();
        context.put(ConnectorDescriptorValidator.CONNECTOR, "COLLECTION");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return Arrays.asList("*");
    }

    private static class CollectionTableSource
            implements StreamTableSource<Row>, LookupableTableSource<Row> {
        private final long emitIntervalMs;
        private final TableSchema schema;
        private final boolean isStreaming;
        private final Optional<Integer> parallelism;
        private final TypeInformation<Row> rowType;

        private CollectionTableSource(
                long emitIntervalMs,
                TableSchema schema,
                boolean isStreaming,
                Optional<Integer> parallelism) {
            this.emitIntervalMs = emitIntervalMs;
            this.schema = schema;
            this.isStreaming = isStreaming;
            this.parallelism = parallelism;
            this.rowType = schema.toRowType();
        }

        @Override
        public boolean isBounded() {
            return !isStreaming;
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment streamEnv) {
            final DataStreamSource<Row> dataStream =
                    streamEnv.createInput(
                            new TestCollectionInputFormat<>(
                                    emitIntervalMs,
                                    SOURCE_DATA,
                                    rowType.createSerializer(new ExecutionConfig())),
                            rowType);
            if (parallelism.isPresent()) {
                dataStream.setParallelism(parallelism.get());
            }
            return dataStream;
        }

        @Override
        public TypeInformation<Row> getReturnType() {
            return rowType;
        }

        @Override
        public TableSchema getTableSchema() {
            return schema;
        }

        @Override
        public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
            final String[] schemaFieldNames = schema.getFieldNames();
            final int[] keys =
                    Arrays.stream(lookupKeys)
                            .map(
                                    k -> {
                                        for (int x = 0; x < schemaFieldNames.length; x++) {
                                            if (k.equals(schemaFieldNames[x])) {
                                                return x;
                                            }
                                        }
                                        throw new IllegalStateException();
                                    })
                            .mapToInt(i -> i)
                            .toArray();

            return new TemporalTableFetcher(DIM_DATA, keys);
        }

        @Override
        public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
            return null;
        }

        @Override
        public boolean isAsyncEnabled() {
            return false;
        }
    }

    private static class CollectionTableSink implements AppendStreamTableSink<Row> {
        private final RowTypeInfo outputType;

        private CollectionTableSink(RowTypeInfo outputType) {
            this.outputType = outputType;
        }

        @Override
        public RowTypeInfo getOutputType() {
            return outputType;
        }

        @Override
        public String[] getFieldNames() {
            return outputType.getFieldNames();
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return outputType.getFieldTypes();
        }

        @Override
        public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
            return dataStream.addSink(new UnsafeMemorySinkFunction(outputType)).setParallelism(1);
        }

        @Override
        public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            return this;
        }
    }

    private static class UnsafeMemorySinkFunction extends RichSinkFunction<Row> {
        private static final long serialVersionUID = -7880686562734099699L;

        private final TypeInformation<Row> outputType;
        private TypeSerializer<Row> serializer = null;

        private UnsafeMemorySinkFunction(TypeInformation<Row> outputType) {
            this.outputType = outputType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            serializer = outputType.createSerializer(new ExecutionConfig());
        }

        @Override
        public void invoke(Row row, Context context) throws Exception {
            RESULT.add(serializer.copy(row));
        }
    }

    private static class TestCollectionInputFormat<T> extends CollectionInputFormat<T> {

        private static final long serialVersionUID = -3222731547793350189L;

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
                } catch (InterruptedException e) {
                }
            }
            return super.reachedEnd();
        }
    }

    private static class TemporalTableFetcher extends TableFunction<Row> {
        private static final long serialVersionUID = 6248306950388784015L;

        private final List<Row> dimData;
        private final int[] keys;

        private TemporalTableFetcher(List<Row> dimData, int[] keys) {
            this.dimData = dimData;
            this.keys = keys;
        }

        public void eval(Row values) {
            for (Row data : dimData) {
                boolean matched = true;
                int idx = 0;
                while (matched && idx < keys.length) {
                    final Object dimField = data.getField(keys[idx]);
                    final Object inputField = values.getField(idx);
                    matched = dimField.equals(inputField);
                    idx += 1;
                }
                if (matched) {
                    // copy the row data
                    final Row ret = new Row(data.getArity());
                    for (int x = 0; x < data.getArity(); x++) {
                        ret.setField(x, data.getField(x));
                    }
                    collect(ret);
                }
            }
        }
    }
}
