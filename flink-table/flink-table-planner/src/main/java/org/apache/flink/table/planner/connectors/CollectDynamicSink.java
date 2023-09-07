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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.ResultProvider;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Function;

/** Table sink for {@link TableResult#collect()}. */
@Internal
public final class CollectDynamicSink implements DynamicTableSink {

    private static final String COLLECT_TRANSFORMATION = "collect";

    private final ObjectIdentifier tableIdentifier;
    private final DataType consumedDataType;
    private final MemorySize maxBatchSize;
    private final Duration socketTimeout;
    private final ClassLoader classLoader;
    private final ZoneId sessionZoneId;
    private final boolean legacyCastBehaviour;

    // mutable attributes
    private CollectResultIterator<RowData> iterator;
    private DataStructureConverter converter;

    CollectDynamicSink(
            ObjectIdentifier tableIdentifier,
            DataType consumedDataType,
            MemorySize maxBatchSize,
            Duration socketTimeout,
            ClassLoader classLoader,
            ZoneId sessionZoneId,
            boolean legacyCastBehaviour) {
        this.tableIdentifier = tableIdentifier;
        this.consumedDataType = consumedDataType;
        this.maxBatchSize = maxBatchSize;
        this.socketTimeout = socketTimeout;
        this.classLoader = classLoader;
        this.sessionZoneId = sessionZoneId;
        this.legacyCastBehaviour = legacyCastBehaviour;
    }

    public ResultProvider getSelectResultProvider() {
        return new CollectResultProvider(
                new RowDataToStringConverterImpl(
                        consumedDataType, sessionZoneId, classLoader, legacyCastBehaviour));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> inputStream) {
                final CheckpointConfig checkpointConfig =
                        inputStream.getExecutionEnvironment().getCheckpointConfig();
                final ExecutionConfig config = inputStream.getExecutionConfig();

                final TypeSerializer<RowData> externalSerializer =
                        InternalTypeInfo.<RowData>of(consumedDataType.getLogicalType())
                                .createSerializer(config);
                final String accumulatorName = tableIdentifier.getObjectName();

                final CollectSinkOperatorFactory<RowData> factory =
                        new CollectSinkOperatorFactory<>(
                                externalSerializer, accumulatorName, maxBatchSize, socketTimeout);
                final CollectSinkOperator<RowData> operator =
                        (CollectSinkOperator<RowData>) factory.getOperator();
                final long resultFetchTimeout =
                        inputStream
                                .getExecutionEnvironment()
                                .getConfiguration()
                                .get(AkkaOptions.ASK_TIMEOUT_DURATION)
                                .toMillis();

                iterator =
                        new CollectResultIterator<>(
                                operator.getOperatorIdFuture(),
                                externalSerializer,
                                accumulatorName,
                                checkpointConfig,
                                resultFetchTimeout);
                converter = context.createDataStructureConverter(consumedDataType);
                converter.open(RuntimeConverter.Context.create(classLoader));

                final CollectStreamSink<RowData> sink =
                        new CollectStreamSink<>(inputStream, factory);
                providerContext.generateUid(COLLECT_TRANSFORMATION).ifPresent(sink::uid);
                return sink.name("Collect table sink");
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new CollectDynamicSink(
                tableIdentifier,
                consumedDataType,
                maxBatchSize,
                socketTimeout,
                classLoader,
                sessionZoneId,
                legacyCastBehaviour);
    }

    @Override
    public String asSummaryString() {
        return String.format("TableToCollect(type=%s)", consumedDataType);
    }

    private final class CollectResultProvider implements ResultProvider {

        private final RowDataToStringConverter rowDataToStringConverter;

        private CloseableRowIteratorWrapper<RowData> rowDataIterator;
        private CloseableRowIteratorWrapper<Row> rowIterator;

        private CollectResultProvider(RowDataToStringConverter rowDataToStringConverter) {
            this.rowDataToStringConverter = rowDataToStringConverter;
        }

        @Override
        public ResultProvider setJobClient(JobClient jobClient) {
            iterator.setJobClient(jobClient);
            return this;
        }

        @Override
        public CloseableIterator<RowData> toInternalIterator() {
            if (this.rowDataIterator == null) {
                this.rowDataIterator =
                        new CloseableRowIteratorWrapper<>(iterator, Function.identity());
            }
            return this.rowDataIterator;
        }

        @Override
        public CloseableIterator<Row> toExternalIterator() {
            if (this.rowIterator == null) {
                this.rowIterator =
                        new CloseableRowIteratorWrapper<>(
                                iterator, r -> (Row) converter.toExternal(r));
            }
            return this.rowIterator;
        }

        @Override
        public RowDataToStringConverter getRowDataStringConverter() {
            return rowDataToStringConverter;
        }

        @Override
        public boolean isFirstRowReady() {
            return (this.rowDataIterator != null && this.rowDataIterator.firstRowProcessed)
                    || (this.rowIterator != null && this.rowIterator.firstRowProcessed)
                    || iterator.hasNext();
        }
    }

    private static final class CloseableRowIteratorWrapper<T> implements CloseableIterator<T> {
        private final CloseableIterator<RowData> iterator;
        private final Function<RowData, T> mapper;

        private boolean firstRowProcessed = false;

        private CloseableRowIteratorWrapper(
                CloseableIterator<RowData> iterator, Function<RowData, T> mapper) {
            this.iterator = iterator;
            this.mapper = mapper;
        }

        @Override
        public void close() throws Exception {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = iterator.hasNext();
            firstRowProcessed = firstRowProcessed || hasNext;
            return hasNext;
        }

        @Override
        public T next() {
            RowData next = iterator.next();
            firstRowProcessed = true;
            return mapper.apply(next);
        }
    }
}
