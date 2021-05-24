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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.CollectResultProvider;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/** Table sink for {@link TableResult#collect()}. */
@Internal
final class CollectDynamicSink implements DynamicTableSink {

    private final ObjectIdentifier tableIdentifier;

    private final DataType consumedDataType;

    // mutable attributes

    private CollectResultIterator<RowData> iterator;

    CollectDynamicSink(ObjectIdentifier tableIdentifier, DataType consumedDataType) {
        this.tableIdentifier = tableIdentifier;
        this.consumedDataType = consumedDataType;
    }

    public CollectResultProvider getSelectResultProvider() {
        return new CollectResultProvider() {
            @Override
            public void setJobClient(JobClient jobClient) {
                iterator.setJobClient(jobClient);
            }

            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public CloseableIterator<Row> getResultIterator() {
                // Row after deserialization
                return (CloseableIterator) iterator;
            }
        };
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return (DataStreamSinkProvider)
                inputStream -> {
                    final CheckpointConfig checkpointConfig =
                            inputStream.getExecutionEnvironment().getCheckpointConfig();
                    final ExecutionConfig config = inputStream.getExecutionConfig();

                    final TypeSerializer<RowData> externalSerializer =
                            ExternalTypeInfo.<RowData>of(consumedDataType, true)
                                    .createSerializer(config);
                    final String accumulatorName = tableIdentifier.getObjectName();

                    final CollectSinkOperatorFactory<RowData> factory =
                            new CollectSinkOperatorFactory<>(externalSerializer, accumulatorName);
                    final CollectSinkOperator<RowData> operator =
                            (CollectSinkOperator<RowData>) factory.getOperator();

                    this.iterator =
                            new CollectResultIterator<>(
                                    operator.getOperatorIdFuture(),
                                    externalSerializer,
                                    accumulatorName,
                                    checkpointConfig);

                    final CollectStreamSink<RowData> sink =
                            new CollectStreamSink<>(inputStream, factory);
                    return sink.name("Collect table sink");
                };
    }

    @Override
    public DynamicTableSink copy() {
        final CollectDynamicSink copy = new CollectDynamicSink(tableIdentifier, consumedDataType);
        // kind of violates the contract of copy() but should not harm
        // as it is null during optimization anyway until physical translation
        copy.iterator = iterator;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return String.format("TableToCollect(type=%s)", consumedDataType);
    }
}
