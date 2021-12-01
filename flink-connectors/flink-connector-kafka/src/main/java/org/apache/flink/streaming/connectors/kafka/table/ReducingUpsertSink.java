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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.SerializableFunction;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * A wrapper of a {@link Sink}. It will buffer the data emitted by the wrapper {@link SinkWriter}
 * and only emit it when the buffer is full or a timer is triggered or a checkpoint happens.
 *
 * <p>The sink provides eventual consistency guarantees without the need of a two-phase protocol
 * because the updates are idempotent therefore duplicates have no effect.
 */
class ReducingUpsertSink<WriterState> implements Sink<RowData, Void, WriterState, Void> {

    private final Sink<RowData, ?, WriterState, ?> wrappedSink;
    private final DataType physicalDataType;
    private final int[] keyProjection;
    private final SinkBufferFlushMode bufferFlushMode;
    private final SerializableFunction<RowData, RowData> valueCopyFunction;

    ReducingUpsertSink(
            Sink<RowData, ?, WriterState, ?> wrappedSink,
            DataType physicalDataType,
            int[] keyProjection,
            SinkBufferFlushMode bufferFlushMode,
            SerializableFunction<RowData, RowData> valueCopyFunction) {
        this.wrappedSink = wrappedSink;
        this.physicalDataType = physicalDataType;
        this.keyProjection = keyProjection;
        this.bufferFlushMode = bufferFlushMode;
        this.valueCopyFunction = valueCopyFunction;
    }

    @Override
    public SinkWriter<RowData, Void, WriterState> createWriter(
            InitContext context, List<WriterState> states) throws IOException {
        final SinkWriter<RowData, ?, WriterState> wrapperWriter =
                wrappedSink.createWriter(context, states);
        return new ReducingUpsertWriter<>(
                wrapperWriter,
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public Optional<SimpleVersionedSerializer<WriterState>> getWriterStateSerializer() {
        return wrappedSink.getWriterStateSerializer();
    }

    @Override
    public Optional<Committer<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
