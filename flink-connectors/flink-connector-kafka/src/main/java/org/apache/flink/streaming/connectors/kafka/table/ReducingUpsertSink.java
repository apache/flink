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

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.SerializableFunction;

import java.io.IOException;
import java.util.Collection;

/**
 * A wrapper of a {@link Sink}. It will buffer the data emitted by the wrapper {@link SinkWriter}
 * and only emit it when the buffer is full or a timer is triggered or a checkpoint happens.
 *
 * <p>The sink provides eventual consistency guarantees without the need of a two-phase protocol
 * because the updates are idempotent therefore duplicates have no effect.
 */
class ReducingUpsertSink<WriterState> implements StatefulSink<RowData, WriterState> {

    private final StatefulSink<RowData, WriterState> wrappedSink;
    private final DataType physicalDataType;
    private final int[] keyProjection;
    private final SinkBufferFlushMode bufferFlushMode;
    private final SerializableFunction<RowData, RowData> valueCopyFunction;

    ReducingUpsertSink(
            StatefulSink<RowData, WriterState> wrappedSink,
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
    public StatefulSinkWriter<RowData, WriterState> createWriter(InitContext context)
            throws IOException {
        final StatefulSinkWriter<RowData, WriterState> wrapperWriter =
                wrappedSink.createWriter(context);
        return new ReducingUpsertWriter<>(
                wrapperWriter,
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public StatefulSinkWriter<RowData, WriterState> restoreWriter(
            InitContext context, Collection<WriterState> recoveredState) throws IOException {
        final StatefulSinkWriter<RowData, WriterState> wrapperWriter =
                wrappedSink.restoreWriter(context, recoveredState);
        return new ReducingUpsertWriter<>(
                wrapperWriter,
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public SimpleVersionedSerializer<WriterState> getWriterStateSerializer() {
        return wrappedSink.getWriterStateSerializer();
    }
}
