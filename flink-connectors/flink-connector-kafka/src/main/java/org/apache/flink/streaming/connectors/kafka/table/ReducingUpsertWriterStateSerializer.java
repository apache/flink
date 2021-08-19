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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.core.io.SimpleVersionedSerialization.readVersionAndDeserializeList;
import static org.apache.flink.core.io.SimpleVersionedSerialization.writeVersionAndSerializeList;
import static org.apache.flink.util.Preconditions.checkNotNull;

class ReducingUpsertWriterStateSerializer<WriterState>
        implements SimpleVersionedSerializer<ReducingUpsertWriterState<WriterState>> {

    private final TypeSerializer<RowData> rowDataTypeSerializer;
    @Nullable private final SimpleVersionedSerializer<WriterState> wrappedStateSerializer;

    ReducingUpsertWriterStateSerializer(
            TypeSerializer<RowData> rowDataTypeSerializer,
            @Nullable SimpleVersionedSerializer<WriterState> wrappedStateSerializer) {
        this.wrappedStateSerializer = wrappedStateSerializer;
        this.rowDataTypeSerializer = checkNotNull(rowDataTypeSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(ReducingUpsertWriterState<WriterState> state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            final List<WriterState> wrappedStates = state.getWrappedStates();
            if (wrappedStateSerializer != null) {
                writeVersionAndSerializeList(wrappedStateSerializer, wrappedStates, out);
            }

            final Map<RowData, Tuple2<RowData, Long>> reduceBuffer = state.getReduceBuffer();
            out.writeInt(reduceBuffer.size());
            for (final Map.Entry<RowData, Tuple2<RowData, Long>> entry : reduceBuffer.entrySet()) {
                rowDataTypeSerializer.serialize(entry.getKey(), out);
                rowDataTypeSerializer.serialize(entry.getValue().f0, out);
                out.writeLong(entry.getValue().f1);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public ReducingUpsertWriterState<WriterState> deserialize(int version, byte[] serialized)
            throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais); ) {
            List<WriterState> wrappedStates = null;
            if (wrappedStateSerializer != null) {
                wrappedStates = readVersionAndDeserializeList(wrappedStateSerializer, in);
            }

            final int reduceBufferSize = in.readInt();
            final Map<RowData, Tuple2<RowData, Long>> reduceBuffer = new HashMap<>();
            for (int ignored = 0; ignored < reduceBufferSize; ignored++) {
                final RowData key = rowDataTypeSerializer.deserialize(in);
                final RowData changed = rowDataTypeSerializer.deserialize(in);
                final long timestamp = in.readLong();
                reduceBuffer.put(key, new Tuple2<>(changed, timestamp));
            }
            return new ReducingUpsertWriterState<>(reduceBuffer, wrappedStates);
        }
    }
}
