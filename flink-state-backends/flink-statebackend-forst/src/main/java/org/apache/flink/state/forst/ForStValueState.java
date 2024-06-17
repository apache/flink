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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.v2.InternalValueState;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The {@link InternalValueState} implement for ForStDB.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class ForStValueState<K, V> extends InternalValueState<K, V>
        implements ValueState<V>, ForStInnerTable<ContextKey<K>, V> {

    /** The column family which this internal value state belongs to. */
    private final ColumnFamilyHandle columnFamilyHandle;

    /** The serialized key builder which should be thread-safe. */
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    private final ThreadLocal<DataOutputSerializer> valueSerializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    private final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    public ForStValueState(
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            ValueStateDescriptor<V> valueStateDescriptor,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer) {
        super(stateRequestHandler, valueStateDescriptor);
        this.columnFamilyHandle = columnFamily;
        this.serializedKeyBuilder = ThreadLocal.withInitial(serializedKeyBuilderInitializer);
        this.valueSerializerView = ThreadLocal.withInitial(valueSerializerViewInitializer);
        this.valueDeserializerView = ThreadLocal.withInitial(valueDeserializerViewInitializer);
    }

    @Override
    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    @Override
    public byte[] serializeKey(ContextKey<K> contextKey) throws IOException {
        return contextKey.getOrCreateSerializedKey(
                ctxKey -> {
                    SerializedCompositeKeyBuilder<K> builder = serializedKeyBuilder.get();
                    builder.setKeyAndKeyGroup(ctxKey.getRawKey(), ctxKey.getKeyGroup());
                    return builder.build();
                });
    }

    @Override
    public byte[] serializeValue(V value) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        getValueSerializer().serialize(value, outputView);
        return outputView.getCopyOfBuffer();
    }

    @Override
    public V deserializeValue(byte[] valueBytes) throws IOException {
        DataInputDeserializer inputView = valueDeserializerView.get();
        inputView.setBuffer(valueBytes);
        return getValueSerializer().deserialize(inputView);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBGetRequest<ContextKey<K>, V> buildDBGetRequest(
            StateRequest<?, ?, ?> stateRequest) {
        Preconditions.checkArgument(stateRequest.getRequestType() == StateRequestType.VALUE_GET);
        ContextKey<K> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext());
        return ForStDBGetRequest.of(
                contextKey, this, (InternalStateFuture<V>) stateRequest.getFuture());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBPutRequest<ContextKey<K>, V> buildDBPutRequest(
            StateRequest<?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.VALUE_UPDATE
                        || stateRequest.getRequestType() == StateRequestType.CLEAR);
        ContextKey<K> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext());
        V value =
                (stateRequest.getRequestType() == StateRequestType.CLEAR)
                        ? null // "Delete(key)" is equivalent to "Put(key, null)"
                        : (V) stateRequest.getPayload();
        return ForStDBPutRequest.of(
                contextKey, value, this, (InternalStateFuture<Void>) stateRequest.getFuture());
    }
}
