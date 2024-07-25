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

import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.v2.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.v2.InternalAggregatingState;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The implementation of {@link AggregatingState} for ForStDB.
 *
 * @param <K> type of key
 * @param <IN> type of input
 * @param <ACC> type of aggregate state
 * @param <OUT> type of output
 */
public class ForStAggregatingState<K, N, IN, ACC, OUT>
        extends InternalAggregatingState<K, N, IN, ACC, OUT>
        implements AggregatingState<IN, OUT>, ForStInnerTable<K, N, ACC> {

    private final ColumnFamilyHandle columnFamilyHandle;
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;
    private final ThreadLocal<DataOutputSerializer> valueSerializerView;
    private final ThreadLocal<DataInputDeserializer> valueDeserializerView;
    private final ThreadLocal<TypeSerializer<N>> namespaceSerializer;
    /* Creates a new InternalKeyedState with the given asyncExecutionController and stateDescriptor.
     *
     * @param stateRequestHandler The async request handler for handling all requests.
     * @param stateDescriptor     The properties of the state.
     */
    public ForStAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateDescriptor,
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            Supplier<TypeSerializer<N>> namespaceSerializerInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer) {
        super(stateRequestHandler, stateDescriptor);
        this.columnFamilyHandle = columnFamily;
        this.serializedKeyBuilder = ThreadLocal.withInitial(serializedKeyBuilderInitializer);
        this.namespaceSerializer = ThreadLocal.withInitial(namespaceSerializerInitializer);
        this.valueDeserializerView = ThreadLocal.withInitial(valueDeserializerViewInitializer);
        this.valueSerializerView = ThreadLocal.withInitial(valueSerializerViewInitializer);
    }

    @Override
    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    @Override
    public byte[] serializeKey(ContextKey<K, N> key) throws IOException {
        return key.getOrCreateSerializedKey(
                ctxKey -> {
                    SerializedCompositeKeyBuilder<K> builder = serializedKeyBuilder.get();
                    builder.setKeyAndKeyGroup(ctxKey.getRawKey(), ctxKey.getKeyGroup());
                    return builder.buildCompositeKeyNamespace(
                            key.getNamespace(this), namespaceSerializer.get());
                });
    }

    @Override
    public byte[] serializeValue(ACC value) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        getValueSerializer().serialize(value, outputView);
        return outputView.getCopyOfBuffer();
    }

    @Override
    public ACC deserializeValue(byte[] value) throws IOException {
        DataInputDeserializer inputView = valueDeserializerView.get();
        inputView.setBuffer(value);
        return getValueSerializer().deserialize(inputView);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBGetRequest<K, N, ACC> buildDBGetRequest(StateRequest<?, ?, ?> stateRequest) {
        ContextKey<K, N> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext());
        return ForStDBGetRequest.of(
                contextKey, this, (InternalStateFuture<ACC>) stateRequest.getFuture());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBPutRequest<K, N, ACC> buildDBPutRequest(StateRequest<?, ?, ?> stateRequest) {
        ContextKey<K, N> contextKey =
                new ContextKey<>((RecordContext<K>) stateRequest.getRecordContext());
        ACC aggregate =
                (stateRequest.getRequestType() == StateRequestType.CLEAR)
                        ? null
                        : (ACC) stateRequest.getPayload();
        return ForStDBPutRequest.of(
                contextKey, aggregate, this, (InternalStateFuture<Void>) stateRequest.getFuture());
    }
}
