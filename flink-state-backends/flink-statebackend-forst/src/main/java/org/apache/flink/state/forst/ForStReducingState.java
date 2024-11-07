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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.AbstractReducingState;
import org.apache.flink.runtime.state.v2.ReducingStateDescriptor;
import org.apache.flink.util.Preconditions;

import org.forstdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The {@link AbstractReducingState} implement for ForStDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class ForStReducingState<K, N, V> extends AbstractReducingState<K, N, V>
        implements ForStInnerTable<K, N, V> {

    /** The column family which this internal value state belongs to. */
    private final ColumnFamilyHandle columnFamilyHandle;

    /** The serialized key builder which should be thread-safe. */
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** The default namespace if not set. * */
    private final N defaultNamespace;

    /** The serializer for namespace. * */
    private final ThreadLocal<TypeSerializer<N>> namespaceSerializer;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    private final ThreadLocal<DataOutputSerializer> valueSerializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    private final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    /** Whether to enable the reuse of serialized key(and namespace). */
    private final boolean enableKeyReuse;

    public ForStReducingState(
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            ReducingStateDescriptor<V> reducingStateDescriptor,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            N defaultNamespace,
            Supplier<TypeSerializer<N>> namespaceSerializerInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer) {
        super(stateRequestHandler, reducingStateDescriptor);
        this.columnFamilyHandle = columnFamily;
        this.serializedKeyBuilder = ThreadLocal.withInitial(serializedKeyBuilderInitializer);
        this.defaultNamespace = defaultNamespace;
        this.namespaceSerializer = ThreadLocal.withInitial(namespaceSerializerInitializer);
        this.valueSerializerView = ThreadLocal.withInitial(valueSerializerViewInitializer);
        this.valueDeserializerView = ThreadLocal.withInitial(valueDeserializerViewInitializer);
        // We only enable key reuse for the most common namespace across all states.
        this.enableKeyReuse =
                (defaultNamespace instanceof VoidNamespace)
                        && (namespaceSerializerInitializer.get()
                                instanceof VoidNamespaceSerializer);
    }

    @Override
    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    @Override
    public byte[] serializeKey(ContextKey<K, N> contextKey) throws IOException {
        return ForStSerializerUtils.serializeKeyAndNamespace(
                contextKey,
                serializedKeyBuilder.get(),
                defaultNamespace,
                namespaceSerializer.get(),
                enableKeyReuse);
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
    public ForStDBGetRequest<K, N, V, V> buildDBGetRequest(StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(stateRequest.getRequestType() == StateRequestType.REDUCING_GET);
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace());
        return new ForStDBSingleGetRequest<>(
                contextKey, this, (InternalStateFuture<V>) stateRequest.getFuture());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBPutRequest<K, N, V> buildDBPutRequest(StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.REDUCING_ADD
                        || stateRequest.getRequestType() == StateRequestType.CLEAR);
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace());
        V value =
                stateRequest.getRequestType() == StateRequestType.CLEAR
                        ? null // "Delete(key)" is equivalent to "Put(key, null)"
                        : (V) stateRequest.getPayload();
        return ForStDBPutRequest.of(
                contextKey, value, this, (InternalStateFuture<Void>) stateRequest.getFuture());
    }
}
