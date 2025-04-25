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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.v2.AbstractMapState;
import org.apache.flink.util.Preconditions;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksIterator;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The {@link AbstractMapState} implement for ForStDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the user key.
 * @param <UV> The type of the user value.
 */
public class ForStMapState<K, N, UK, UV> extends AbstractMapState<K, N, UK, UV>
        implements MapState<UK, UV>, ForStInnerTable<K, N, UV> {

    /** The column family which this internal value state belongs to. */
    private final ColumnFamilyHandle columnFamilyHandle;

    /** The serialized key builder which should be thread-safe. */
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** The default namespace if not set. * */
    private final N defaultNamespace;

    private final ThreadLocal<TypeSerializer<N>> namespaceSerializer;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    final ThreadLocal<DataOutputSerializer> valueSerializerView;

    final ThreadLocal<DataInputDeserializer> keyDeserializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    /** Serializer for the user keys. */
    final ThreadLocal<TypeSerializer<UK>> userKeySerializer;

    /** Serializer for the user values. */
    final ThreadLocal<TypeSerializer<UV>> userValueSerializer;

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    public ForStMapState(
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> valueSerializer,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            N defaultNamespace,
            Supplier<TypeSerializer<N>> namespaceSerializerInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> keyDeserializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer,
            int keyGroupPrefixBytes) {
        super(stateRequestHandler, valueSerializer);
        this.columnFamilyHandle = columnFamily;
        this.serializedKeyBuilder = ThreadLocal.withInitial(serializedKeyBuilderInitializer);
        this.defaultNamespace = defaultNamespace;
        this.namespaceSerializer = ThreadLocal.withInitial(namespaceSerializerInitializer);
        this.valueSerializerView = ThreadLocal.withInitial(valueSerializerViewInitializer);
        this.keyDeserializerView = ThreadLocal.withInitial(keyDeserializerViewInitializer);
        this.valueDeserializerView = ThreadLocal.withInitial(valueDeserializerViewInitializer);
        this.userKeySerializer = ThreadLocal.withInitial(userKeySerializer::duplicate);
        this.userValueSerializer = ThreadLocal.withInitial(valueSerializer::duplicate);
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return columnFamilyHandle;
    }

    @Override
    public byte[] serializeKey(ContextKey<K, N> contextKey) throws IOException {
        SerializedCompositeKeyBuilder<K> builder = serializedKeyBuilder.get();
        builder.setKeyAndKeyGroup(contextKey.getRawKey(), contextKey.getKeyGroup());
        N namespace = contextKey.getNamespace();
        builder.setNamespace(
                namespace == null ? defaultNamespace : namespace, namespaceSerializer.get());
        if (contextKey.getUserKey() == null) { // value get
            return builder.build();
        }
        UK userKey = (UK) contextKey.getUserKey(); // map get
        return builder.buildCompositeKeyUserKey(userKey, userKeySerializer.get());
    }

    @Override
    public byte[] serializeValue(UV value) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        outputView.writeBoolean(false);
        userValueSerializer.get().serialize(value, outputView);
        return outputView.getCopyOfBuffer();
    }

    @Override
    public UV deserializeValue(byte[] valueBytes) throws IOException {
        DataInputDeserializer inputView = valueDeserializerView.get();
        inputView.setBuffer(valueBytes);
        boolean isNull = inputView.readBoolean();
        return isNull ? null : userValueSerializer.get().deserialize(inputView);
    }

    public UK deserializeUserKey(byte[] userKeyBytes, int userKeyOffset) throws IOException {
        DataInputDeserializer inputView = keyDeserializerView.get();
        inputView.setBuffer(userKeyBytes, userKeyOffset, userKeyBytes.length - userKeyOffset);
        return userKeySerializer.get().deserialize(inputView);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ForStDBGetRequest<?, ?, ?, ?> buildDBGetRequest(StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.MAP_GET
                        || stateRequest.getRequestType() == StateRequestType.MAP_CONTAINS
                        || stateRequest.getRequestType() == StateRequestType.MAP_IS_EMPTY);
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace(),
                        stateRequest.getPayload());

        if (stateRequest.getRequestType() == StateRequestType.MAP_GET) {
            return new ForStDBSingleGetRequest<>(
                    contextKey, this, (InternalStateFuture<UV>) stateRequest.getFuture());
        }
        return new ForStDBMapCheckRequest<>(
                contextKey,
                this,
                (InternalStateFuture<Boolean>) stateRequest.getFuture(),
                stateRequest.getRequestType() == StateRequestType.MAP_IS_EMPTY);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ForStDBPutRequest<K, N, UV> buildDBPutRequest(StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkNotNull(stateRequest.getPayload());
        ContextKey<K, N> contextKey;
        if (stateRequest.getRequestType() == StateRequestType.MAP_PUT) {
            contextKey =
                    new ContextKey<>(
                            (RecordContext<K>) stateRequest.getRecordContext(),
                            (N) stateRequest.getNamespace(),
                            ((Tuple2<UK, UV>) stateRequest.getPayload()).f0);
        } else if (stateRequest.getRequestType() == StateRequestType.MAP_REMOVE) {
            contextKey =
                    new ContextKey<>(
                            (RecordContext<K>) stateRequest.getRecordContext(),
                            (N) stateRequest.getNamespace(),
                            stateRequest.getPayload());
        } else {
            throw new IllegalArgumentException(
                    "The State type is: "
                            + stateRequest.getRequestType().name()
                            + ", which is not a valid put request.");
        }
        UV value = null;
        if (stateRequest.getRequestType() == StateRequestType.MAP_PUT) {
            value = ((Tuple2<UK, UV>) stateRequest.getPayload()).f1;
        }

        return ForStDBPutRequest.of(
                contextKey, value, this, (InternalStateFuture<Void>) stateRequest.getFuture());
    }

    /**
     * Build a request for bunch put. Maily used for {@link StateRequestType#MAP_PUT_ALL} and {@link
     * StateRequestType#CLEAR}.
     *
     * @param stateRequest The state request.
     * @return The {@code ForStDBBunchPutRequest}.
     */
    @SuppressWarnings("unchecked")
    public ForStDBBunchPutRequest<K, N, UK, UV> buildDBBunchPutRequest(
            StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.MAP_PUT_ALL
                        || stateRequest.getRequestType() == StateRequestType.CLEAR);
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace(),
                        null);
        Map<UK, UV> value = (Map<UK, UV>) stateRequest.getPayload();
        return new ForStDBBunchPutRequest(contextKey, value, this, stateRequest.getFuture());
    }

    /**
     * Build a request for iterator. Used for {@link StateRequestType#MAP_ITER}, {@link
     * StateRequestType#MAP_ITER_KEY}, {@link StateRequestType#MAP_ITER_VALUE} and {@link
     * StateRequestType#ITERATOR_LOADING}.
     *
     * @param stateRequest The state request.
     * @return The {@code ForStDBIterRequest}.
     */
    @SuppressWarnings("unchecked")
    public ForStDBIterRequest<K, N, UK, UV, ?> buildDBIterRequest(
            StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(
                stateRequest.getRequestType() == StateRequestType.MAP_ITER
                        || stateRequest.getRequestType() == StateRequestType.MAP_ITER_KEY
                        || stateRequest.getRequestType() == StateRequestType.MAP_ITER_VALUE
                        || stateRequest.getRequestType() == StateRequestType.ITERATOR_LOADING);
        RocksIterator rocksIterator = null;
        StateRequestType requestType = stateRequest.getRequestType();
        if (requestType == StateRequestType.ITERATOR_LOADING) {
            Tuple2<StateRequestType, RocksIterator> payload =
                    (Tuple2<StateRequestType, RocksIterator>) stateRequest.getPayload();
            requestType = payload.f0;
            rocksIterator = payload.f1;
        }
        return buildDBIterRequest(stateRequest, requestType, rocksIterator);
    }

    @SuppressWarnings("unchecked")
    private ForStDBIterRequest<K, N, UK, UV, ?> buildDBIterRequest(
            StateRequest<?, ?, ?, ?> stateRequest,
            StateRequestType requestType,
            RocksIterator rocksIterator) {
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace(),
                        null);
        switch (requestType) {
            case MAP_ITER:
                return new ForStDBMapEntryIterRequest<>(
                        contextKey,
                        this,
                        stateRequestHandler,
                        rocksIterator,
                        (InternalStateFuture<StateIterator<Map.Entry<UK, UV>>>)
                                stateRequest.getFuture());
            case MAP_ITER_KEY:
                return new ForStDBMapKeyIterRequest<>(
                        contextKey,
                        this,
                        stateRequestHandler,
                        rocksIterator,
                        (InternalStateFuture<StateIterator<UK>>) stateRequest.getFuture());
            case MAP_ITER_VALUE:
                return new ForStDBMapValueIterRequest<>(
                        contextKey,
                        this,
                        stateRequestHandler,
                        rocksIterator,
                        (InternalStateFuture<StateIterator<UV>>) stateRequest.getFuture());
            default:
                throw new IllegalArgumentException(
                        "Unknown request type: "
                                + stateRequest
                                + ", current request type: "
                                + stateRequest.getRequestType());
        }
    }

    @SuppressWarnings("unchecked")
    static <N, UK, UV, K, SV, S extends State> S create(
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> valueSerializer,
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            N defaultNamespace,
            Supplier<TypeSerializer<N>> namespaceSerializerInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> keyDeserializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer,
            int keyGroupPrefixBytes) {
        return (S)
                new ForStMapState<>(
                        stateRequestHandler,
                        columnFamily,
                        userKeySerializer,
                        valueSerializer,
                        serializedKeyBuilderInitializer,
                        defaultNamespace,
                        namespaceSerializerInitializer,
                        valueSerializerViewInitializer,
                        keyDeserializerViewInitializer,
                        valueDeserializerViewInitializer,
                        keyGroupPrefixBytes);
    }
}
