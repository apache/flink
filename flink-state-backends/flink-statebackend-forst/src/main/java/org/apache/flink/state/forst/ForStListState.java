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

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateIterator;
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
import org.apache.flink.runtime.state.v2.AbstractListState;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.util.Preconditions;

import org.forstdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * The {@link AbstractListState} implement for ForStDB.
 *
 * <p>{@link ForStStateBackend} must ensure that we set the {@link org.forstdb.StringAppendOperator}
 * on the column family that we use for our state since we use the {@code merge()} call.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class ForStListState<K, N, V> extends AbstractListState<K, N, V>
        implements ListState<V>, ForStInnerTable<K, N, List<V>> {

    /** The column family which this internal value state belongs to. */
    private final ColumnFamilyHandle columnFamilyHandle;

    /** The serialized key builder which should be thread-safe. */
    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** The default namespace if not set. * */
    private final N defaultNamespace;

    private final ThreadLocal<TypeSerializer<N>> namespaceSerializer;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    private final ThreadLocal<DataOutputSerializer> valueSerializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    private final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    /** Whether to enable the reuse of serialized key(and namespace). */
    private final boolean enableKeyReuse;

    public ForStListState(
            StateRequestHandler stateRequestHandler,
            ColumnFamilyHandle columnFamily,
            ListStateDescriptor<V> listStateDescriptor,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilderInitializer,
            N defaultNamespace,
            Supplier<TypeSerializer<N>> namespaceSerializerInitializer,
            Supplier<DataOutputSerializer> valueSerializerViewInitializer,
            Supplier<DataInputDeserializer> valueDeserializerViewInitializer) {
        super(stateRequestHandler, listStateDescriptor);
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
    public byte[] serializeValue(List<V> valueList) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        return ListDelimitedSerializer.serializeList(valueList, getValueSerializer(), outputView);
    }

    @Override
    public List<V> deserializeValue(byte[] valueBytes) throws IOException {
        DataInputDeserializer inputView = valueDeserializerView.get();
        inputView.setBuffer(valueBytes);
        return ListDelimitedSerializer.deserializeList(valueBytes, getValueSerializer(), inputView);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBGetRequest<K, N, List<V>, StateIterator<V>> buildDBGetRequest(
            StateRequest<?, ?, ?, ?> stateRequest) {
        Preconditions.checkArgument(stateRequest.getRequestType() == StateRequestType.LIST_GET);
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace());
        return new ForStDBListGetRequest<>(
                contextKey, this, (InternalStateFuture<StateIterator<V>>) stateRequest.getFuture());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ForStDBPutRequest<K, N, List<V>> buildDBPutRequest(
            StateRequest<?, ?, ?, ?> stateRequest) {
        ContextKey<K, N> contextKey =
                new ContextKey<>(
                        (RecordContext<K>) stateRequest.getRecordContext(),
                        (N) stateRequest.getNamespace());
        List<V> value;
        boolean merge = false;
        switch (stateRequest.getRequestType()) {
            case CLEAR:
                value = null;
                // "Delete(key)" is equivalent to "Put(key, null)"
                break;
            case LIST_UPDATE:
                value = (List<V>) stateRequest.getPayload();
                break;
            case LIST_ADD:
                value = Collections.singletonList((V) stateRequest.getPayload());
                merge = true;
                break;
            case LIST_ADD_ALL:
                value = (List<V>) stateRequest.getPayload();
                merge = true;
                break;
            default:
                throw new IllegalArgumentException();
        }
        if (merge) {
            return ForStDBPutRequest.ofMerge(
                    contextKey, value, this, (InternalStateFuture<Void>) stateRequest.getFuture());
        } else {
            return ForStDBPutRequest.of(
                    contextKey, value, this, (InternalStateFuture<Void>) stateRequest.getFuture());
        }
    }
}
