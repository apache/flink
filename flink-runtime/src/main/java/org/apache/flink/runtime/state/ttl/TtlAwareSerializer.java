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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This class wraps a {@link TypeSerializer} with ttl awareness. It will return true when the
 * wrapped {@link TypeSerializer} is instance of {@link TtlStateFactory.TtlSerializer}. Also, it
 * wraps the value migration process between TtlSerializer and non-ttl typeSerializer.
 *
 * @param <T> The data type that the serializer serializes.
 * @param <S> The original serializer the TtlAwareSerializer wraps.
 */
public class TtlAwareSerializer<T, S extends TypeSerializer<T>> extends TypeSerializer<T> {

    private final boolean isTtlEnabled;

    private final S typeSerializer;

    public TtlAwareSerializer(S typeSerializer) {
        checkArgument(
                !(typeSerializer instanceof TtlAwareSerializer),
                typeSerializer
                        + " is already instance of TtlAwareSerializer, should not be wrapped repeatedly.");
        this.typeSerializer = typeSerializer;
        this.isTtlEnabled = TtlStateFactory.TtlSerializer.isTtlStateSerializer(typeSerializer);
    }

    public TtlAwareSerializer(S typeSerializer, boolean isTtlEnabled) {
        checkArgument(
                !(typeSerializer instanceof TtlAwareSerializer),
                typeSerializer
                        + " is already instance of TtlAwareSerializer, should not be wrapped repeatedly.");
        this.typeSerializer = typeSerializer;
        this.isTtlEnabled = isTtlEnabled;
    }

    @Override
    public boolean isImmutableType() {
        return typeSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new TtlAwareSerializer<>(typeSerializer.duplicate(), isTtlEnabled);
    }

    @Override
    public T createInstance() {
        return typeSerializer.createInstance();
    }

    @Override
    public T copy(T from) {
        return typeSerializer.copy(from);
    }

    @Override
    public T copy(T from, T reuse) {
        return typeSerializer.copy(from, reuse);
    }

    @Override
    public int getLength() {
        return typeSerializer.getLength();
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        typeSerializer.serialize(record, target);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return typeSerializer.deserialize(source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return typeSerializer.deserialize(reuse, source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TtlAwareSerializer<?, ?> that = (TtlAwareSerializer<?, ?>) o;
        return isTtlEnabled == that.isTtlEnabled
                && Objects.equals(typeSerializer, that.typeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTtlEnabled, typeSerializer);
    }

    @SuppressWarnings("unchecked")
    public void migrateValueFromPriorSerializer(
            TtlAwareSerializer<T, ?> priorTtlAwareSerializer,
            SupplierWithException<T, IOException> inputSupplier,
            DataOutputView target,
            TtlTimeProvider ttlTimeProvider)
            throws IOException {
        T outputRecord;
        if (this.isTtlEnabled()) {
            outputRecord =
                    priorTtlAwareSerializer.isTtlEnabled
                            ? inputSupplier.get()
                            : (T)
                                    new TtlValue<>(
                                            inputSupplier.get(),
                                            ttlTimeProvider.currentTimestamp());
        } else {
            outputRecord =
                    priorTtlAwareSerializer.isTtlEnabled
                            ? ((TtlValue<T>) inputSupplier.get()).getUserValue()
                            : inputSupplier.get();
        }
        this.serialize(outputRecord, target);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        typeSerializer.copy(source, target);
    }

    public boolean isTtlEnabled() {
        return isTtlEnabled;
    }

    public S getOriginalTypeSerializer() {
        return typeSerializer;
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new TtlAwareSerializerSnapshotWrapper<>(typeSerializer.snapshotConfiguration())
                .getTtlAwareSerializerSnapshot();
    }

    public static boolean isSerializerTtlEnabled(TypeSerializer<?> typeSerializer) {
        return wrapTtlAwareSerializer(typeSerializer).isTtlEnabled();
    }

    public static boolean needTtlStateMigration(
            TypeSerializer<?> previousSerializer, TypeSerializer<?> newSerializer) {
        return TtlAwareSerializer.isSerializerTtlEnabled(previousSerializer)
                != TtlAwareSerializer.isSerializerTtlEnabled(newSerializer);
    }

    public static TtlAwareSerializer<?, ?> wrapTtlAwareSerializer(
            TypeSerializer<?> typeSerializer) {
        if (typeSerializer instanceof TtlAwareSerializer) {
            return (TtlAwareSerializer<?, ?>) typeSerializer;
        }

        if (typeSerializer instanceof ListSerializer) {
            return new TtlAwareListSerializer<>((ListSerializer<?>) typeSerializer);
        }

        if (typeSerializer instanceof MapSerializer) {
            return new TtlAwareMapSerializer<>((MapSerializer<?, ?>) typeSerializer);
        }

        return new TtlAwareSerializer<>(typeSerializer);
    }

    /**
     * The list version of {@link TtlAwareSerializer}.
     *
     * @param <T>
     */
    public static class TtlAwareListSerializer<T>
            extends TtlAwareSerializer<List<T>, ListSerializer<T>> {

        public TtlAwareListSerializer(ListSerializer<T> typeSerializer) {
            super(typeSerializer);
        }

        // ------------------------------------------------------------------------
        //  ListSerializer specific properties
        // ------------------------------------------------------------------------

        /**
         * Gets the serializer for the elements of the list.
         *
         * @return The serializer for the elements of the list
         */
        @SuppressWarnings("unchecked")
        public TtlAwareSerializer<T, TypeSerializer<T>> getElementSerializer() {
            return (TtlAwareSerializer<T, TypeSerializer<T>>)
                    TtlAwareSerializer.wrapTtlAwareSerializer(
                            getOriginalTypeSerializer().getElementSerializer());
        }
    }

    /** The map version of {@link TtlAwareSerializer}. */
    public static class TtlAwareMapSerializer<K, V>
            extends TtlAwareSerializer<Map<K, V>, MapSerializer<K, V>> {

        public TtlAwareMapSerializer(MapSerializer<K, V> typeSerializer) {
            super(typeSerializer);
        }

        // ------------------------------------------------------------------------
        //  MapSerializer specific properties
        // ------------------------------------------------------------------------

        @SuppressWarnings("unchecked")
        public TtlAwareSerializer<K, TypeSerializer<K>> getKeySerializer() {
            return (TtlAwareSerializer<K, TypeSerializer<K>>)
                    TtlAwareSerializer.wrapTtlAwareSerializer(
                            getOriginalTypeSerializer().getKeySerializer());
        }

        @SuppressWarnings("unchecked")
        public TtlAwareSerializer<V, TypeSerializer<V>> getValueSerializer() {
            return (TtlAwareSerializer<V, TypeSerializer<V>>)
                    TtlAwareSerializer.wrapTtlAwareSerializer(
                            getOriginalTypeSerializer().getValueSerializer());
        }
    }
}
