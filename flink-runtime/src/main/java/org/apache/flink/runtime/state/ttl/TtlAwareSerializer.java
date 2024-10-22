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
import java.util.Objects;

/**
 * This class wraps a {@link TypeSerializer} with ttl awareness. It will return true when the
 * wrapped {@link TypeSerializer} is instance of {@link TtlStateFactory.TtlSerializer}. Also, it
 * wraps the value migration process between TtlSerializer and non-ttl typeSerializer.
 */
public class TtlAwareSerializer<T> extends TypeSerializer<T> {

    private final boolean isTtlEnabled;

    private final TypeSerializer<T> typeSerializer;

    public TtlAwareSerializer(TypeSerializer<T> typeSerializer) {
        this.typeSerializer = typeSerializer;
        this.isTtlEnabled = typeSerializer instanceof TtlStateFactory.TtlSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return typeSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new TtlAwareSerializer<>(typeSerializer.duplicate());
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
        TtlAwareSerializer<?> that = (TtlAwareSerializer<?>) o;
        return isTtlEnabled == that.isTtlEnabled
                && Objects.equals(typeSerializer, that.typeSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isTtlEnabled, typeSerializer);
    }

    @SuppressWarnings("unchecked")
    public void migrateValueFromPriorSerializer(
            TtlAwareSerializer<T> priorTtlAwareSerializer,
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

    public TypeSerializer<T> getOriginalTypeSerializer() {
        return typeSerializer;
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new TtlAwareSerializerSnapshot<>(
                typeSerializer.snapshotConfiguration(), isTtlEnabled);
    }

    public static boolean isSerializerTtlEnabled(TypeSerializer<?> typeSerializer) {
        TypeSerializer<?> wrappedTypeSerializer = wrapTtlAwareSerializer(typeSerializer);
        boolean ttlSerializer =
                wrappedTypeSerializer instanceof TtlAwareSerializer
                        && ((TtlAwareSerializer<?>) wrappedTypeSerializer).isTtlEnabled();
        boolean ttlListSerializer =
                wrappedTypeSerializer instanceof ListSerializer
                        && ((ListSerializer<?>) wrappedTypeSerializer).getElementSerializer()
                                instanceof TtlAwareSerializer
                        && ((TtlAwareSerializer<?>)
                                        ((ListSerializer<?>) wrappedTypeSerializer)
                                                .getElementSerializer())
                                .isTtlEnabled();
        boolean ttlMapSerializer =
                wrappedTypeSerializer instanceof MapSerializer
                        && ((MapSerializer<?, ?>) wrappedTypeSerializer).getValueSerializer()
                                instanceof TtlAwareSerializer
                        && ((TtlAwareSerializer<?>)
                                        ((MapSerializer<?, ?>) wrappedTypeSerializer)
                                                .getValueSerializer())
                                .isTtlEnabled();
        return ttlSerializer || ttlListSerializer || ttlMapSerializer;
    }

    public static TypeSerializer<?> wrapTtlAwareSerializer(TypeSerializer<?> typeSerializer) {
        if (typeSerializer instanceof TtlAwareSerializer) {
            return typeSerializer;
        }

        if (typeSerializer instanceof ListSerializer) {
            return ((ListSerializer<?>) typeSerializer).getElementSerializer()
                            instanceof TtlAwareSerializer
                    ? typeSerializer
                    : new ListSerializer<>(
                            new TtlAwareSerializer<>(
                                    ((ListSerializer<?>) typeSerializer).getElementSerializer()));
        }

        if (typeSerializer instanceof MapSerializer) {
            return ((MapSerializer<?, ?>) typeSerializer).getValueSerializer()
                            instanceof TtlAwareSerializer
                    ? typeSerializer
                    : new MapSerializer<>(
                            ((MapSerializer<?, ?>) typeSerializer).getKeySerializer(),
                            new TtlAwareSerializer<>(
                                    ((MapSerializer<?, ?>) typeSerializer).getValueSerializer()));
        }

        return new TtlAwareSerializer<>(typeSerializer);
    }
}
