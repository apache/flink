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

import javax.annotation.Nullable;

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

    /**
     * Snapshot of {@link #bareValueSerializer()}, computed on first use. {@link
     * #migrateValueFromPriorSerializer} runs once per migrated state value while the serializer
     * stays the same, and taking a snapshot allocates one object per nested serializer.
     */
    private transient TypeSerializerSnapshot<?> bareValueSerializerSnapshot;

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

    /**
     * Reads one state value written by {@code priorTtlAwareSerializer}, adapts it to this
     * serializer's TTL setting and value schema, and writes it to {@code target}.
     *
     * <p>The value is unwrapped to its bare form, passed through {@link
     * TypeSerializerSnapshot#migrate}, and re-wrapped. The hook returns the value unchanged unless
     * the value serializer overrides it, so a value whose schema did not change is written back
     * byte for byte.
     *
     * @param priorSerializerSnapshot the snapshot persisted with the state for {@code
     *     priorTtlAwareSerializer}, or {@code null} for a state that carries none.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void migrateValueFromPriorSerializer(
            TtlAwareSerializer<T, ?> priorTtlAwareSerializer,
            @Nullable TypeSerializerSnapshot<T> priorSerializerSnapshot,
            SupplierWithException<T, IOException> inputSupplier,
            DataOutputView target,
            TtlTimeProvider ttlTimeProvider)
            throws IOException {
        T priorValue = inputSupplier.get();
        Object bareValue =
                priorTtlAwareSerializer.wrapsTtlValue()
                        ? ((TtlValue<?>) priorValue).getUserValue()
                        : priorValue;

        TypeSerializerSnapshot newSnapshot = bareValueSerializerSnapshot();
        Object migratedValue =
                newSnapshot.migrate(
                        priorBareValueSerializerSnapshot(
                                priorTtlAwareSerializer, priorSerializerSnapshot),
                        bareValue);

        T outputRecord;
        if (this.wrapsTtlValue()) {
            // Carrying the prior timestamp over keeps the value's expiry where it was; migration
            // is not a state access.
            long lastAccessTimestamp =
                    priorTtlAwareSerializer.wrapsTtlValue()
                            ? ((TtlValue<?>) priorValue).getLastAccessTimestamp()
                            : ttlTimeProvider.currentTimestamp();
            outputRecord = (T) new TtlValue<>(migratedValue, lastAccessTimestamp);
        } else {
            outputRecord = (T) migratedValue;
        }
        this.serialize(outputRecord, target);
    }

    /**
     * The snapshot describing the schema the prior bare value was written with.
     *
     * <p>The snapshot persisted with the state is preferred over one re-derived from the prior
     * serializer, because the prior serializer is itself restored from that snapshot and the round
     * trip back to a snapshot is not always lossless: a POJO field that no longer exists on the
     * class returns under a generated placeholder name, which would present a schema that was never
     * written. Only the absence of a persisted snapshot falls back to the re-derived one: a
     * persisted snapshot that does not match the prior serializer is an error, not a second reason
     * to fall back, because re-deriving there would silently reintroduce that lossy round trip.
     */
    private static TypeSerializerSnapshot<?> priorBareValueSerializerSnapshot(
            TtlAwareSerializer<?, ?> priorSerializer,
            @Nullable TypeSerializerSnapshot<?> priorSerializerSnapshot) {
        if (priorSerializerSnapshot == null) {
            return priorSerializer.bareValueSerializerSnapshot();
        }
        // TtlAwareSerializerSnapshot is the snapshot counterpart of this class, so the persisted
        // snapshot carries that layer wherever the serializer carries the wrapper: for a list or
        // map state it is the element or value snapshot, for a value state the whole snapshot.
        TypeSerializerSnapshot<?> priorSnapshot =
                priorSerializerSnapshot instanceof TtlAwareSerializerSnapshot
                        ? ((TtlAwareSerializerSnapshot<?>) priorSerializerSnapshot)
                                .getOriginalTypeSerializerSnapshot()
                        : priorSerializerSnapshot;

        // Thrown rather than checked through Preconditions: this runs once per migrated state
        // value, so the message must not be built while the check is passing.
        boolean isTtlSnapshot = priorSnapshot instanceof TtlStateFactory.TtlSerializerSnapshot;
        if (!priorSerializer.wrapsTtlValue()) {
            if (isTtlSnapshot) {
                throw new IllegalArgumentException(
                        "The prior serializer does not wrap values in TtlValue, but its persisted snapshot is a TtlSerializerSnapshot.");
            }
            return priorSnapshot;
        }
        if (!isTtlSnapshot) {
            throw new IllegalArgumentException(
                    "The prior serializer wraps values in TtlValue, so its persisted snapshot should be a TtlSerializerSnapshot, but was "
                            + priorSnapshot.getClass().getName()
                            + ".");
        }
        // The persisted snapshot describes the TtlValue envelope, so descend to the user value
        // the same way bareValueSerializer() descends the serializer.
        return ((TtlStateFactory.TtlSerializerSnapshot<?>) priorSnapshot)
                .getValueSerializerSnapshot();
    }

    private TypeSerializerSnapshot<?> bareValueSerializerSnapshot() {
        if (bareValueSerializerSnapshot == null) {
            bareValueSerializerSnapshot = bareValueSerializer().snapshotConfiguration();
        }
        return bareValueSerializerSnapshot;
    }

    /**
     * The serializer of the bare (non-TTL) value: the user value serializer of a {@link
     * TtlStateFactory.TtlSerializer}, otherwise the wrapped serializer itself.
     */
    private TypeSerializer<?> bareValueSerializer() {
        return wrapsTtlValue()
                ? ((TtlStateFactory.TtlSerializer<?>) typeSerializer).getValueSerializer()
                : typeSerializer;
    }

    /**
     * Whether the values this serializer reads and writes are {@link TtlValue} envelopes. Narrower
     * than {@link #isTtlEnabled()}, which is also true for a list or map serializer whose element
     * or value serializer is a {@link TtlStateFactory.TtlSerializer}: such a serializer wraps the
     * collection, not a single {@code TtlValue}.
     */
    private boolean wrapsTtlValue() {
        return typeSerializer instanceof TtlStateFactory.TtlSerializer;
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
