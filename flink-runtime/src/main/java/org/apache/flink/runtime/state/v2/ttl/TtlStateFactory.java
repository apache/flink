/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2.ttl;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.TtlReduceFunction;
import org.apache.flink.runtime.state.ttl.TtlStateContext;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.runtime.state.v2.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.ReducingStateDescriptor;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;

public class TtlStateFactory<K, N, SV, TTLSV, S extends State, IS> {
    public static <K, N, SV, TTLSV, S extends State, IS extends S>
            IS createStateAndWrapWithTtlIfEnabled(
                    N defaultNamespace,
                    TypeSerializer<N> namespaceSerializer,
                    StateDescriptor<SV> stateDesc,
                    AsyncKeyedStateBackend<K> stateBackend,
                    TtlTimeProvider timeProvider)
                    throws Exception {
        Preconditions.checkNotNull(namespaceSerializer);
        Preconditions.checkNotNull(stateDesc);
        Preconditions.checkNotNull(stateBackend);
        Preconditions.checkNotNull(timeProvider);
        if (stateDesc.getTtlConfig().isEnabled()) {
            if (!stateDesc.getTtlConfig().getCleanupStrategies().inRocksdbCompactFilter()) {
                throw new UnsupportedOperationException(
                        "Only ROCKSDB_COMPACTION_FILTER strategy is supported in state V2.");
            }
            if (stateDesc
                    .getTtlConfig()
                    .getUpdateType()
                    .equals(StateTtlConfig.UpdateType.OnReadAndWrite)) {
                throw new UnsupportedOperationException(
                        "OnReadAndWrite update type is not supported in state V2.");
            }
        }
        return stateDesc.getTtlConfig().isEnabled()
                ? new TtlStateFactory<K, N, SV, TTLSV, S, IS>(
                                defaultNamespace,
                                namespaceSerializer,
                                stateDesc,
                                stateBackend,
                                timeProvider)
                        .createState()
                : stateBackend.createStateInternal(
                        defaultNamespace, namespaceSerializer, stateDesc);
    }

    public static boolean isTtlStateSerializer(TypeSerializer<?> typeSerializer) {
        //  element's serializer in state descriptor.
        boolean ttlSerializer = typeSerializer instanceof TtlSerializer;
        return ttlSerializer;
    }

    private final Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> stateFactories;
    private N defaultNamespace;
    private TypeSerializer<N> namespaceSerializer;
    private StateDescriptor<SV> stateDesc;
    private AsyncKeyedStateBackend<K> stateBackend;
    private TtlTimeProvider timeProvider;

    @Nonnull private final StateTtlConfig ttlConfig;
    private final long ttl;

    private TtlStateFactory(
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<SV> stateDesc,
            AsyncKeyedStateBackend<K> stateBackend,
            TtlTimeProvider timeProvider) {
        this.defaultNamespace = defaultNamespace;
        this.namespaceSerializer = namespaceSerializer;
        this.stateDesc = stateDesc;
        this.stateBackend = stateBackend;
        this.ttlConfig = stateDesc.getTtlConfig();
        this.timeProvider = timeProvider;
        this.ttl = ttlConfig.getTimeToLive().toMillis();
        this.stateFactories = createStateFactories();
    }

    private Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> createStateFactories() {
        return Stream.of(
                        Tuple2.of(
                                StateDescriptor.Type.VALUE,
                                (SupplierWithException<IS, Exception>) this::createValueState),
                        Tuple2.of(
                                StateDescriptor.Type.LIST,
                                (SupplierWithException<IS, Exception>) this::createListState),
                        Tuple2.of(
                                StateDescriptor.Type.MAP,
                                (SupplierWithException<IS, Exception>) this::createMapState),
                        Tuple2.of(
                                StateDescriptor.Type.REDUCING,
                                (SupplierWithException<IS, Exception>) this::createReducingState),
                        Tuple2.of(
                                StateDescriptor.Type.AGGREGATING,
                                (SupplierWithException<IS, Exception>)
                                        this::createAggregatingState))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    @SuppressWarnings("unchecked")
    private IS createState() throws Exception {
        SupplierWithException<IS, Exception> stateFactory = stateFactories.get(stateDesc.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State type: %s is not supported by %s",
                            stateDesc.getType(), TtlStateFactory.class);
            throw new FlinkRuntimeException(message);
        }
        return stateFactory.get();
    }

    @SuppressWarnings("unchecked")
    private IS createValueState() throws Exception {
        ValueStateDescriptor<TtlValue<SV>> ttlDescriptor =
                stateDesc.getSerializer() instanceof TtlSerializer
                        ? (ValueStateDescriptor<TtlValue<SV>>) stateDesc
                        : new ValueStateDescriptor<>(
                                stateDesc.getStateId(),
                                new TtlTypeInformation<>(
                                        new TtlSerializer<>(
                                                LongSerializer.INSTANCE,
                                                stateDesc.getSerializer())));
        return (IS) new TtlValueState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private <T> IS createListState() throws Exception {
        ListStateDescriptor<T> listStateDesc = (ListStateDescriptor<T>) stateDesc;
        ListStateDescriptor<TtlValue<T>> ttlDescriptor =
                listStateDesc.getSerializer() instanceof TtlSerializer
                        ? (ListStateDescriptor<TtlValue<T>>) stateDesc
                        : new ListStateDescriptor<>(
                                stateDesc.getStateId(),
                                new TtlTypeInformation<>(
                                        new TtlSerializer<>(
                                                LongSerializer.INSTANCE,
                                                listStateDesc.getSerializer())));
        return (IS) new TtlListState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private <UK, UV> IS createMapState() throws Exception {
        MapStateDescriptor<UK, UV> mapStateDesc = (MapStateDescriptor<UK, UV>) stateDesc;
        MapStateDescriptor<UK, TtlValue<UV>> ttlDescriptor =
                mapStateDesc.getSerializer() instanceof TtlSerializer
                        ? (MapStateDescriptor<UK, TtlValue<UV>>) stateDesc
                        : new MapStateDescriptor<>(
                                stateDesc.getStateId(),
                                mapStateDesc.getUserKeyType(),
                                new TtlTypeInformation<>(
                                        new TtlSerializer<>(
                                                LongSerializer.INSTANCE,
                                                mapStateDesc.getSerializer())));
        return (IS) new TtlMapState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private IS createReducingState() throws Exception {
        ReducingStateDescriptor<SV> reducingStateDesc = (ReducingStateDescriptor<SV>) stateDesc;
        ReducingStateDescriptor<TtlValue<SV>> ttlDescriptor =
                stateDesc.getSerializer() instanceof TtlSerializer
                        ? (ReducingStateDescriptor<TtlValue<SV>>) stateDesc
                        : new ReducingStateDescriptor<>(
                                stateDesc.getStateId(),
                                new TtlReduceFunction<>(
                                        reducingStateDesc.getReduceFunction(),
                                        ttlConfig,
                                        timeProvider),
                                new TtlTypeInformation<>(
                                        new TtlSerializer<>(
                                                LongSerializer.INSTANCE,
                                                stateDesc.getSerializer())));
        return (IS) new TtlReducingState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private <IN, OUT> IS createAggregatingState() throws Exception {
        AggregatingStateDescriptor<IN, SV, OUT> aggregatingStateDescriptor =
                (AggregatingStateDescriptor<IN, SV, OUT>) stateDesc;
        TtlAggregateFunction<IN, SV, OUT> ttlAggregateFunction =
                new TtlAggregateFunction<>(
                        aggregatingStateDescriptor.getAggregateFunction(), ttlConfig, timeProvider);
        AggregatingStateDescriptor<IN, TtlValue<SV>, OUT> ttlDescriptor =
                stateDesc.getSerializer() instanceof TtlSerializer
                        ? (AggregatingStateDescriptor<IN, TtlValue<SV>, OUT>) stateDesc
                        : new AggregatingStateDescriptor<>(
                                stateDesc.getStateId(),
                                ttlAggregateFunction,
                                new TtlTypeInformation<>(
                                        new TtlSerializer<>(
                                                LongSerializer.INSTANCE,
                                                stateDesc.getSerializer())));
        return (IS)
                new TtlAggregatingState<>(
                        createTtlStateContext(ttlDescriptor), ttlAggregateFunction);
    }

    @SuppressWarnings("unchecked")
    private <OIS extends State, TTLS extends State, V, TTLV>
            TtlStateContext<OIS, V> createTtlStateContext(StateDescriptor<TTLV> ttlDescriptor)
                    throws Exception {

        ttlDescriptor.enableTimeToLive(
                stateDesc.getTtlConfig()); // also used by RocksDB backend for TTL compaction filter
        // config
        OIS originalState =
                (OIS)
                        stateBackend.createStateInternal(
                                defaultNamespace, namespaceSerializer, ttlDescriptor);
        return new TtlStateContext<>(
                originalState,
                ttlConfig,
                timeProvider,
                (TypeSerializer<V>) stateDesc.getSerializer(),
                () -> {});
    }

    public static class TtlTypeInformation<T> extends TypeInformation<TtlValue<T>> {

        Class<?> typeClass;

        TypeSerializer<TtlValue<T>> typeSerializer;

        TtlTypeInformation(TypeSerializer<TtlValue<T>> typeSerializer) {
            this.typeSerializer = typeSerializer;
            typeClass = TtlValue.class;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public int getTotalFields() {
            return 2;
        }

        @Override
        public Class<TtlValue<T>> getTypeClass() {
            return (Class<TtlValue<T>>) typeClass;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<TtlValue<T>> createSerializer(SerializerConfig config) {
            return typeSerializer;
        }

        @Override
        public String toString() {
            return "TtlTypeInformation{}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return typeSerializer.equals(((TtlTypeInformation<T>) obj).typeSerializer);
        }

        @Override
        public int hashCode() {
            return typeSerializer.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TtlTypeInformation;
        }
    }

    /**
     * Serializer for user state value with TTL. Visibility is public for usage with external tools.
     */
    public static class TtlSerializer<T> extends CompositeSerializer<TtlValue<T>> {
        private static final long serialVersionUID = 131020282727167064L;

        @SuppressWarnings("WeakerAccess")
        public TtlSerializer(
                TypeSerializer<Long> timestampSerializer, TypeSerializer<T> userValueSerializer) {
            super(true, timestampSerializer, userValueSerializer);
            checkArgument(!(userValueSerializer instanceof TtlSerializer));
        }

        @SuppressWarnings("WeakerAccess")
        public TtlSerializer(
                PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
            super(precomputed, fieldSerializers);
        }

        @SuppressWarnings("unchecked")
        @Override
        public TtlValue<T> createInstance(@Nonnull Object... values) {
            Preconditions.checkArgument(values.length == 2);
            return new TtlValue<>((T) values[1], (long) values[0]);
        }

        @Override
        protected void setField(@Nonnull TtlValue<T> v, int index, Object fieldValue) {
            throw new UnsupportedOperationException("TtlValue is immutable");
        }

        @Override
        protected Object getField(@Nonnull TtlValue<T> v, int index) {
            return index == 0 ? v.getLastAccessTimestamp() : v.getUserValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected CompositeSerializer<TtlValue<T>> createSerializerInstance(
                PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
            Preconditions.checkNotNull(originalSerializers);
            Preconditions.checkArgument(originalSerializers.length == 2);
            return new TtlSerializer<>(precomputed, originalSerializers);
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<Long> getTimestampSerializer() {
            return (TypeSerializer<Long>) (TypeSerializer<?>) fieldSerializers[0];
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<T> getValueSerializer() {
            return (TypeSerializer<T>) fieldSerializers[1];
        }

        @Override
        public TypeSerializerSnapshot<TtlValue<T>> snapshotConfiguration() {
            return new TtlSerializerSnapshot<>(this);
        }
    }

    /** A {@link TypeSerializerSnapshot} for TtlSerializer. */
    public static final class TtlSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<TtlValue<T>, TtlSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings({"WeakerAccess", "unused"})
        public TtlSerializerSnapshot() {}

        TtlSerializerSnapshot(TtlSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(TtlSerializer<T> outerSerializer) {
            return new TypeSerializer[] {
                outerSerializer.getTimestampSerializer(), outerSerializer.getValueSerializer()
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        protected TtlSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<Long> timestampSerializer = (TypeSerializer<Long>) nestedSerializers[0];
            TypeSerializer<T> valueSerializer = (TypeSerializer<T>) nestedSerializers[1];

            return new TtlSerializer<>(timestampSerializer, valueSerializer);
        }
    }
}
