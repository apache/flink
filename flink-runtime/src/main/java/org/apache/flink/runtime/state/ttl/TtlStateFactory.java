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

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** This state factory wraps state objects, produced by backends, with TTL logic. */
public class TtlStateFactory<K, N, SV, TTLSV, S extends State, IS extends S> {
    public static <K, N, SV, TTLSV, S extends State, IS extends S>
            IS createStateAndWrapWithTtlIfEnabled(
                    TypeSerializer<N> namespaceSerializer,
                    StateDescriptor<S, SV> stateDesc,
                    KeyedStateBackend<K> stateBackend,
                    TtlTimeProvider timeProvider)
                    throws Exception {
        Preconditions.checkNotNull(namespaceSerializer);
        Preconditions.checkNotNull(stateDesc);
        Preconditions.checkNotNull(stateBackend);
        Preconditions.checkNotNull(timeProvider);
        return stateDesc.getTtlConfig().isEnabled()
                ? new TtlStateFactory<K, N, SV, TTLSV, S, IS>(
                                namespaceSerializer, stateDesc, stateBackend, timeProvider)
                        .createState()
                : stateBackend.createInternalState(namespaceSerializer, stateDesc);
    }

    private final Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> stateFactories;

    @Nonnull private final TypeSerializer<N> namespaceSerializer;
    @Nonnull private final StateDescriptor<S, SV> stateDesc;
    @Nonnull private final KeyedStateBackend<K> stateBackend;
    @Nonnull private final StateTtlConfig ttlConfig;
    @Nonnull private final TtlTimeProvider timeProvider;
    private final long ttl;
    @Nullable private final TtlIncrementalCleanup<K, N, TTLSV> incrementalCleanup;

    private TtlStateFactory(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull KeyedStateBackend<K> stateBackend,
            @Nonnull TtlTimeProvider timeProvider) {
        this.namespaceSerializer = namespaceSerializer;
        this.stateDesc = stateDesc;
        this.stateBackend = stateBackend;
        this.ttlConfig = stateDesc.getTtlConfig();
        this.timeProvider = timeProvider;
        this.ttl = ttlConfig.getTtl().toMilliseconds();
        this.stateFactories = createStateFactories();
        this.incrementalCleanup = getTtlIncrementalCleanup();
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
        IS state = stateFactory.get();
        if (incrementalCleanup != null) {
            incrementalCleanup.setTtlState((AbstractTtlState<K, N, ?, TTLSV, ?>) state);
        }
        return state;
    }

    @SuppressWarnings("unchecked")
    private IS createValueState() throws Exception {
        ValueStateDescriptor<TtlValue<SV>> ttlDescriptor =
                new ValueStateDescriptor<>(
                        stateDesc.getName(),
                        new TtlSerializer<>(LongSerializer.INSTANCE, stateDesc.getSerializer()));
        return (IS) new TtlValueState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private <T> IS createListState() throws Exception {
        ListStateDescriptor<T> listStateDesc = (ListStateDescriptor<T>) stateDesc;
        ListStateDescriptor<TtlValue<T>> ttlDescriptor =
                new ListStateDescriptor<>(
                        stateDesc.getName(),
                        new TtlSerializer<>(
                                LongSerializer.INSTANCE, listStateDesc.getElementSerializer()));
        return (IS) new TtlListState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private <UK, UV> IS createMapState() throws Exception {
        MapStateDescriptor<UK, UV> mapStateDesc = (MapStateDescriptor<UK, UV>) stateDesc;
        MapStateDescriptor<UK, TtlValue<UV>> ttlDescriptor =
                new MapStateDescriptor<>(
                        stateDesc.getName(),
                        mapStateDesc.getKeySerializer(),
                        new TtlSerializer<>(
                                LongSerializer.INSTANCE, mapStateDesc.getValueSerializer()));
        return (IS) new TtlMapState<>(createTtlStateContext(ttlDescriptor));
    }

    @SuppressWarnings("unchecked")
    private IS createReducingState() throws Exception {
        ReducingStateDescriptor<SV> reducingStateDesc = (ReducingStateDescriptor<SV>) stateDesc;
        ReducingStateDescriptor<TtlValue<SV>> ttlDescriptor =
                new ReducingStateDescriptor<>(
                        stateDesc.getName(),
                        new TtlReduceFunction<>(
                                reducingStateDesc.getReduceFunction(), ttlConfig, timeProvider),
                        new TtlSerializer<>(LongSerializer.INSTANCE, stateDesc.getSerializer()));
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
                new AggregatingStateDescriptor<>(
                        stateDesc.getName(),
                        ttlAggregateFunction,
                        new TtlSerializer<>(LongSerializer.INSTANCE, stateDesc.getSerializer()));
        return (IS)
                new TtlAggregatingState<>(
                        createTtlStateContext(ttlDescriptor), ttlAggregateFunction);
    }

    @SuppressWarnings("unchecked")
    private <OIS extends State, TTLS extends State, V, TTLV>
            TtlStateContext<OIS, V> createTtlStateContext(StateDescriptor<TTLS, TTLV> ttlDescriptor)
                    throws Exception {

        ttlDescriptor.enableTimeToLive(
                stateDesc.getTtlConfig()); // also used by RocksDB backend for TTL compaction filter
        // config
        OIS originalState =
                (OIS)
                        stateBackend.createInternalState(
                                namespaceSerializer, ttlDescriptor, getSnapshotTransformFactory());
        return new TtlStateContext<>(
                originalState,
                ttlConfig,
                timeProvider,
                (TypeSerializer<V>) stateDesc.getSerializer(),
                registerTtlIncrementalCleanupCallback((InternalKvState<?, ?, ?>) originalState));
    }

    private TtlIncrementalCleanup<K, N, TTLSV> getTtlIncrementalCleanup() {
        StateTtlConfig.IncrementalCleanupStrategy config =
                ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
        return config != null ? new TtlIncrementalCleanup<>(config.getCleanupSize()) : null;
    }

    private Runnable registerTtlIncrementalCleanupCallback(InternalKvState<?, ?, ?> originalState) {
        StateTtlConfig.IncrementalCleanupStrategy config =
                ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
        boolean cleanupConfigured = config != null && incrementalCleanup != null;
        boolean isCleanupActive =
                cleanupConfigured
                        && isStateIteratorSupported(
                                originalState, incrementalCleanup.getCleanupSize());
        Runnable callback = isCleanupActive ? incrementalCleanup::stateAccessed : () -> {};
        if (isCleanupActive && config.runCleanupForEveryRecord()) {
            stateBackend.registerKeySelectionListener(stub -> callback.run());
        }
        return callback;
    }

    private boolean isStateIteratorSupported(InternalKvState<?, ?, ?> originalState, int size) {
        boolean stateIteratorSupported = false;
        try {
            stateIteratorSupported = originalState.getStateIncrementalVisitor(size) != null;
        } catch (Throwable t) {
            // ignore
        }
        return stateIteratorSupported;
    }

    private StateSnapshotTransformFactory<?> getSnapshotTransformFactory() {
        if (!ttlConfig.getCleanupStrategies().inFullSnapshot()) {
            return StateSnapshotTransformFactory.noTransform();
        } else {
            return new TtlStateSnapshotTransformer.Factory<>(timeProvider, ttl);
        }
    }

    /**
     * Serializer for user state value with TTL. Visibility is public for usage with external tools.
     */
    public static class TtlSerializer<T> extends CompositeSerializer<TtlValue<T>>
            implements TypeSerializerConfigSnapshot.SelfResolvingTypeSerializer<TtlValue<T>> {
        private static final long serialVersionUID = 131020282727167064L;

        @SuppressWarnings("WeakerAccess")
        public TtlSerializer(
                TypeSerializer<Long> timestampSerializer, TypeSerializer<T> userValueSerializer) {
            super(true, timestampSerializer, userValueSerializer);
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

        @Override
        public TypeSerializerSchemaCompatibility<TtlValue<T>>
                resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass(
                        TypeSerializerConfigSnapshot<TtlValue<T>> deprecatedConfigSnapshot) {

            if (deprecatedConfigSnapshot instanceof ConfigSnapshot) {
                ConfigSnapshot castedLegacyConfigSnapshot =
                        (ConfigSnapshot) deprecatedConfigSnapshot;
                TtlSerializerSnapshot<T> newSnapshot = new TtlSerializerSnapshot<>();

                return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                        this,
                        newSnapshot,
                        castedLegacyConfigSnapshot.getNestedSerializerSnapshots());
            }

            return TypeSerializerSchemaCompatibility.incompatible();
        }

        public static boolean isTtlStateSerializer(TypeSerializer<?> typeSerializer) {
            boolean ttlSerializer = typeSerializer instanceof TtlStateFactory.TtlSerializer;
            boolean ttlListSerializer =
                    typeSerializer instanceof ListSerializer
                            && ((ListSerializer) typeSerializer).getElementSerializer()
                                    instanceof TtlStateFactory.TtlSerializer;
            boolean ttlMapSerializer =
                    typeSerializer instanceof MapSerializer
                            && ((MapSerializer) typeSerializer).getValueSerializer()
                                    instanceof TtlStateFactory.TtlSerializer;
            return ttlSerializer || ttlListSerializer || ttlMapSerializer;
        }
    }

    /** A {@link TypeSerializerSnapshot} for TtlSerializer. */
    public static final class TtlSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<TtlValue<T>, TtlSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings({"WeakerAccess", "unused"})
        public TtlSerializerSnapshot() {
            super(TtlSerializer.class);
        }

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
