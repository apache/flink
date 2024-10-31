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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.runtime.state.v2.ReducingStateDescriptor;
import org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;
import org.apache.flink.runtime.state.v2.ttl.TtlStateFactory;
import org.apache.flink.state.forst.snapshot.ForStSnapshotStrategyBase;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;

/**
 * A KeyedStateBackend that stores its state in {@code ForSt}. This state backend can store very
 * large state that exceeds memory even disk to remote storage.
 */
public class ForStKeyedStateBackend<K> implements AsyncKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStKeyedStateBackend.class);

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    /** The key groups which this state backend is responsible for. */
    private final KeyGroupRange keyGroupRange;

    protected final TtlTimeProvider ttlTimeProvider;

    /** The key serializer. */
    protected final TypeSerializer<K> keySerializer;

    /** Supplier to create SerializedCompositeKeyBuilder. */
    private final Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    /** Supplier to create DataOutputSerializer to serialize value. */
    private final Supplier<DataOutputSerializer> valueSerializerView;

    /** Supplier to create DataInputDeserializer to deserialize value. */
    private final Supplier<DataInputDeserializer> valueDeserializerView;

    private final UUID backendUID;

    /** The container of ForSt options. */
    private final ForStResourceContainer optionsContainer;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /**
     * We are not using the default column family for Flink state ops, but we still need to remember
     * this handle so that we can close it properly when the backend is closed. Note that the one
     * returned by {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamily;

    private final ForStSnapshotStrategyBase<K, ?> snapshotStrategy;

    /** Factory for priority queue state. */
    private final PriorityQueueSetFactory priorityQueueFactory;

    private final HeapPriorityQueuesManager heapPriorityQueuesManager;

    /**
     * Registry for all opened streams, so they can be closed if the task using this backend is
     * closed.
     */
    private final CloseableRegistry cancelStreamRegistry;

    /** The native metrics monitor. */
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    /**
     * Our ForSt database. The different k/v states that we have don't each have their own ForSt
     * instance. They all write to this instance but to their own column family.
     */
    protected final RocksDB db;

    /** Handler to handle state request. */
    private StateRequestHandler stateRequestHandler;

    /**
     * Information about the k/v states, maintained in the order as we create them. This is used to
     * retrieve the column family that is used for a state and also for sanity checks when
     * restoring.
     */
    private final LinkedHashMap<String, ForStKvStateInfo> kvStateInformation;

    /** Lock guarding the {@code managedStateExecutors} and {@code disposed}. */
    private final Object lock = new Object();

    /** The StateExecutors which are managed by this ForStKeyedStateBackend. */
    @GuardedBy("lock")
    private final Set<StateExecutor> managedStateExecutors;

    /** Mark whether this backend is already disposed and prevent duplicate disposing. */
    @GuardedBy("lock")
    private boolean disposed = false;

    private final ForStDBTtlCompactFiltersManager ttlCompactFiltersManager;

    public ForStKeyedStateBackend(
            UUID backendUID,
            ForStResourceContainer optionsContainer,
            int keyGroupPrefixBytes,
            TypeSerializer<K> keySerializer,
            Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder,
            Supplier<DataOutputSerializer> valueSerializerView,
            Supplier<DataInputDeserializer> valueDeserializerView,
            RocksDB db,
            LinkedHashMap<String, ForStKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStSnapshotStrategyBase<K, ?> snapshotStrategy,
            PriorityQueueSetFactory priorityQueueFactory,
            CloseableRegistry cancelStreamRegistry,
            ForStNativeMetricMonitor nativeMetricMonitor,
            InternalKeyContext<K> keyContext,
            TtlTimeProvider ttlTimeProvider,
            ForStDBTtlCompactFiltersManager ttlCompactFiltersManager) {
        this.backendUID = backendUID;
        this.optionsContainer = Preconditions.checkNotNull(optionsContainer);
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keyGroupRange = keyContext.getKeyGroupRange();
        this.keySerializer = keySerializer;
        this.serializedKeyBuilder = serializedKeyBuilder;
        this.valueSerializerView = valueSerializerView;
        this.valueDeserializerView = valueDeserializerView;
        this.db = db;
        this.kvStateInformation = kvStateInformation;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.defaultColumnFamily = defaultColumnFamilyHandle;
        this.snapshotStrategy = snapshotStrategy;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.ttlTimeProvider = ttlTimeProvider;
        this.ttlCompactFiltersManager = ttlCompactFiltersManager;
        this.managedStateExecutors = new HashSet<>(1);
        this.priorityQueueFactory = priorityQueueFactory;
        if (priorityQueueFactory instanceof HeapPriorityQueueSetFactory) {
            this.heapPriorityQueuesManager =
                    new HeapPriorityQueuesManager(
                            registeredPQStates,
                            (HeapPriorityQueueSetFactory) priorityQueueFactory,
                            keyContext.getKeyGroupRange(),
                            keyContext.getNumberOfKeyGroups());
        } else {
            this.heapPriorityQueuesManager = null;
        }
    }

    @Override
    public void setup(@Nonnull StateRequestHandler stateRequestHandler) {
        this.stateRequestHandler = stateRequestHandler;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, SV> S createState(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        return TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                defaultNamespace, namespaceSerializer, stateDesc, this, ttlTimeProvider);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        Preconditions.checkNotNull(
                stateRequestHandler,
                "A non-null stateRequestHandler must be setup before createState");

        Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult =
                tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

        ColumnFamilyHandle columnFamilyHandle = registerResult.f0;

        switch (stateDesc.getType()) {
            case VALUE:
                return (S)
                        new ForStValueState<>(
                                stateRequestHandler,
                                columnFamilyHandle,
                                (ValueStateDescriptor<SV>) stateDesc,
                                serializedKeyBuilder,
                                defaultNamespace,
                                namespaceSerializer::duplicate,
                                valueSerializerView,
                                valueDeserializerView);

            case LIST:
                return (S)
                        new ForStListState<>(
                                stateRequestHandler,
                                columnFamilyHandle,
                                (ListStateDescriptor<SV>) stateDesc,
                                serializedKeyBuilder,
                                defaultNamespace,
                                namespaceSerializer::duplicate,
                                valueSerializerView,
                                valueDeserializerView);
            case MAP:
                Supplier<DataInputDeserializer> keyDeserializerView = DataInputDeserializer::new;
                return ForStMapState.create(
                        stateDesc,
                        stateRequestHandler,
                        columnFamilyHandle,
                        serializedKeyBuilder,
                        defaultNamespace,
                        namespaceSerializer::duplicate,
                        valueSerializerView,
                        keyDeserializerView,
                        valueDeserializerView,
                        keyGroupPrefixBytes);
            case REDUCING:
                return (S)
                        new ForStReducingState<>(
                                stateRequestHandler,
                                columnFamilyHandle,
                                (ReducingStateDescriptor<SV>) stateDesc,
                                serializedKeyBuilder,
                                defaultNamespace,
                                namespaceSerializer::duplicate,
                                valueSerializerView,
                                valueDeserializerView);
            case AGGREGATING:
                return (S)
                        new ForStAggregatingState<>(
                                (AggregatingStateDescriptor<?, SV, ?>) stateDesc,
                                stateRequestHandler,
                                columnFamilyHandle,
                                serializedKeyBuilder,
                                defaultNamespace,
                                namespaceSerializer::duplicate,
                                valueSerializerView,
                                valueDeserializerView);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported state type: %s", stateDesc.getType()));
        }
    }

    private <N, SV>
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    tryRegisterKvStateInformation(
                            StateDescriptor<SV> stateDesc, TypeSerializer<N> namespaceSerializer)
                            throws Exception {

        ForStKvStateInfo oldStateInfo = kvStateInformation.get(stateDesc.getStateId());

        TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

        ForStKvStateInfo newStateInfo;
        RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
        if (oldStateInfo != null) {
            @SuppressWarnings("unchecked")
            RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo =
                    (RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo.metaInfo;

            newMetaInfo =
                    updateRestoredStateMetaInfo(
                            Tuple2.of(oldStateInfo.columnFamilyHandle, castedMetaInfo),
                            stateDesc,
                            stateSerializer,
                            namespaceSerializer);

            newStateInfo = new ForStKvStateInfo(oldStateInfo.columnFamilyHandle, newMetaInfo);
            kvStateInformation.put(stateDesc.getStateId(), newStateInfo);
        } else {
            newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getStateId(),
                            stateDesc.getType(),
                            namespaceSerializer,
                            stateSerializer,
                            StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

            newStateInfo =
                    ForStOperationUtils.createAsyncStateInfo(
                            newMetaInfo,
                            db,
                            columnFamilyOptionsFactory,
                            ttlCompactFiltersManager,
                            optionsContainer.getWriteBufferManagerCapacity());
            ForStOperationUtils.registerKvStateInformation(
                    this.kvStateInformation,
                    this.nativeMetricMonitor,
                    stateDesc.getStateId(),
                    newStateInfo);
        }

        return Tuple2.of(newStateInfo.columnFamilyHandle, newMetaInfo);
    }

    private <N, SV> RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> oldStateInfo,
            StateDescriptor<SV> stateDesc,
            TypeSerializer<SV> stateSerializer,
            TypeSerializer<N> namespaceSerializer)
            throws Exception {

        RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo.f1;

        restoredKvStateMetaInfo.checkStateMetaInfo(stateDesc);

        // fetch current serializer now because if it is incompatible, we can't access it anymore to
        // improve the error message
        TypeSerializer<SV> previousStateSerializer = restoredKvStateMetaInfo.getStateSerializer();

        TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility =
                restoredKvStateMetaInfo.updateStateSerializer(stateSerializer);

        if (!stateSerializer.equals(previousStateSerializer)
                && newStateSerializerCompatibility.isCompatibleAfterMigration()) {

            // TODO: 2024/6/6 wangfeifan - Implement state migration
            throw new UnsupportedOperationException("State migration not support yet.");

        } else if (newStateSerializerCompatibility.isIncompatible()) {
            throw new StateMigrationException(
                    "The new state serializer ("
                            + stateSerializer
                            + ") must not be incompatible with the old state serializer ("
                            + previousStateSerializer
                            + ").");
        }

        return restoredKvStateMetaInfo;
    }

    @Override
    @Nonnull
    public StateExecutor createStateExecutor() {
        synchronized (lock) {
            if (disposed) {
                throw new FlinkRuntimeException(
                        "Attempt to create StateExecutor after ForStKeyedStateBackend is disposed.");
            }
            StateExecutor stateExecutor =
                    new ForStStateExecutor(
                            optionsContainer.isCoordinatorInline(),
                            optionsContainer.isWriteInline(),
                            optionsContainer.getReadIoParallelism(),
                            optionsContainer.getWriteIoParallelism(),
                            db,
                            optionsContainer.getWriteOptions());
            managedStateExecutors.add(stateExecutor);
            return stateExecutor;
        }
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        return new SnapshotStrategyRunner<>(
                        snapshotStrategy.getDescription(),
                        snapshotStrategy,
                        cancelStreamRegistry,
                        ASYNCHRONOUS)
                .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (snapshotStrategy != null) {
            snapshotStrategy.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (snapshotStrategy != null) {
            snapshotStrategy.notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        LOG.info("Backend:{} checkpoint: {} subsumed.", backendUID, checkpointId);
    }

    /** Should only be called by one thread, and only after all accesses to the DB happened. */
    @Override
    public void dispose() {
        synchronized (lock) {
            if (this.disposed) {
                return;
            }
            for (StateExecutor executor : managedStateExecutors) {
                executor.shutdown();
            }
            // IMPORTANT: null reference to signal potential async checkpoint workers that the db
            // was disposed, as working on the disposed object results in SEGFAULTS.
            if (db != null) {

                // Metric collection occurs on a background thread. When this method returns
                // it is guaranteed that thr ForSt reference has been invalidated
                // and no more metric collection will be attempted against the database.
                if (nativeMetricMonitor != null) {
                    nativeMetricMonitor.close();
                }

                IOUtils.closeQuietly(defaultColumnFamily);

                // ... and finally close the DB instance ...
                IOUtils.closeQuietly(db);

                LOG.info(
                        "Closed ForSt State Backend. Cleaning up ForSt local working directory {}, remote working directory {}.",
                        optionsContainer.getLocalBasePath(),
                        optionsContainer.getRemoteBasePath());

                try {
                    optionsContainer.clearDirectories();
                } catch (Exception ex) {
                    LOG.warn(
                            "Could not delete ForSt local working directory {}, remote working directory {}.",
                            optionsContainer.getLocalBasePath(),
                            optionsContainer.getRemoteBasePath(),
                            ex);
                }

                IOUtils.closeQuietly(optionsContainer);
            }
            IOUtils.closeQuietly(snapshotStrategy);
            this.disposed = true;
        }
    }

    @VisibleForTesting
    File getLocalBasePath() {
        return optionsContainer.getLocalBasePath();
    }

    @VisibleForTesting
    Path getRemoteBasePath() {
        return optionsContainer.getRemoteBasePath();
    }

    @Override
    public void close() throws IOException {
        dispose();
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        if (this.heapPriorityQueuesManager != null) {
            return this.heapPriorityQueuesManager.createOrUpdate(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        } else {
            return priorityQueueFactory.create(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }
    }

    /** ForSt specific information about the k/v states. */
    public static class ForStKvStateInfo implements AutoCloseable {
        public final ColumnFamilyHandle columnFamilyHandle;
        public final RegisteredStateMetaInfoBase metaInfo;

        public ForStKvStateInfo(
                ColumnFamilyHandle columnFamilyHandle, RegisteredStateMetaInfoBase metaInfo) {
            this.columnFamilyHandle = columnFamilyHandle;
            this.metaInfo = metaInfo;
        }

        @Override
        public void close() throws Exception {
            this.columnFamilyHandle.close();
        }
    }
}
