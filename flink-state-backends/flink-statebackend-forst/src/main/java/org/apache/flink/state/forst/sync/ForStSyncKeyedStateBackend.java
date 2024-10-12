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

package org.apache.flink.state.forst.sync;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlAwareSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStDBWriteBatchWrapper;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;
import org.forstdb.Snapshot;
import org.forstdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RunnableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@link AbstractKeyedStateBackend} that stores its state in {@code RocksDB} and serializes
 * state to streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory}
 * upon checkpointing. This state backend can store very large state that exceeds memory and spills
 * to disk. Except for the snapshotting, this class should be accessed as if it is not threadsafe.
 *
 * <p>This class follows the rules for closing/releasing native RocksDB resources as described in +
 * <a
 * href="https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families">
 * this document</a>.
 */
public class ForStSyncKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStSyncKeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateCreateFactory) ForStSyncValueState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateCreateFactory) ForStSyncListState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateCreateFactory) ForStSyncMapState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateCreateFactory) ForStSyncAggregatingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateCreateFactory) ForStSyncReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateUpdateFactory) ForStSyncValueState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateUpdateFactory) ForStSyncListState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateUpdateFactory) ForStSyncMapState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateUpdateFactory) ForStSyncAggregatingState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateUpdateFactory) ForStSyncReducingState::update))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                        registerResult,
                ForStSyncKeyedStateBackend<K> backend)
                throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc,
                Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                        registerResult,
                IS existingState)
                throws Exception;
    }

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of RocksDB option factory and predefined options. */
    private final ForStResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final File instanceBasePath;

    /**
     * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call
     * that disposes the RocksDB object.
     */
    private final ResourceGuard rocksDBResourceGuard;

    /** The write options to use in the states. We disable write ahead logging. */
    private final WriteOptions writeOptions;

    /**
     * The read options to use when creating iterators. We ensure total order seek in case user
     * misuse, see FLINK-17800 for more details.
     */
    private final ReadOptions readOptions;

    /**
     * The max memory size for one batch in {@link
     * org.apache.flink.state.forst.ForStDBWriteBatchWrapper}.
     */
    private final long writeBatchSize;

    /** Map of created k/v states. */
    private final Map<String, State> createdKVStates;

    /**
     * Information about the k/v states, maintained in the order as we create them. This is used to
     * retrieve the column family that is used for a state and also for sanity checks when
     * restoring.
     */
    private final LinkedHashMap<String, ForStDbKvStateInfo> kvStateInformation;

    private final HeapPriorityQueuesManager heapPriorityQueuesManager;

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    /**
     * We are not using the default column family for Flink state ops, but we still need to remember
     * this handle so that we can close it properly when the backend is closed. Note that the one
     * returned by {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamily;

    /** Shared wrapper for batch writes to the RocksDB instance. */
    private final ForStDBWriteBatchWrapper writeBatchWrapper;

    /**
     * The checkpoint snapshot strategy, e.g., if we use full or incremental checkpoints, local
     * state, and so on.
     */
    // private final RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy;

    /** The native metrics monitor. */
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    /** Factory for priority queue state. */
    private final PriorityQueueSetFactory priorityQueueFactory;

    /**
     * Helper to build the byte arrays of composite keys to address data in RocksDB. Shared across
     * all states.
     */
    private final SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;

    /**
     * Our RocksDB database, this is used by the actual subclasses of {@link AbstractForStSyncState}
     * to store state. The different k/v states that we have don't each have their own RocksDB
     * instance. They all write to this instance but to their own column family.
     */
    protected final RocksDB db;

    // mark whether this backend is already disposed and prevent duplicate disposing
    private boolean disposed = false;

    private final ForStDBTtlCompactFiltersManager ttlCompactFiltersManager;

    public ForStSyncKeyedStateBackend(
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            RocksDB db,
            LinkedHashMap<String, ForStDbKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            ResourceGuard rocksDBResourceGuard,
            // RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy,
            ForStDBWriteBatchWrapper writeBatchWrapper,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStNativeMetricMonitor nativeMetricMonitor,
            SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
            PriorityQueueSetFactory priorityQueueFactory,
            ForStDBTtlCompactFiltersManager ttlCompactFiltersManager,
            InternalKeyContext<K> keyContext,
            @Nonnegative long writeBatchSize,
            @Nullable CompletableFuture<Void> asyncCompactFuture) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);

        this.ttlCompactFiltersManager = ttlCompactFiltersManager;

        // ensure that we use the right merge operator, because other code relies on this
        this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);

        this.optionsContainer = Preconditions.checkNotNull(optionsContainer);

        this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);

        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.kvStateInformation = kvStateInformation;
        this.createdKVStates = new HashMap<>();

        this.writeOptions = optionsContainer.getWriteOptions();
        this.readOptions = optionsContainer.getReadOptions();
        this.writeBatchSize = writeBatchSize;
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        // this.checkpointSnapshotStrategy = checkpointSnapshotStrategy;
        this.writeBatchWrapper = writeBatchWrapper;
        this.defaultColumnFamily = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.sharedRocksKeyBuilder = sharedRocksKeyBuilder;
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

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        ForStDbKvStateInfo columnInfo = kvStateInformation.get(state);
        if (columnInfo == null
                || !(columnInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
            return Stream.empty();
        }

        RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.metaInfo;

        final TypeSerializer<N> namespaceSerializer =
                registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
        final DataOutputSerializer namespaceOutputView = new DataOutputSerializer(8);
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        getKeySerializer(), namespaceSerializer);
        final byte[] nameSpaceBytes;
        try {
            CompositeKeySerializationUtils.writeNameSpace(
                    namespace, namespaceSerializer, namespaceOutputView, ambiguousKeyPossible);
            nameSpaceBytes = namespaceOutputView.getCopyOfBuffer();
        } catch (IOException ex) {
            throw new FlinkRuntimeException("Failed to get keys from RocksDB state backend.", ex);
        }

        ForStIteratorWrapper iterator =
                ForStOperationUtils.getForStIterator(
                        db, columnInfo.columnFamilyHandle, readOptions);
        iterator.seekToFirst();

        final ForStStateKeysIterator<K> iteratorWrapper =
                new ForStStateKeysIterator<>(
                        iterator,
                        state,
                        getKeySerializer(),
                        keyGroupPrefixBytes,
                        ambiguousKeyPossible,
                        nameSpaceBytes);

        Stream<K> targetStream =
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED),
                        false);
        return targetStream.onClose(iteratorWrapper::close);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        ForStDbKvStateInfo columnInfo = kvStateInformation.get(state);
        if (columnInfo == null
                || !(columnInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
            return Stream.empty();
        }

        RegisteredKeyValueStateBackendMetaInfo<N, ?> registeredKeyValueStateBackendMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<N, ?>) columnInfo.metaInfo;

        final TypeSerializer<N> namespaceSerializer =
                registeredKeyValueStateBackendMetaInfo.getNamespaceSerializer();
        boolean ambiguousKeyPossible =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        getKeySerializer(), namespaceSerializer);

        ForStIteratorWrapper iterator =
                ForStOperationUtils.getForStIterator(
                        db, columnInfo.columnFamilyHandle, readOptions);
        iterator.seekToFirst();

        final ForStStateKeysAndNamespaceIterator<K, N> iteratorWrapper =
                new ForStStateKeysAndNamespaceIterator<>(
                        iterator,
                        state,
                        getKeySerializer(),
                        namespaceSerializer,
                        keyGroupPrefixBytes,
                        ambiguousKeyPossible);

        Stream<Tuple2<K, N>> targetStream =
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iteratorWrapper, Spliterator.ORDERED),
                        false);
        return targetStream.onClose(iteratorWrapper::close);
    }

    @VisibleForTesting
    ColumnFamilyHandle getColumnFamilyHandle(String state) {
        ForStDbKvStateInfo columnInfo = kvStateInformation.get(state);
        return columnInfo != null ? columnInfo.columnFamilyHandle : null;
    }

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        sharedRocksKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
    }

    @Override
    public void setCurrentKeyAndKeyGroup(K newKey, int newKeyGroupIndex) {
        super.setCurrentKeyAndKeyGroup(newKey, newKeyGroupIndex);
        sharedRocksKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
    }

    /** Should only be called by one thread, and only after all accesses to the DB happened. */
    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        super.dispose();

        // This call will block until all clients that still acquire access to the RocksDB instance
        // have released it,
        // so that we cannot release the native resources while clients are still working with it in
        // parallel.
        rocksDBResourceGuard.close();

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {
            IOUtils.closeQuietly(writeBatchWrapper);

            // Metric collection occurs on a background thread. When this method returns
            // it is guaranteed that thr RocksDB reference has been invalidated
            // and no more metric collection will be attempted against the database.
            if (nativeMetricMonitor != null) {
                nativeMetricMonitor.close();
            }

            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(kvStateInformation.values().size());

            // RocksDB's native memory management requires that *all* CFs (including default) are
            // closed before the
            // DB is closed. See:
            // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
            // Start with default CF ...
            ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamily);
            IOUtils.closeQuietly(defaultColumnFamily);

            // ... continue with the ones created by Flink...
            for (ForStDbKvStateInfo kvStateInfo : kvStateInformation.values()) {
                ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                        columnFamilyOptions, kvStateInfo.columnFamilyHandle);
                IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
            }

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            columnFamilyOptions.forEach(IOUtils::closeQuietly);

            IOUtils.closeQuietly(optionsContainer);

            kvStateInformation.clear();

            cleanInstanceBasePath();
        }
        // todo: rebase after checkpoint pr merged
        // IOUtils.closeQuietly(checkpointSnapshotStrategy);
        this.disposed = true;
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

    private void cleanInstanceBasePath() {
        LOG.info(
                "Closed RocksDB State Backend. Cleaning up RocksDB working directory {}.",
                instanceBasePath);

        try {
            FileUtils.deleteDirectory(instanceBasePath);
        } catch (IOException ex) {
            LOG.warn("Could not delete RocksDB working directory: {}", instanceBasePath, ex);
        }
    }

    // ------------------------------------------------------------------------
    //  Getters and Setters
    // ------------------------------------------------------------------------

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    @VisibleForTesting
    PriorityQueueSetFactory getPriorityQueueFactory() {
        return priorityQueueFactory;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }

    SerializedCompositeKeyBuilder<K> getSharedRocksKeyBuilder() {
        return sharedRocksKeyBuilder;
    }

    @VisibleForTesting
    boolean isDisposed() {
        return this.disposed;
    }

    /**
     * Triggers an asynchronous snapshot of the keyed state backend from RocksDB. This snapshot can
     * be canceled and is also stopped when the backend is closed through {@link #dispose()}. For
     * each backend, this method must always be called by the same thread.
     *
     * @param checkpointId The Id of the checkpoint.
     * @param timestamp The timestamp of the checkpoint.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform this checkpoint.
     * @return Future to the state handle of the snapshot data.
     * @throws Exception indicating a problem in the synchronous part of the checkpoint.
     */
    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        // TODO: implement snapshot on sync keyed state backend later, skip now.
        return DoneFuture.of(SnapshotResult.empty());
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
        // TODO: maybe do some thing when implement checkpoint
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // TODO: maybe do some thing when implement checkpoint
    }

    /**
     * Registers a k/v state information, which includes its state id, type, RocksDB column family
     * handle, and serializers.
     *
     * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the
     * global RocksDB database and the list of k/v state information. When a k/v state is first
     * requested we check here whether we already have a registered entry for that and return it
     * (after some necessary state compatibility checks) or create a new one if it does not exist.
     */
    @SuppressWarnings("unchecked")
    private <N, S extends State, SV, SEV>
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    tryRegisterKvStateInformation(
                            StateDescriptor<S, SV> stateDesc,
                            TypeSerializer<N> namespaceSerializer,
                            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
                            boolean allowFutureMetadataUpdates)
                            throws Exception {

        ForStDbKvStateInfo oldStateInfo = kvStateInformation.get(stateDesc.getName());

        TypeSerializer<SV> stateSerializer =
                (TypeSerializer<SV>)
                        TtlAwareSerializer.wrapTtlAwareSerializer(stateDesc.getSerializer());

        ForStDbKvStateInfo newRocksStateInfo;
        RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
        if (oldStateInfo != null) {
            RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo =
                    (RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo.metaInfo;

            newMetaInfo =
                    updateRestoredStateMetaInfo(
                            Tuple2.of(oldStateInfo.columnFamilyHandle, castedMetaInfo),
                            stateDesc,
                            namespaceSerializer,
                            stateSerializer);

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            newRocksStateInfo =
                    new ForStDbKvStateInfo(oldStateInfo.columnFamilyHandle, newMetaInfo);
            kvStateInformation.put(stateDesc.getName(), newRocksStateInfo);
        } else {
            newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            stateSerializer,
                            StateSnapshotTransformFactory.noTransform());

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            newRocksStateInfo =
                    ForStOperationUtils.createStateInfo(
                            newMetaInfo,
                            db,
                            columnFamilyOptionsFactory,
                            ttlCompactFiltersManager,
                            optionsContainer.getWriteBufferManagerCapacity(),
                            // Using ICloseableRegistry.NO_OP here because there is no restore in
                            // progress; created column families will be closed in dispose()
                            ICloseableRegistry.NO_OP);
            ForStOperationUtils.registerKvStateInformation(
                    this.kvStateInformation,
                    this.nativeMetricMonitor,
                    stateDesc.getName(),
                    newRocksStateInfo);
        }

        // todo: update SnapshotTransformFactory
        return Tuple2.of(newRocksStateInfo.columnFamilyHandle, newMetaInfo);
    }

    private <N, S extends State, SV>
            RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
                    Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                            oldStateInfo,
                    StateDescriptor<S, SV> stateDesc,
                    TypeSerializer<N> namespaceSerializer,
                    TypeSerializer<SV> stateSerializer)
                    throws Exception {

        RegisteredKeyValueStateBackendMetaInfo<N, SV> restoredKvStateMetaInfo = oldStateInfo.f1;

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        TypeSerializer<N> previousNamespaceSerializer =
                restoredKvStateMetaInfo.getNamespaceSerializer();

        TypeSerializerSchemaCompatibility<N> s =
                restoredKvStateMetaInfo.updateNamespaceSerializer(namespaceSerializer);
        if (s.isCompatibleAfterMigration() || s.isIncompatible()) {
            throw new StateMigrationException(
                    "The new namespace serializer ("
                            + namespaceSerializer
                            + ") must be compatible with the old namespace serializer ("
                            + previousNamespaceSerializer
                            + ").");
        }

        restoredKvStateMetaInfo.checkStateMetaInfo(stateDesc);

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        TypeSerializer<SV> previousStateSerializer = restoredKvStateMetaInfo.getStateSerializer();

        TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility =
                restoredKvStateMetaInfo.updateStateSerializer(stateSerializer);
        if (newStateSerializerCompatibility.isCompatibleAfterMigration()) {
            migrateStateValues(stateDesc, oldStateInfo);
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

    /**
     * Migrate only the state value, that is the "value" that is stored in RocksDB. We don't migrate
     * the key here, which is made up of key group, key, namespace and map key (in case of
     * MapState).
     */
    @SuppressWarnings("unchecked")
    private <N, S extends State, SV> void migrateStateValues(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> stateMetaInfo)
            throws Exception {

        if (stateDesc.getType() == StateDescriptor.Type.MAP) {
            TypeSerializerSnapshot<SV> previousSerializerSnapshot =
                    stateMetaInfo.f1.getPreviousStateSerializerSnapshot();
            checkState(
                    previousSerializerSnapshot != null,
                    "the previous serializer snapshot should exist.");
            checkState(
                    previousSerializerSnapshot instanceof MapSerializerSnapshot,
                    "previous serializer snapshot should be a MapSerializerSnapshot.");

            TypeSerializer<SV> newSerializer = stateMetaInfo.f1.getStateSerializer();
            checkState(
                    newSerializer instanceof MapSerializer,
                    "new serializer should be a MapSerializer.");

            MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) newSerializer;
            MapSerializerSnapshot<?, ?> mapSerializerSnapshot =
                    (MapSerializerSnapshot<?, ?>) previousSerializerSnapshot;
            if (!checkMapStateKeySchemaCompatibility(mapSerializerSnapshot, mapSerializer)) {
                throw new StateMigrationException(
                        "The new serializer for a MapState requires state migration in order for the job to proceed, since the key schema has changed. However, migration for MapState currently only allows value schema evolutions.");
            }
        }

        LOG.info(
                "Performing state migration for state {} because the state serializer's schema, i.e. serialization format, has changed.",
                stateDesc);

        // we need to get an actual state instance because migration is different
        // for different state types. For example, ListState needs to deal with
        // individual elements
        State state = createState(stateDesc, stateMetaInfo);
        if (!(state instanceof AbstractForStSyncState)) {
            throw new FlinkRuntimeException(
                    "State should be an AbstractForStSyncState but is " + state);
        }

        @SuppressWarnings("unchecked")
        AbstractForStSyncState<?, ?, SV> rocksDBState = (AbstractForStSyncState<?, ?, SV>) state;

        Snapshot rocksDBSnapshot = db.getSnapshot();
        try (ForStIteratorWrapper iterator =
                        ForStOperationUtils.getForStIterator(db, stateMetaInfo.f0, readOptions);
                ForStDBWriteBatchWrapper batchWriter =
                        new ForStDBWriteBatchWrapper(db, getWriteOptions(), getWriteBatchSize());
                Closeable ignored =
                        cancelStreamRegistry.registerCloseableTemporarily(
                                writeBatchWrapper.getCancelCloseable())) {
            iterator.seekToFirst();

            DataInputDeserializer serializedValueInput = new DataInputDeserializer();
            DataOutputSerializer migratedSerializedValueOutput = new DataOutputSerializer(512);
            while (iterator.isValid()) {
                serializedValueInput.setBuffer(iterator.value());

                rocksDBState.migrateSerializedValue(
                        serializedValueInput,
                        migratedSerializedValueOutput,
                        stateMetaInfo.f1.getPreviousStateSerializer(),
                        stateMetaInfo.f1.getStateSerializer());

                batchWriter.put(
                        stateMetaInfo.f0,
                        iterator.key(),
                        migratedSerializedValueOutput.getCopyOfBuffer());

                migratedSerializedValueOutput.clear();
                iterator.next();
            }
        } finally {
            db.releaseSnapshot(rocksDBSnapshot);
            rocksDBSnapshot.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static <UK> boolean checkMapStateKeySchemaCompatibility(
            MapSerializerSnapshot<?, ?> mapStateSerializerSnapshot,
            MapSerializer<?, ?> newMapStateSerializer) {
        TypeSerializerSnapshot<UK> previousKeySerializerSnapshot =
                (TypeSerializerSnapshot<UK>) mapStateSerializerSnapshot.getKeySerializerSnapshot();
        TypeSerializer<UK> newUserKeySerializer =
                (TypeSerializer<UK>) newMapStateSerializer.getKeySerializer();

        TypeSerializerSchemaCompatibility<UK> keyCompatibility =
                newUserKeySerializer
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(previousKeySerializerSnapshot);
        return keyCompatibility.isCompatibleAsIs();
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult =
                tryRegisterKvStateInformation(
                        stateDesc,
                        namespaceSerializer,
                        snapshotTransformFactory,
                        allowFutureMetadataUpdates);
        if (!allowFutureMetadataUpdates) {
            // Config compact filter only when no future metadata updates
            ttlCompactFiltersManager.configCompactFilter(
                    stateDesc, registerResult.f1.getStateSerializer());
        }

        return createState(stateDesc, registerResult);
    }

    private <N, SV, S extends State, IS extends S> IS createState(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult)
            throws Exception {
        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(
                            stateDesc, registerResult, ForStSyncKeyedStateBackend.this);
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(stateDesc, registerResult, createdState);
        }

        createdKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    /** Only visible for testing, DO NOT USE. */
    File getInstanceBasePath() {
        return instanceBasePath;
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int count = 0;

        for (ForStDbKvStateInfo metaInfo : kvStateInformation.values()) {
            // TODO maybe filterOrTransform only for k/v states
            try (ForStIteratorWrapper rocksIterator =
                    ForStOperationUtils.getForStIterator(
                            db, metaInfo.columnFamilyHandle, readOptions)) {
                rocksIterator.seekToFirst();

                while (rocksIterator.isValid()) {
                    count++;
                    rocksIterator.next();
                }
            }
        }

        return count;
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return priorityQueueFactory instanceof HeapPriorityQueueSetFactory
                && !checkpointType.isSavepoint();
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return true;
    }

    /** Rocks DB specific information about the k/v states. */
    public static class ForStDbKvStateInfo implements AutoCloseable {
        public final ColumnFamilyHandle columnFamilyHandle;
        public final RegisteredStateMetaInfoBase metaInfo;

        public ForStDbKvStateInfo(
                ColumnFamilyHandle columnFamilyHandle, RegisteredStateMetaInfoBase metaInfo) {
            this.columnFamilyHandle = columnFamilyHandle;
            this.metaInfo = metaInfo;
        }

        @Override
        public void close() throws Exception {
            this.columnFamilyHandle.close();
        }
    }

    @Nonnegative
    long getWriteBatchSize() {
        return writeBatchSize;
    }
}
