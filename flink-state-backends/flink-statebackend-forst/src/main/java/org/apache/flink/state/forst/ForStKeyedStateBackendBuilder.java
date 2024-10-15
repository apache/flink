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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendBuilder;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.restore.ForStHeapTimersFullRestoreOperation;
import org.apache.flink.state.forst.restore.ForStIncrementalRestoreOperation;
import org.apache.flink.state.forst.restore.ForStNoneRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreResult;
import org.apache.flink.state.forst.snapshot.ForStIncrementalSnapshotStrategy;
import org.apache.flink.state.forst.snapshot.ForStSnapshotStrategyBase;
import org.apache.flink.state.forst.sync.ForStPriorityQueueConfig;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builder class for {@link ForStKeyedStateBackend} which handles all necessary initializations and
 * cleanups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForStKeyedStateBackendBuilder<K>
        implements StateBackendBuilder<ForStKeyedStateBackend<K>, BackendBuildingException> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int KEY_SERIALIZER_BUFFER_START_SIZE = 32;

    private static final int VALUE_SERIALIZER_BUFFER_START_SIZE = 128;

    /** String that identifies the operator that owns this backend. */
    private final String operatorIdentifier;

    /** The configuration of rocksDB priorityQueue state. */
    private final ForStPriorityQueueConfig priorityQueueConfig;

    protected final ClassLoader userCodeClassLoader;

    protected final CloseableRegistry cancelStreamRegistry;

    private final StateSerializerProvider<K> keySerializerProvider;

    private final int numberOfKeyGroups;
    private final KeyGroupRange keyGroupRange;
    private final TtlTimeProvider ttlTimeProvider;

    private final Collection<KeyedStateHandle> restoreStateHandles;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of ForSt option factory and predefined options. */
    private final ForStResourceContainer optionsContainer;

    private final MetricGroup metricGroup;
    private final StateBackend.CustomInitializationMetrics customInitializationMetrics;

    /** True if incremental checkpointing is enabled. */
    private boolean enableIncrementalCheckpointing;

    /** ForSt property-based and statistics-based native metrics options. */
    private ForStNativeMetricOptions nativeMetricOptions;

    public ForStKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ForStPriorityQueueConfig priorityQueueConfig,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            StateBackend.CustomInitializationMetrics customInitializationMetrics,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        this.operatorIdentifier = operatorIdentifier;
        this.userCodeClassLoader = userCodeClassLoader;
        this.optionsContainer = optionsContainer;
        this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);
        this.keySerializerProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.priorityQueueConfig = priorityQueueConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.metricGroup = metricGroup;
        this.customInitializationMetrics = customInitializationMetrics;
        this.restoreStateHandles = stateHandles;
        this.nativeMetricOptions = new ForStNativeMetricOptions();
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    ForStKeyedStateBackendBuilder<K> setEnableIncrementalCheckpointing(
            boolean enableIncrementalCheckpointing) {
        this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
        return this;
    }

    ForStKeyedStateBackendBuilder<K> setNativeMetricOptions(
            ForStNativeMetricOptions nativeMetricOptions) {
        this.nativeMetricOptions = nativeMetricOptions;
        return this;
    }

    @Override
    public ForStKeyedStateBackend<K> build() throws BackendBuildingException {
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        ForStNativeMetricMonitor nativeMetricMonitor = null;

        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();

        LinkedHashMap<String, ForStKeyedStateBackend.ForStKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();

        ForStDBTtlCompactFiltersManager ttlCompactFiltersManager =
                new ForStDBTtlCompactFiltersManager(
                        ttlTimeProvider,
                        optionsContainer.getQueryTimeAfterNumEntries(),
                        optionsContainer.getPeriodicCompactionTime());

        RocksDB db = null;
        ForStRestoreOperation restoreOperation = null;
        // Number of bytes required to prefix the key groups.
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);

        ResourceGuard forstResourceGuard = new ResourceGuard();
        ForStSnapshotStrategyBase<K, ?> snapshotStrategy = null;

        // it is important that we only create the key builder after the restore, and not before;
        // restore operations may reconfigure the key serializer, so accessing the key serializer
        // only now we can be certain that the key serializer used in the builder is final.
        Supplier<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder =
                () ->
                        new SerializedCompositeKeyBuilder<>(
                                // must create new copy for each SerializedCompositeKeyBuilder
                                keySerializerProvider.currentSchemaSerializer().duplicate(),
                                keyGroupPrefixBytes,
                                KEY_SERIALIZER_BUFFER_START_SIZE);
        Supplier<DataOutputSerializer> valueSerializerView =
                () -> new DataOutputSerializer(VALUE_SERIALIZER_BUFFER_START_SIZE);
        Supplier<DataInputDeserializer> valueDeserializerView = DataInputDeserializer::new;

        UUID backendUID = UUID.randomUUID();
        PriorityQueueSetFactory priorityQueueFactory;

        try {
            optionsContainer.prepareDirectories();
            restoreOperation = getForStRestoreOperation(kvStateInformation, registeredPQStates);
            ForStRestoreResult restoreResult = restoreOperation.restore();
            db = restoreResult.getDb();
            defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
            nativeMetricMonitor = restoreResult.getNativeMetricMonitor();

            // TODO: init materializedSstFiles and lastCompletedCheckpointId when implement restore
            SortedMap<Long, Collection<IncrementalKeyedStateHandle.HandleAndLocalPath>>
                    materializedSstFiles = new TreeMap<>();
            long lastCompletedCheckpointId = -1L;

            snapshotStrategy =
                    initializeSnapshotStrategy(
                            db,
                            forstResourceGuard,
                            keySerializerProvider.currentSchemaSerializer(),
                            kvStateInformation,
                            keyGroupRange,
                            keyGroupPrefixBytes,
                            backendUID,
                            materializedSstFiles,
                            lastCompletedCheckpointId);

            priorityQueueFactory = createHeapQueueFactory();

        } catch (Throwable e) {
            // Do clean up
            IOUtils.closeQuietly(cancelStreamRegistryForBackend);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(nativeMetricMonitor);
            IOUtils.closeQuietly(db);
            // it's possible that db has been initialized but later restore steps failed
            IOUtils.closeQuietly(restoreOperation);
            try {
                optionsContainer.clearDirectories();
            } catch (Exception ex) {
                logger.warn(
                        "Failed to delete ForSt local base path {}, remote base path {}.",
                        optionsContainer.getLocalBasePath(),
                        optionsContainer.getRemoteBasePath(),
                        ex);
            }
            IOUtils.closeQuietly(optionsContainer);
            IOUtils.closeQuietly(snapshotStrategy);
            // Log and rethrow
            if (e instanceof BackendBuildingException) {
                throw (BackendBuildingException) e;
            } else {
                String errMsg = "Caught unexpected exception.";
                logger.error(errMsg, e);
                throw new BackendBuildingException(errMsg, e);
            }
        }
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        logger.info(
                "Finished building ForSt keyed state-backend at local base path: {}, remote base path: {}.",
                optionsContainer.getLocalBasePath(),
                optionsContainer.getRemoteBasePath());
        return new ForStKeyedStateBackend<>(
                backendUID,
                this.optionsContainer,
                keyGroupPrefixBytes,
                this.keySerializerProvider.currentSchemaSerializer(),
                serializedKeyBuilder,
                valueSerializerView,
                valueDeserializerView,
                db,
                kvStateInformation,
                registeredPQStates,
                columnFamilyOptionsFactory,
                defaultColumnFamilyHandle,
                snapshotStrategy,
                priorityQueueFactory,
                cancelStreamRegistryForBackend,
                nativeMetricMonitor,
                keyContext,
                ttlTimeProvider,
                ttlCompactFiltersManager);
    }

    private ForStRestoreOperation getForStRestoreOperation(
            LinkedHashMap<String, ForStKeyedStateBackend.ForStKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {
        // Currently, ForStDB does not support mixing local-dir and remote-dir, and ForStDB will
        // concatenates the dfs directory with the local directory as working dir when using flink
        // env. We expect to directly use the dfs directory in flink env or local directory as
        // working dir. We will implement this in ForStDB later, but before that, we achieved this
        // by setting the dbPath to "/" when the dfs directory existed.
        // TODO: use localForStPath as dbPath after ForSt Support mixing local-dir and remote-dir
        File instanceForStPath =
                optionsContainer.getRemoteForStPath() == null
                        ? optionsContainer.getLocalForStPath()
                        : new File("/");

        if (CollectionUtil.isEmptyOrAllElementsNull(restoreStateHandles)) {
            return new ForStNoneRestoreOperation(
                    Collections.emptyMap(),
                    instanceForStPath,
                    optionsContainer.getDbOptions(),
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup);
        }
        KeyedStateHandle firstStateHandle = restoreStateHandles.iterator().next();
        if (firstStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
            return new ForStIncrementalRestoreOperation<>(
                    operatorIdentifier,
                    keyGroupRange,
                    cancelStreamRegistry,
                    userCodeClassLoader,
                    kvStateInformation,
                    keySerializerProvider,
                    optionsContainer,
                    optionsContainer.getBasePath(),
                    instanceForStPath,
                    optionsContainer.getDbOptions(),
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    customInitializationMetrics,
                    CollectionUtil.checkedSubTypeCast(
                            restoreStateHandles, IncrementalRemoteKeyedStateHandle.class));
        } else if (priorityQueueConfig.getPriorityQueueStateType()
                == ForStStateBackend.PriorityQueueStateType.HEAP) {
            // Note: This branch can be touched after ForSt Support canonical savepoint,
            // Timers are stored as raw keyed state instead of managed keyed state now.
            return new ForStHeapTimersFullRestoreOperation<>(
                    keyGroupRange,
                    numberOfKeyGroups,
                    userCodeClassLoader,
                    kvStateInformation,
                    registeredPQStates,
                    createHeapQueueFactory(),
                    keySerializerProvider,
                    instanceForStPath,
                    optionsContainer.getDbOptions(),
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    restoreStateHandles,
                    cancelStreamRegistry);
        }

        // TODO: Support Restoring
        throw new UnsupportedOperationException("Not support restoring yet for ForStStateBackend");
    }

    private ForStSnapshotStrategyBase<K, ?> initializeSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard forstResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull
                    LinkedHashMap<String, ForStKeyedStateBackend.ForStKvStateInfo>
                            kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull UUID backendUID,
            @Nonnull
                    SortedMap<Long, Collection<IncrementalKeyedStateHandle.HandleAndLocalPath>>
                            uploadedStateHandles,
            long lastCompletedCheckpointId)
            throws IOException {

        ForStSnapshotStrategyBase<K, ?> snapshotStrategy;

        ForStFlinkFileSystem forStFs =
                optionsContainer.getRemoteForStPath() != null
                        ? (ForStFlinkFileSystem)
                                ForStFlinkFileSystem.get(
                                        optionsContainer.getRemoteForStPath().toUri())
                        : null;
        ForStStateDataTransfer stateTransfer =
                new ForStStateDataTransfer(ForStStateDataTransfer.DEFAULT_THREAD_NUM, forStFs);

        if (enableIncrementalCheckpointing) {
            snapshotStrategy =
                    new ForStIncrementalSnapshotStrategy<>(
                            db,
                            forstResourceGuard,
                            optionsContainer,
                            keySerializer,
                            kvStateInformation,
                            keyGroupRange,
                            keyGroupPrefixBytes,
                            backendUID,
                            uploadedStateHandles,
                            stateTransfer,
                            lastCompletedCheckpointId);

        } else {
            throw new UnsupportedOperationException("Not implemented yet for ForStStateBackend");
        }
        return snapshotStrategy;
    }

    private HeapPriorityQueueSetFactory createHeapQueueFactory() {
        return new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
    }
}
