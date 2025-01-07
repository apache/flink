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

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.state.rocksdb.restore.RocksDBFullRestoreOperation;
import org.apache.flink.state.rocksdb.restore.RocksDBHeapTimersFullRestoreOperation;
import org.apache.flink.state.rocksdb.restore.RocksDBIncrementalRestoreOperation;
import org.apache.flink.state.rocksdb.restore.RocksDBNoneRestoreOperation;
import org.apache.flink.state.rocksdb.restore.RocksDBRestoreOperation;
import org.apache.flink.state.rocksdb.restore.RocksDBRestoreResult;
import org.apache.flink.state.rocksdb.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.state.rocksdb.snapshot.RocksIncrementalSnapshotStrategy;
import org.apache.flink.state.rocksdb.snapshot.RocksNativeFullSnapshotStrategy;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionConfig;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionManager;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING;
import static org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder class for {@link RocksDBKeyedStateBackend} which handles all necessary initializations
 * and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class RocksDBKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {

    static final String DB_INSTANCE_DIR_STRING = "db";

    /** String that identifies the operator that owns this backend. */
    private final String operatorIdentifier;

    /** The configuration of rocksDB priorityQueue state. */
    private final RocksDBPriorityQueueConfig priorityQueueConfig;

    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of RocksDB option factory and predefined options. */
    private final RocksDBResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final File instanceBasePath;

    /** Path where this configured instance stores its RocksDB database. */
    private final File instanceRocksDBPath;

    private final MetricGroup metricGroup;
    private final StateBackend.CustomInitializationMetrics customInitializationMetrics;

    /** True if incremental checkpointing is enabled. */
    private boolean enableIncrementalCheckpointing;

    /** RocksDB property-based and statistics-based native metrics options. */
    private RocksDBNativeMetricOptions nativeMetricOptions;

    private int numberOfTransferingThreads;
    private long writeBatchSize =
            RocksDBConfigurableOptions.WRITE_BATCH_SIZE.defaultValue().getBytes();

    private RocksDB injectedTestDB; // for testing
    private boolean incrementalRestoreAsyncCompactAfterRescale =
            INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE.defaultValue();
    private boolean rescalingUseDeleteFilesInRange =
            USE_DELETE_FILES_IN_RANGE_DURING_RESCALING.defaultValue();

    private double overlapFractionThreshold = RESTORE_OVERLAP_FRACTION_THRESHOLD.defaultValue();
    private boolean useIngestDbRestoreMode = USE_INGEST_DB_RESTORE_MODE.defaultValue();
    private ColumnFamilyHandle injectedDefaultColumnFamilyHandle; // for testing
    private RocksDBStateUploader injectRocksDBStateUploader; // for testing
    private RocksDBManualCompactionConfig manualCompactionConfig =
            RocksDBManualCompactionConfig.getDefault();
    private ExecutorService ioExecutor;
    private AsyncExceptionHandler asyncExceptionHandler;

    public RocksDBKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            RocksDBPriorityQueueConfig priorityQueueConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            MetricGroup metricGroup,
            StateBackend.CustomInitializationMetrics customInitializationMetrics,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            CloseableRegistry cancelStreamRegistry) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);

        this.operatorIdentifier = operatorIdentifier;
        this.priorityQueueConfig = priorityQueueConfig;
        this.localRecoveryConfig = localRecoveryConfig;
        // ensure that we use the right merge operator, because other code relies on this
        this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);
        this.optionsContainer = optionsContainer;
        this.instanceBasePath = instanceBasePath;
        this.instanceRocksDBPath = getInstanceRocksDBPath(instanceBasePath);
        this.metricGroup = metricGroup;
        this.customInitializationMetrics = customInitializationMetrics;
        this.enableIncrementalCheckpointing = false;
        this.nativeMetricOptions = new RocksDBNativeMetricOptions();
        this.numberOfTransferingThreads =
                RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue();
    }

    @VisibleForTesting
    RocksDBKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            RocksDBPriorityQueueConfig rocksDBPriorityQueueConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            RocksDB injectedTestDB,
            ColumnFamilyHandle injectedDefaultColumnFamilyHandle,
            CloseableRegistry cancelStreamRegistry) {
        this(
                operatorIdentifier,
                userCodeClassLoader,
                instanceBasePath,
                optionsContainer,
                columnFamilyOptionsFactory,
                kvStateRegistry,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                localRecoveryConfig,
                rocksDBPriorityQueueConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                metricGroup,
                (key, value) -> {},
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);
        this.injectedTestDB = injectedTestDB;
        this.injectedDefaultColumnFamilyHandle = injectedDefaultColumnFamilyHandle;
    }

    RocksDBKeyedStateBackendBuilder<K> setEnableIncrementalCheckpointing(
            boolean enableIncrementalCheckpointing) {
        this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setNativeMetricOptions(
            RocksDBNativeMetricOptions nativeMetricOptions) {
        this.nativeMetricOptions = nativeMetricOptions;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setNumberOfTransferingThreads(
            int numberOfTransferingThreads) {
        Preconditions.checkState(
                injectRocksDBStateUploader == null,
                "numberOfTransferingThreads can be set only when injectRocksDBStateUploader is null.");
        this.numberOfTransferingThreads = numberOfTransferingThreads;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setWriteBatchSize(long writeBatchSize) {
        checkArgument(writeBatchSize >= 0, "Write batch size should be non negative.");
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setRocksDBStateUploader(
            RocksDBStateUploader rocksDBStateUploader) {
        Preconditions.checkState(
                injectRocksDBStateUploader == null, "rocksDBStateUploader can be only set once");
        Preconditions.checkState(
                numberOfTransferingThreads
                        == RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue(),
                "RocksDBStateUploader can only be set if numberOfTransferingThreads has not been manually set.");
        this.injectRocksDBStateUploader = rocksDBStateUploader;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setOverlapFractionThreshold(
            double overlapFractionThreshold) {
        this.overlapFractionThreshold = overlapFractionThreshold;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setIncrementalRestoreAsyncCompactAfterRescale(
            boolean incrementalRestoreAsyncCompactAfterRescale) {
        this.incrementalRestoreAsyncCompactAfterRescale =
                incrementalRestoreAsyncCompactAfterRescale;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setUseIngestDbRestoreMode(boolean useIngestDbRestoreMode) {
        this.useIngestDbRestoreMode = useIngestDbRestoreMode;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setRescalingUseDeleteFilesInRange(
            boolean rescalingUseDeleteFilesInRange) {
        this.rescalingUseDeleteFilesInRange = rescalingUseDeleteFilesInRange;
        return this;
    }

    RocksDBKeyedStateBackendBuilder<K> setIOExecutor(ExecutorService ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    public static File getInstanceRocksDBPath(File instanceBasePath) {
        return new File(instanceBasePath, DB_INSTANCE_DIR_STRING);
    }

    private static void checkAndCreateDirectory(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                throw new IOException("Not a directory: " + directory);
            }
        } else if (!directory.mkdirs()) {
            throw new IOException(
                    String.format("Could not create RocksDB data directory at %s.", directory));
        }
    }

    @Override
    public RocksDBKeyedStateBackend<K> build() throws BackendBuildingException {
        RocksDBWriteBatchWrapper writeBatchWrapper = null;
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        RocksDBNativeMetricMonitor nativeMetricMonitor = null;
        CloseableRegistry cancelRegistryForBackend = new CloseableRegistry();
        LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();
        RocksDB db = null;
        RocksDBRestoreOperation restoreOperation = null;
        CompletableFuture<Void> asyncCompactAfterRestoreFuture = null;

        RocksDbTtlCompactFiltersManager ttlCompactFiltersManager =
                new RocksDbTtlCompactFiltersManager(
                        ttlTimeProvider,
                        optionsContainer.getQueryTimeAfterNumEntries(),
                        optionsContainer.getPeriodicCompactionTime());

        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        RocksDBSnapshotStrategyBase<K, ?> checkpointStrategy = null;
        PriorityQueueSetFactory priorityQueueFactory;
        SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;
        // Number of bytes required to prefix the key groups.
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);
        RocksDBManualCompactionManager manualCompactionManager;

        try {
            // Variables for snapshot strategy when incremental checkpoint is enabled
            UUID backendUID = UUID.randomUUID();
            SortedMap<Long, Collection<HandleAndLocalPath>> materializedSstFiles = new TreeMap<>();
            long lastCompletedCheckpointId = -1L;
            if (injectedTestDB != null) {
                db = injectedTestDB;
                defaultColumnFamilyHandle = injectedDefaultColumnFamilyHandle;
                nativeMetricMonitor =
                        nativeMetricOptions.isEnabled()
                                ? new RocksDBNativeMetricMonitor(
                                        nativeMetricOptions, metricGroup, db, null)
                                : null;
            } else {
                prepareDirectories();
                restoreOperation =
                        getRocksDBRestoreOperation(
                                keyGroupPrefixBytes,
                                rocksDBResourceGuard,
                                cancelStreamRegistry,
                                cancelRegistryForBackend,
                                kvStateInformation,
                                registeredPQStates,
                                ttlCompactFiltersManager);
                RocksDBRestoreResult restoreResult = restoreOperation.restore();
                db = restoreResult.getDb();
                defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
                nativeMetricMonitor = restoreResult.getNativeMetricMonitor();
                if (ioExecutor != null) {
                    asyncCompactAfterRestoreFuture =
                            restoreResult
                                    .getAsyncCompactTaskAfterRestore()
                                    .map((task) -> CompletableFuture.runAsync(task, ioExecutor))
                                    .orElse(null);
                }
                if (restoreOperation instanceof RocksDBIncrementalRestoreOperation) {
                    backendUID = restoreResult.getBackendUID();
                    materializedSstFiles = restoreResult.getRestoredSstFiles();
                    lastCompletedCheckpointId = restoreResult.getLastCompletedCheckpointId();
                }
            }

            writeBatchWrapper =
                    new RocksDBWriteBatchWrapper(
                            db, optionsContainer.getWriteOptions(), writeBatchSize);

            // it is important that we only create the key builder after the restore, and not
            // before;
            // restore operations may reconfigure the key serializer, so accessing the key
            // serializer
            // only now we can be certain that the key serializer used in the builder is final.
            sharedRocksKeyBuilder =
                    new SerializedCompositeKeyBuilder<>(
                            keySerializerProvider.currentSchemaSerializer(),
                            keyGroupPrefixBytes,
                            32);
            // init snapshot strategy after db is assured to be initialized
            checkpointStrategy =
                    initializeSavepointAndCheckpointStrategies(
                            rocksDBResourceGuard,
                            kvStateInformation,
                            keyGroupPrefixBytes,
                            db,
                            backendUID,
                            materializedSstFiles,
                            lastCompletedCheckpointId);
            // init priority queue factory
            manualCompactionManager =
                    RocksDBManualCompactionManager.create(db, manualCompactionConfig, ioExecutor);
            priorityQueueFactory =
                    initPriorityQueueFactory(
                            keyGroupPrefixBytes,
                            kvStateInformation,
                            db,
                            writeBatchWrapper,
                            nativeMetricMonitor,
                            manualCompactionManager);
        } catch (Throwable e) {
            // Do clean up
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(kvStateInformation.values().size());
            IOUtils.closeQuietly(cancelRegistryForBackend);
            IOUtils.closeQuietly(writeBatchWrapper);
            IOUtils.closeQuietly(rocksDBResourceGuard);
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(nativeMetricMonitor);
            for (RocksDBKeyedStateBackend.RocksDbKvStateInfo kvStateInfo :
                    kvStateInformation.values()) {
                RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                        columnFamilyOptions, kvStateInfo.columnFamilyHandle);
                IOUtils.closeQuietly(kvStateInfo.columnFamilyHandle);
            }
            IOUtils.closeQuietly(db);
            // it's possible that db has been initialized but later restore steps failed
            IOUtils.closeQuietly(restoreOperation);
            IOUtils.closeAllQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(optionsContainer);
            ttlCompactFiltersManager.disposeAndClearRegisteredCompactionFactories();
            kvStateInformation.clear();
            IOUtils.closeQuietly(checkpointStrategy);
            try {
                FileUtils.deleteDirectory(instanceBasePath);
            } catch (Exception ex) {
                logger.warn("Failed to delete base path for RocksDB: " + instanceBasePath, ex);
            }
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
        logger.info("Finished building RocksDB keyed state-backend at {}.", instanceBasePath);
        return new RocksDBKeyedStateBackend<>(
                this.userCodeClassLoader,
                this.instanceBasePath,
                this.optionsContainer,
                columnFamilyOptionsFactory,
                this.kvStateRegistry,
                this.keySerializerProvider.currentSchemaSerializer(),
                this.executionConfig,
                this.ttlTimeProvider,
                latencyTrackingStateConfig,
                db,
                kvStateInformation,
                registeredPQStates,
                keyGroupPrefixBytes,
                cancelRegistryForBackend,
                this.keyGroupCompressionDecorator,
                rocksDBResourceGuard,
                checkpointStrategy,
                writeBatchWrapper,
                defaultColumnFamilyHandle,
                nativeMetricMonitor,
                sharedRocksKeyBuilder,
                priorityQueueFactory,
                ttlCompactFiltersManager,
                keyContext,
                writeBatchSize,
                asyncCompactAfterRestoreFuture,
                manualCompactionManager);
    }

    private RocksDBRestoreOperation getRocksDBRestoreOperation(
            int keyGroupPrefixBytes,
            ResourceGuard rocksDBResourceGuard,
            CloseableRegistry cancelStreamRegistryForRestore,
            CloseableRegistry cancelRegistryForBackend,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {
        DBOptions dbOptions = optionsContainer.getDbOptions();
        if (CollectionUtil.isEmptyOrAllElementsNull(restoreStateHandles)) {
            return new RocksDBNoneRestoreOperation<>(
                    kvStateInformation,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    ttlCompactFiltersManager,
                    optionsContainer.getWriteBufferManagerCapacity());
        }
        KeyedStateHandle firstStateHandle = restoreStateHandles.iterator().next();
        if (firstStateHandle instanceof IncrementalKeyedStateHandle) {
            return new RocksDBIncrementalRestoreOperation<>(
                    operatorIdentifier,
                    keyGroupRange,
                    keyGroupPrefixBytes,
                    numberOfTransferingThreads,
                    rocksDBResourceGuard,
                    cancelStreamRegistryForRestore,
                    cancelRegistryForBackend,
                    userCodeClassLoader,
                    kvStateInformation,
                    keySerializerProvider,
                    instanceBasePath,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    customInitializationMetrics,
                    CollectionUtil.checkedSubTypeCast(
                            restoreStateHandles, IncrementalKeyedStateHandle.class),
                    ttlCompactFiltersManager,
                    writeBatchSize,
                    optionsContainer.getWriteBufferManagerCapacity(),
                    overlapFractionThreshold,
                    useIngestDbRestoreMode,
                    incrementalRestoreAsyncCompactAfterRescale,
                    rescalingUseDeleteFilesInRange,
                    ioExecutor,
                    asyncExceptionHandler);
        } else if (priorityQueueConfig.getPriorityQueueStateType()
                == EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP) {
            return new RocksDBHeapTimersFullRestoreOperation<>(
                    keyGroupRange,
                    numberOfKeyGroups,
                    userCodeClassLoader,
                    kvStateInformation,
                    registeredPQStates,
                    createHeapQueueFactory(),
                    keySerializerProvider,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    restoreStateHandles,
                    ttlCompactFiltersManager,
                    writeBatchSize,
                    optionsContainer.getWriteBufferManagerCapacity(),
                    cancelStreamRegistry);
        } else {
            return new RocksDBFullRestoreOperation<>(
                    keyGroupRange,
                    userCodeClassLoader,
                    kvStateInformation,
                    keySerializerProvider,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup,
                    restoreStateHandles,
                    ttlCompactFiltersManager,
                    writeBatchSize,
                    optionsContainer.getWriteBufferManagerCapacity(),
                    cancelStreamRegistry);
        }
    }

    private RocksDBSnapshotStrategyBase<K, ?> initializeSavepointAndCheckpointStrategies(
            ResourceGuard rocksDBResourceGuard,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            int keyGroupPrefixBytes,
            RocksDB db,
            UUID backendUID,
            SortedMap<Long, Collection<HandleAndLocalPath>> materializedSstFiles,
            long lastCompletedCheckpointId) {
        RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy;
        RocksDBStateUploader stateUploader =
                injectRocksDBStateUploader == null
                        ? new RocksDBStateUploader(
                                RocksDBStateDataTransferHelper.forThreadNumIfSpecified(
                                        numberOfTransferingThreads, ioExecutor))
                        : injectRocksDBStateUploader;
        if (enableIncrementalCheckpointing) {
            checkpointSnapshotStrategy =
                    new RocksIncrementalSnapshotStrategy<>(
                            db,
                            rocksDBResourceGuard,
                            keySerializerProvider.currentSchemaSerializer(),
                            kvStateInformation,
                            keyGroupRange,
                            keyGroupPrefixBytes,
                            localRecoveryConfig,
                            instanceBasePath,
                            backendUID,
                            materializedSstFiles,
                            stateUploader,
                            lastCompletedCheckpointId);
        } else {
            checkpointSnapshotStrategy =
                    new RocksNativeFullSnapshotStrategy<>(
                            db,
                            rocksDBResourceGuard,
                            keySerializerProvider.currentSchemaSerializer(),
                            kvStateInformation,
                            keyGroupRange,
                            keyGroupPrefixBytes,
                            localRecoveryConfig,
                            instanceBasePath,
                            backendUID,
                            stateUploader);
        }
        return checkpointSnapshotStrategy;
    }

    private PriorityQueueSetFactory initPriorityQueueFactory(
            int keyGroupPrefixBytes,
            Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            RocksDB db,
            RocksDBWriteBatchWrapper writeBatchWrapper,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            RocksDBManualCompactionManager manualCompactionManager) {
        PriorityQueueSetFactory priorityQueueFactory;
        switch (priorityQueueConfig.getPriorityQueueStateType()) {
            case HEAP:
                priorityQueueFactory = createHeapQueueFactory();
                break;
            case ROCKSDB:
                priorityQueueFactory =
                        new RocksDBPriorityQueueSetFactory(
                                keyGroupRange,
                                keyGroupPrefixBytes,
                                numberOfKeyGroups,
                                kvStateInformation,
                                db,
                                optionsContainer.getReadOptions(),
                                writeBatchWrapper,
                                nativeMetricMonitor,
                                columnFamilyOptionsFactory,
                                optionsContainer.getWriteBufferManagerCapacity(),
                                priorityQueueConfig.getRocksDBPriorityQueueSetCacheSize(),
                                manualCompactionManager);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown priority queue state type: "
                                + priorityQueueConfig.getPriorityQueueStateType());
        }
        return priorityQueueFactory;
    }

    private HeapPriorityQueueSetFactory createHeapQueueFactory() {
        return new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
    }

    private void prepareDirectories() throws IOException {
        checkAndCreateDirectory(instanceBasePath);
        if (instanceRocksDBPath.exists()) {
            // Clear the base directory when the backend is created
            // in case something crashed and the backend never reached dispose()
            FileUtils.deleteDirectory(instanceBasePath);
        }
    }

    public RocksDBKeyedStateBackendBuilder<K> setManualCompactionConfig(
            RocksDBManualCompactionConfig manualCompactionConfig) {
        this.manualCompactionConfig = checkNotNull(manualCompactionConfig);
        return this;
    }

    public RocksDBKeyedStateBackendBuilder<K> setAsyncExceptionHandler(
            AsyncExceptionHandler asyncExceptionHandler) {
        this.asyncExceptionHandler = asyncExceptionHandler;
        return this;
    }
}
