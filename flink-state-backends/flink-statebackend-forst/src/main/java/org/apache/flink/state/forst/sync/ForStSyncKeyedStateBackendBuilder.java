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

package org.apache.flink.state.forst.sync;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
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
import org.apache.flink.state.forst.ForStConfigurableOptions;
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStDBWriteBatchWrapper;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.ForStStateBackend;
import org.apache.flink.state.forst.ForStStateDataTransfer;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.restore.ForStHeapTimersFullRestoreOperation;
import org.apache.flink.state.forst.restore.ForStIncrementalRestoreOperation;
import org.apache.flink.state.forst.restore.ForStNoneRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreResult;
import org.apache.flink.state.forst.snapshot.ForStIncrementalSnapshotStrategy;
import org.apache.flink.state.forst.snapshot.ForStNativeFullSnapshotStrategy;
import org.apache.flink.state.forst.snapshot.ForStSnapshotStrategyBase;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.RocksDB;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Builder class for {@link org.apache.flink.state.forst.sync.ForStSyncKeyedStateBackend} which
 * handles all necessary initializations and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForStSyncKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {

    static final String DB_INSTANCE_DIR_STRING = "db";

    /** String that identifies the operator that owns this backend. */
    private final String operatorIdentifier;

    /** The configuration of rocksDB priorityQueue state. */
    private final ForStPriorityQueueConfig priorityQueueConfig;

    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of RocksDB option factory and predefined options. */
    private final ForStResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final Path instanceBasePath;

    /** Path where this configured instance stores its RocksDB database. */
    private final Path instanceForStDBPath;

    private final MetricGroup metricGroup;
    private final StateBackend.CustomInitializationMetrics customInitializationMetrics;

    /** True if incremental checkpointing is enabled. */
    private boolean enableIncrementalCheckpointing;

    /** RocksDB property-based and statistics-based native metrics options. */
    private ForStNativeMetricOptions nativeMetricOptions;

    private int numberOfTransferingThreads;
    private long writeBatchSize =
            ForStConfigurableOptions.WRITE_BATCH_SIZE.defaultValue().getBytes();

    private RocksDB injectedTestDB; // for testing
    // todo: checkpoint/restore related
    private boolean incrementalRestoreAsyncCompactAfterRescale = false;
    private boolean rescalingUseDeleteFilesInRange = false;

    private double overlapFractionThreshold = 0.5;
    private boolean useIngestDbRestoreMode = false;
    private ColumnFamilyHandle injectedDefaultColumnFamilyHandle; // for testing
    private AsyncExceptionHandler asyncExceptionHandler;

    public ForStSyncKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            Path instanceBasePath,
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            ForStPriorityQueueConfig priorityQueueConfig,
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
        this.instanceForStDBPath = getInstanceRocksDBPath(instanceBasePath);
        this.metricGroup = metricGroup;
        this.customInitializationMetrics = customInitializationMetrics;
        this.enableIncrementalCheckpointing = false;
        this.nativeMetricOptions = new ForStNativeMetricOptions();
        this.numberOfTransferingThreads = 4;
    }

    @VisibleForTesting
    ForStSyncKeyedStateBackendBuilder(
            String operatorIdentifier,
            ClassLoader userCodeClassLoader,
            Path instanceBasePath,
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            LocalRecoveryConfig localRecoveryConfig,
            ForStPriorityQueueConfig rocksDBPriorityQueueConfig,
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

    public ForStSyncKeyedStateBackendBuilder<K> setNativeMetricOptions(
            ForStNativeMetricOptions nativeMetricOptions) {
        this.nativeMetricOptions = nativeMetricOptions;
        return this;
    }

    public ForStSyncKeyedStateBackendBuilder<K> setWriteBatchSize(long writeBatchSize) {
        checkArgument(writeBatchSize >= 0, "Write batch size should be non negative.");
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    public static Path getInstanceRocksDBPath(Path instanceBasePath) {
        return new Path(instanceBasePath, DB_INSTANCE_DIR_STRING);
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
    public ForStSyncKeyedStateBackend<K> build() throws BackendBuildingException {
        ForStDBWriteBatchWrapper writeBatchWrapper = null;
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        ForStNativeMetricMonitor nativeMetricMonitor = null;
        CloseableRegistry cancelRegistryForBackend = new CloseableRegistry();
        LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new LinkedHashMap<>();
        RocksDB db = null;
        ForStRestoreOperation restoreOperation = null;
        CompletableFuture<Void> asyncCompactAfterRestoreFuture = null;

        ForStDBTtlCompactFiltersManager ttlCompactFiltersManager =
                new ForStDBTtlCompactFiltersManager(
                        ttlTimeProvider,
                        optionsContainer.getQueryTimeAfterNumEntries(),
                        optionsContainer.getPeriodicCompactionTime());

        ForStSnapshotStrategyBase<K, ?> checkpointStrategy = null;

        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        PriorityQueueSetFactory priorityQueueFactory;
        SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder;
        // Number of bytes required to prefix the key groups.
        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);

        try {
            // Variables for snapshot strategy when incremental checkpoint is enabled
            UUID backendUID = UUID.randomUUID();
            SortedMap<Long, Collection<HandleAndLocalPath>> materializedSstFiles = new TreeMap<>();
            long lastCompletedCheckpointId = -1L;
            prepareDirectories();
            restoreOperation = getForStDBRestoreOperation(kvStateInformation, registeredPQStates);
            ForStRestoreResult restoreResult = restoreOperation.restore();
            db = restoreResult.getDb();
            defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
            nativeMetricMonitor = restoreResult.getNativeMetricMonitor();

            writeBatchWrapper =
                    new ForStDBWriteBatchWrapper(
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
                    initializeSnapshotStrategy(
                            db,
                            rocksDBResourceGuard,
                            keySerializerProvider.currentSchemaSerializer(),
                            kvStateInformation,
                            keyGroupRange,
                            keyGroupPrefixBytes,
                            backendUID,
                            materializedSstFiles,
                            lastCompletedCheckpointId);

            // init priority queue factory
            priorityQueueFactory =
                    initPriorityQueueFactory(
                            keyGroupPrefixBytes,
                            kvStateInformation,
                            db,
                            writeBatchWrapper,
                            nativeMetricMonitor);
        } catch (Throwable e) {
            // Do clean up
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(kvStateInformation.values().size());
            IOUtils.closeQuietly(cancelRegistryForBackend);
            IOUtils.closeQuietly(writeBatchWrapper);
            IOUtils.closeQuietly(rocksDBResourceGuard);
            ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(nativeMetricMonitor);
            for (ForStOperationUtils.ForStKvStateInfo kvStateInfo : kvStateInformation.values()) {
                ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
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

            try {
                FileUtils.deleteDirectory(new File(instanceBasePath.getPath()));
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
        return new ForStSyncKeyedStateBackend<>(
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
                asyncCompactAfterRestoreFuture);
    }

    private ForStSnapshotStrategyBase<K, ?> initializeSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard forstResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull UUID backendUID,
            @Nonnull
                    SortedMap<Long, Collection<IncrementalKeyedStateHandle.HandleAndLocalPath>>
                            uploadedStateHandles,
            long lastCompletedCheckpointId)
            throws IOException {

        ForStFlinkFileSystem forStFs =
                optionsContainer.getRemoteForStPath() != null
                        ? (ForStFlinkFileSystem)
                                ForStFlinkFileSystem.get(
                                        optionsContainer.getRemoteForStPath().toUri())
                        : null;
        ForStStateDataTransfer stateTransfer =
                new ForStStateDataTransfer(ForStStateDataTransfer.DEFAULT_THREAD_NUM, forStFs);

        if (enableIncrementalCheckpointing) {
            return new ForStIncrementalSnapshotStrategy<>(
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
            return new ForStNativeFullSnapshotStrategy<>(
                    db,
                    forstResourceGuard,
                    optionsContainer,
                    keySerializer,
                    kvStateInformation,
                    keyGroupRange,
                    keyGroupPrefixBytes,
                    backendUID,
                    stateTransfer);
        }
    }

    private ForStRestoreOperation getForStDBRestoreOperation(
            LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {
        // Currently, ForStDB does not support mixing local-dir and remote-dir, and ForStDB will
        // concatenates the dfs directory with the local directory as working dir when using flink
        // env. We expect to directly use the dfs directory in flink env or local directory as
        // working dir. We will implement this in ForStDB later, but before that, we achieved this
        // by setting the dbPath to "/" when the dfs directory existed.

        if (CollectionUtil.isEmptyOrAllElementsNull(restoreStateHandles)) {
            return new ForStNoneRestoreOperation(
                    Collections.emptyMap(),
                    instanceForStDBPath,
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
                    instanceForStDBPath,
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
                    instanceForStDBPath,
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

    private PriorityQueueSetFactory initPriorityQueueFactory(
            int keyGroupPrefixBytes,
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            RocksDB db,
            ForStDBWriteBatchWrapper writeBatchWrapper,
            ForStNativeMetricMonitor nativeMetricMonitor) {
        PriorityQueueSetFactory priorityQueueFactory;
        switch (priorityQueueConfig.getPriorityQueueStateType()) {
            case HEAP:
                priorityQueueFactory = createHeapQueueFactory();
                break;
            case ForStDB:
                priorityQueueFactory =
                        new ForStDBPriorityQueueSetFactory(
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
                                priorityQueueConfig.getRocksDBPriorityQueueSetCacheSize());
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
        File baseFile = new File(instanceBasePath.getPath());
        checkAndCreateDirectory(baseFile);
        if (new File(instanceForStDBPath.getPath()).exists()) {
            // Clear the base directory when the backend is created
            // in case something crashed and the backend never reached dispose()
            FileUtils.deleteDirectory(baseFile);
        }
    }
}
