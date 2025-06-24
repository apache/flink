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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateBackend.CustomInitializationMetrics;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.state.rocksdb.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.apache.flink.state.rocksdb.RocksDBOperationUtils;
import org.apache.flink.state.rocksdb.RocksDBStateDataTransferHelper;
import org.apache.flink.state.rocksdb.RocksDBStateDownloader;
import org.apache.flink.state.rocksdb.RocksDBWriteBatchWrapper;
import org.apache.flink.state.rocksdb.RocksIteratorWrapper;
import org.apache.flink.state.rocksdb.StateHandleDownloadSpec;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.RunnableWithException;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.MetricNames.DOWNLOAD_STATE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.RESTORE_ASYNC_COMPACTION_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.RESTORE_STATE_DURATION;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/** Encapsulates the process of restoring a RocksDB instance from an incremental snapshot. */
public class RocksDBIncrementalRestoreOperation<K> implements RocksDBRestoreOperation {

    private static final Logger logger =
            LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

    @SuppressWarnings("unchecked")
    private static final Class<? extends IncrementalKeyedStateHandle>[]
            EXPECTED_STATE_HANDLE_CLASSES =
                    new Class[] {
                        IncrementalRemoteKeyedStateHandle.class,
                        IncrementalLocalKeyedStateHandle.class
                    };

    private final String operatorIdentifier;
    private final SortedMap<Long, Collection<HandleAndLocalPath>> restoredSstFiles;
    private final RocksDBHandle rocksHandle;
    private final Collection<IncrementalKeyedStateHandle> restoreStateHandles;

    /**
     * This registry will be closed after restore and should only contain Closeables that are closed
     * by the end of the restore operation.
     */
    private final CloseableRegistry cancelStreamRegistryForRestore;

    /**
     * This registry will only be closed when the created backend is closed and should be used for
     * all Closeables that are closed at some later point after restore.
     */
    private final CloseableRegistry cancelRegistryForBackend;

    private final KeyGroupRange keyGroupRange;
    private final File instanceBasePath;
    private final int numberOfTransferringThreads;
    private final int keyGroupPrefixBytes;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final ClassLoader userCodeClassLoader;
    private final CustomInitializationMetrics customInitializationMetrics;
    private final ResourceGuard dbResourceGuard;

    private long lastCompletedCheckpointId;
    private UUID backendUID;
    private final long writeBatchSize;
    private final double overlapFractionThreshold;

    private boolean isKeySerializerCompatibilityChecked;

    private final boolean useIngestDbRestoreMode;

    private final boolean asyncCompactAfterRescale;

    private final boolean useDeleteFilesInRange;

    private final ExecutorService ioExecutor;

    private final AsyncExceptionHandler asyncExceptionHandler;

    public RocksDBIncrementalRestoreOperation(
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int numberOfTransferringThreads,
            ResourceGuard dbResourceGuard,
            CloseableRegistry cancelStreamRegistryForRestore,
            CloseableRegistry cancelRegistryForBackend,
            ClassLoader userCodeClassLoader,
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            File instanceBasePath,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            CustomInitializationMetrics customInitializationMetrics,
            @Nonnull Collection<IncrementalKeyedStateHandle> restoreStateHandles,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity,
            double overlapFractionThreshold,
            boolean useIngestDbRestoreMode,
            boolean asyncCompactAfterRescale,
            boolean useDeleteFilesInRange,
            ExecutorService ioExecutor,
            AsyncExceptionHandler asyncExceptionHandler) {
        this.rocksHandle =
                new RocksDBHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
        this.operatorIdentifier = operatorIdentifier;
        this.restoredSstFiles = new TreeMap<>();
        this.lastCompletedCheckpointId = -1L;
        this.backendUID = UUID.randomUUID();
        this.writeBatchSize = writeBatchSize;
        this.overlapFractionThreshold = overlapFractionThreshold;
        this.customInitializationMetrics = customInitializationMetrics;
        this.restoreStateHandles = restoreStateHandles;
        this.dbResourceGuard = dbResourceGuard;
        this.cancelStreamRegistryForRestore = cancelStreamRegistryForRestore;
        this.cancelRegistryForBackend = cancelRegistryForBackend;
        this.keyGroupRange = keyGroupRange;
        this.instanceBasePath = instanceBasePath;
        this.numberOfTransferringThreads = numberOfTransferringThreads;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keySerializerProvider = keySerializerProvider;
        this.userCodeClassLoader = userCodeClassLoader;
        this.useIngestDbRestoreMode = useIngestDbRestoreMode;
        this.asyncCompactAfterRescale = asyncCompactAfterRescale;
        this.useDeleteFilesInRange = useDeleteFilesInRange;
        this.ioExecutor = ioExecutor;
        this.asyncExceptionHandler = asyncExceptionHandler;
    }

    /**
     * Root method that branches for different implementations of {@link
     * IncrementalKeyedStateHandle}.
     */
    @Override
    public RocksDBRestoreResult restore() throws Exception {

        if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
            return null;
        }

        logger.info(
                "Starting RocksDB incremental recovery in operator {} "
                        + "target key-group range {}. Use IngestDB={}, Use AsyncCompaction={}, State Handles={}",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval(),
                useIngestDbRestoreMode,
                asyncCompactAfterRescale,
                restoreStateHandles);

        final List<StateHandleDownloadSpec> allDownloadSpecs =
                new ArrayList<>(restoreStateHandles.size());

        final List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles =
                new ArrayList<>(restoreStateHandles.size());

        final Path absolutInstanceBasePath = instanceBasePath.getAbsoluteFile().toPath();

        try {
            runAndReportDuration(
                    () ->
                            makeAllStateHandlesLocal(
                                    absolutInstanceBasePath,
                                    localKeyedStateHandles,
                                    allDownloadSpecs),
                    DOWNLOAD_STATE_DURATION);

            runAndReportDuration(
                    () -> restoreFromLocalState(localKeyedStateHandles), RESTORE_STATE_DURATION);

            logger.info(
                    "Finished RocksDB incremental recovery in operator {} with "
                            + "target key-group range range {}.",
                    operatorIdentifier,
                    keyGroupRange.prettyPrintInterval());

            return new RocksDBRestoreResult(
                    this.rocksHandle.getDb(),
                    this.rocksHandle.getDefaultColumnFamilyHandle(),
                    this.rocksHandle.getNativeMetricMonitor(),
                    lastCompletedCheckpointId,
                    backendUID,
                    restoredSstFiles,
                    createAsyncCompactionTask());
        } finally {
            // Cleanup all download directories
            allDownloadSpecs.stream()
                    .map(StateHandleDownloadSpec::getDownloadDestination)
                    .forEach(this::cleanUpPathQuietly);
        }
    }

    @Nullable
    private Runnable createAsyncCompactionTask() {

        if (!asyncCompactAfterRescale) {
            return null;
        }

        return () -> {
            long t = System.currentTimeMillis();
            logger.info(
                    "Starting async compaction after restore for backend {} in operator {}",
                    backendUID,
                    operatorIdentifier);
            try {
                RunnableWithException asyncRangeCompactionTask =
                        RocksDBIncrementalCheckpointUtils.createAsyncRangeCompactionTask(
                                rocksHandle.getDb(),
                                rocksHandle.getColumnFamilyHandles(),
                                keyGroupPrefixBytes,
                                keyGroupRange,
                                dbResourceGuard,
                                // This task will be owned by the backend's lifecycle because it
                                // continues to exist after restore is completed.
                                cancelRegistryForBackend);
                runAndReportDuration(asyncRangeCompactionTask, RESTORE_ASYNC_COMPACTION_DURATION);
                logger.info(
                        "Completed async compaction after restore for backend {} in operator {} after {} ms.",
                        backendUID,
                        operatorIdentifier,
                        System.currentTimeMillis() - t);
            } catch (Throwable throwable) {
                asyncExceptionHandler.handleAsyncException(
                        String.format(
                                "Failed async compaction after restore for backend {} in operator {} after {} ms.",
                                backendUID,
                                operatorIdentifier,
                                System.currentTimeMillis() - t),
                        throwable);
            }
        };
    }

    private void restoreFromLocalState(
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles) throws Exception {
        if (localKeyedStateHandles.size() == 1) {
            // This happens if we don't rescale and for some scale out scenarios.
            initBaseDBFromSingleStateHandle(localKeyedStateHandles.get(0));
        } else {
            // This happens for all scale ins and some scale outs.
            restoreFromMultipleStateHandles(localKeyedStateHandles);
        }
    }

    /**
     * Downloads and converts all {@link IncrementalRemoteKeyedStateHandle}s to {@link
     * IncrementalLocalKeyedStateHandle}s.
     *
     * @param absolutInstanceBasePath the base path of the restoring DB instance as absolute path.
     * @param localKeyedStateHandlesOut the output parameter for the created {@link
     *     IncrementalLocalKeyedStateHandle}s.
     * @param allDownloadSpecsOut output parameter for the created download specs.
     * @throws Exception if an unexpected state handle type is passed as argument.
     */
    private void makeAllStateHandlesLocal(
            Path absolutInstanceBasePath,
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandlesOut,
            List<StateHandleDownloadSpec> allDownloadSpecsOut)
            throws Exception {
        // Prepare and collect all the download request to pull remote state to a local directory
        for (IncrementalKeyedStateHandle stateHandle : restoreStateHandles) {
            if (stateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                StateHandleDownloadSpec downloadRequest =
                        new StateHandleDownloadSpec(
                                (IncrementalRemoteKeyedStateHandle) stateHandle,
                                absolutInstanceBasePath.resolve(UUID.randomUUID().toString()));
                allDownloadSpecsOut.add(downloadRequest);
            } else if (stateHandle instanceof IncrementalLocalKeyedStateHandle) {
                localKeyedStateHandlesOut.add((IncrementalLocalKeyedStateHandle) stateHandle);
            } else {
                throw unexpectedStateHandleException(
                        EXPECTED_STATE_HANDLE_CLASSES, stateHandle.getClass());
            }
        }

        allDownloadSpecsOut.stream()
                .map(StateHandleDownloadSpec::createLocalStateHandleForDownloadedState)
                .forEach(localKeyedStateHandlesOut::add);

        transferRemoteStateToLocalDirectory(allDownloadSpecsOut);
    }

    /**
     * Initializes the base DB that we restore from a single local state handle.
     *
     * @param stateHandle the state handle to restore the base DB from.
     * @throws Exception on any error during restore.
     */
    private void initBaseDBFromSingleStateHandle(IncrementalLocalKeyedStateHandle stateHandle)
            throws Exception {

        logger.info(
                "Starting opening base RocksDB instance in operator {} with target key-group range {} from state handle {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval(),
                stateHandle);

        // Restore base DB from selected initial handle
        restoreBaseDBFromLocalState(stateHandle);

        KeyGroupRange stateHandleKeyGroupRange = stateHandle.getKeyGroupRange();

        // Check if the key-groups range has changed.
        if (Objects.equals(stateHandleKeyGroupRange, keyGroupRange)) {
            // This is the case if we didn't rescale, so we can restore all the info from the
            // previous backend instance (backend id and incremental checkpoint history).
            restorePreviousIncrementalFilesStatus(stateHandle);
        } else {
            // If the key-groups don't match, this was a scale out, and we need to clip the
            // key-groups range of the db to the target range for this backend.
            try {
                RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
                        this.rocksHandle.getDb(),
                        this.rocksHandle.getColumnFamilyHandles(),
                        keyGroupRange,
                        stateHandleKeyGroupRange,
                        keyGroupPrefixBytes,
                        useDeleteFilesInRange);
            } catch (RocksDBException e) {
                String errMsg = "Failed to clip DB after initialization.";
                logger.error(errMsg, e);
                throw new BackendBuildingException(errMsg, e);
            }
        }
        logger.info(
                "Finished opening base RocksDB instance in operator {} with target key-group range {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval());
    }

    /**
     * Initializes the base DB that we restore from a list of multiple local state handles.
     *
     * @param localKeyedStateHandles the list of state handles to restore the base DB from.
     * @throws Exception on any error during restore.
     */
    private void restoreFromMultipleStateHandles(
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles) throws Exception {

        logger.info(
                "Starting to restore backend with range {} in operator {} from multiple state handles {} with useIngestDbRestoreMode = {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                localKeyedStateHandles,
                useIngestDbRestoreMode);

        byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
        CompositeKeySerializationUtils.serializeKeyGroup(
                keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

        byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
        CompositeKeySerializationUtils.serializeKeyGroup(
                keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

        if (useIngestDbRestoreMode) {
            // Optimized path for merging multiple handles with Ingest/Clip
            mergeStateHandlesWithClipAndIngest(
                    localKeyedStateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
        } else {
            // Optimized path for single handle and legacy path for merging multiple handles.
            mergeStateHandlesWithCopyFromTemporaryInstance(
                    localKeyedStateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
        }

        logger.info(
                "Completed restoring backend with range {} in operator {} from multiple state handles with useIngestDbRestoreMode = {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                useIngestDbRestoreMode);
    }

    /**
     * Restores the base DB by merging multiple state handles into one. This method first checks if
     * all data to import is in the expected key-groups range and then uses import/export.
     * Otherwise, this method falls back to copying the data using a temporary DB.
     *
     * @param localKeyedStateHandles the list of state handles to restore the base DB from.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any restore error.
     */
    private void mergeStateHandlesWithClipAndIngest(
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        final Path absolutInstanceBasePath = instanceBasePath.getAbsoluteFile().toPath();
        final Path exportCfBasePath = absolutInstanceBasePath.resolve("export-cfs");
        Files.createDirectories(exportCfBasePath);

        final Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                exportedColumnFamilyMetaData = new HashMap<>(localKeyedStateHandles.size());

        final List<IncrementalLocalKeyedStateHandle> notImportableHandles =
                new ArrayList<>(localKeyedStateHandles.size());

        try {

            KeyGroupRange exportedSstKeyGroupsRange =
                    exportColumnFamiliesWithSstDataInKeyGroupsRange(
                            exportCfBasePath,
                            localKeyedStateHandles,
                            exportedColumnFamilyMetaData,
                            notImportableHandles);

            if (exportedColumnFamilyMetaData.isEmpty()) {
                // Nothing coule be exported, so we fall back to
                // #mergeStateHandlesWithCopyFromTemporaryInstance
                mergeStateHandlesWithCopyFromTemporaryInstance(
                        notImportableHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
            } else {
                // We initialize the base DB by importing all the exported data.
                initBaseDBFromColumnFamilyImports(
                        exportedColumnFamilyMetaData, exportedSstKeyGroupsRange);
                // Copy data from handles that we couldn't directly import using temporary
                // instances.
                copyToBaseDBUsingTempDBs(
                        notImportableHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
            }
        } finally {
            // Close native RocksDB objects
            exportedColumnFamilyMetaData.values().forEach(IOUtils::closeAllQuietly);
            // Cleanup export base directory
            cleanUpPathQuietly(exportCfBasePath);
        }
    }

    /**
     * Prepares the data for importing by exporting from temporary RocksDB instances. We can only
     * import data that does not exceed the target key-groups range and skip state handles that
     * exceed their range.
     *
     * @param exportCfBasePath the base path for the export files.
     * @param localKeyedStateHandles the state handles to prepare for import.
     * @param exportedColumnFamiliesOut output parameter for the metadata of completed exports.
     * @param skipped output parameter for state handles that could not be exported because the data
     *     exceeded the proclaimed range.
     * @return the total key-groups range of the exported data.
     * @throws Exception on any export error.
     */
    private KeyGroupRange exportColumnFamiliesWithSstDataInKeyGroupsRange(
            Path exportCfBasePath,
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles,
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                    exportedColumnFamiliesOut,
            List<IncrementalLocalKeyedStateHandle> skipped)
            throws Exception {

        logger.info(
                "Starting restore export for backend with range {} in operator {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        int minExportKeyGroup = Integer.MAX_VALUE;
        int maxExportKeyGroup = Integer.MIN_VALUE;
        int index = 0;
        for (IncrementalLocalKeyedStateHandle stateHandle : localKeyedStateHandles) {

            final String logLineSuffix =
                    " for state handle at index "
                            + index
                            + " with proclaimed key-group range "
                            + stateHandle.getKeyGroupRange().prettyPrintInterval()
                            + " for backend with range "
                            + keyGroupRange.prettyPrintInterval()
                            + " in operator "
                            + operatorIdentifier
                            + ".";

            logger.debug("Opening temporary database" + logLineSuffix);
            try (RestoredDBInstance tmpRestoreDBInfo =
                    restoreTempDBInstanceFromLocalState(stateHandle)) {

                List<ColumnFamilyHandle> tmpColumnFamilyHandles =
                        tmpRestoreDBInfo.columnFamilyHandles;

                logger.debug("Checking actual keys of sst files" + logLineSuffix);

                // Check if the data in all SST files referenced in the handle is within the
                // proclaimed key-groups range of the handle.
                RocksDBIncrementalCheckpointUtils.RangeCheckResult rangeCheckResult =
                        RocksDBIncrementalCheckpointUtils.checkSstDataAgainstKeyGroupRange(
                                tmpRestoreDBInfo.db,
                                keyGroupPrefixBytes,
                                stateHandle.getKeyGroupRange());

                logger.info("{}" + logLineSuffix, rangeCheckResult);

                if (rangeCheckResult.allInRange()) {

                    logger.debug("Start exporting" + logLineSuffix);

                    List<RegisteredStateMetaInfoBase> registeredStateMetaInfoBases =
                            tmpRestoreDBInfo.stateMetaInfoSnapshots.stream()
                                    .map(RegisteredStateMetaInfoBase::fromMetaInfoSnapshot)
                                    .collect(Collectors.toList());

                    // Export all the Column Families and store the result in
                    // exportedColumnFamiliesOut
                    RocksDBIncrementalCheckpointUtils.exportColumnFamilies(
                            tmpRestoreDBInfo.db,
                            tmpColumnFamilyHandles,
                            registeredStateMetaInfoBases,
                            exportCfBasePath,
                            exportedColumnFamiliesOut);

                    minExportKeyGroup =
                            Math.min(
                                    minExportKeyGroup,
                                    stateHandle.getKeyGroupRange().getStartKeyGroup());
                    maxExportKeyGroup =
                            Math.max(
                                    maxExportKeyGroup,
                                    stateHandle.getKeyGroupRange().getEndKeyGroup());

                    logger.debug("Done exporting" + logLineSuffix);
                } else {
                    // Actual key range in files exceeds proclaimed range, cannot import. We
                    // will copy this handle using a temporary DB later.
                    skipped.add(stateHandle);
                    logger.debug("Skipped export" + logLineSuffix);
                }
            }

            ++index;
        }

        KeyGroupRange exportedKeyGroupsRange =
                minExportKeyGroup <= maxExportKeyGroup
                        ? new KeyGroupRange(minExportKeyGroup, maxExportKeyGroup)
                        : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

        logger.info(
                "Completed restore export for backend with range {} in operator {}. {} exported handles with overall exported range {}. {} Skipped handles: {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                localKeyedStateHandles.size() - skipped.size(),
                exportedKeyGroupsRange.prettyPrintInterval(),
                skipped.size(),
                skipped);

        return exportedKeyGroupsRange;
    }

    /**
     * Helper method that merges the data from multiple state handles into the restoring base DB by
     * the help of copying through temporary RocksDB instances.
     *
     * @param localKeyedStateHandles the state handles to merge into the base DB.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any merge error.
     */
    private void mergeStateHandlesWithCopyFromTemporaryInstance(
            List<IncrementalLocalKeyedStateHandle> localKeyedStateHandles,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        logger.info(
                "Starting to merge state for backend with range {} in operator {} from multiple state handles using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        // Choose the best state handle for the initial DB and remove it from the list
        final IncrementalLocalKeyedStateHandle selectedInitialHandle =
                localKeyedStateHandles.remove(
                        RocksDBIncrementalCheckpointUtils.findTheBestStateHandleForInitial(
                                localKeyedStateHandles, keyGroupRange, overlapFractionThreshold));

        Preconditions.checkNotNull(selectedInitialHandle);

        // Init the base DB instance with the initial state
        initBaseDBFromSingleStateHandle(selectedInitialHandle);

        // Copy remaining handles using temporary RocksDB instances
        copyToBaseDBUsingTempDBs(
                localKeyedStateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);

        logger.info(
                "Completed merging state for backend with range {} in operator {} from multiple state handles using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);
    }

    /**
     * Initializes the base DB by importing from previously exported data.
     *
     * @param exportedColumnFamilyMetaData the export (meta) data.
     * @param exportKeyGroupRange the total key-groups range of the exported data.
     * @throws Exception on import error.
     */
    private void initBaseDBFromColumnFamilyImports(
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                    exportedColumnFamilyMetaData,
            KeyGroupRange exportKeyGroupRange)
            throws Exception {

        // We initialize the base DB by importing all the exported data.
        logger.info(
                "Starting to import exported state handles for backend with range {} in operator {} using Clip/Ingest DB with exported range {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                exportKeyGroupRange.prettyPrintInterval());
        rocksHandle.openDB();
        for (Map.Entry<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>> entry :
                exportedColumnFamilyMetaData.entrySet()) {
            rocksHandle.registerStateColumnFamilyHandleWithImport(
                    entry.getKey(), entry.getValue(), cancelStreamRegistryForRestore);
        }

        // Use Range delete to clip the temp db to the target range of the backend
        RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
                rocksHandle.getDb(),
                rocksHandle.getColumnFamilyHandles(),
                keyGroupRange,
                exportKeyGroupRange,
                keyGroupPrefixBytes,
                useDeleteFilesInRange);

        logger.info(
                "Completed importing exported state handles for backend with range {} in operator {} using Clip/Ingest DB.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);
    }

    /**
     * Restores the checkpointing status and state for this backend. This can only be done if the
     * backend was not rescaled and is therefore identical to the source backend in the previous
     * run.
     *
     * @param localKeyedStateHandle the single state handle from which the backend is restored.
     */
    private void restorePreviousIncrementalFilesStatus(
            IncrementalKeyedStateHandle localKeyedStateHandle) {
        backendUID = localKeyedStateHandle.getBackendIdentifier();
        restoredSstFiles.put(
                localKeyedStateHandle.getCheckpointId(),
                localKeyedStateHandle.getSharedStateHandles());
        lastCompletedCheckpointId = localKeyedStateHandle.getCheckpointId();
        logger.info(
                "Restored previous incremental files status in backend with range {} in operator {}: backend uuid {}, last checkpoint id {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                backendUID,
                lastCompletedCheckpointId);
    }

    /**
     * Restores the base DB from local state of a single state handle.
     *
     * @param localKeyedStateHandle the state handle tor estore from.
     * @throws Exception on any restore error.
     */
    private void restoreBaseDBFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle)
            throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(localKeyedStateHandle.getMetaDataStateHandle());
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

        this.rocksHandle.openDB(
                createColumnFamilyDescriptors(stateMetaInfoSnapshots, true),
                stateMetaInfoSnapshots,
                restoreSourcePath,
                cancelStreamRegistryForRestore);
    }

    /**
     * Helper method to download files, as specified in the given download specs, to the local
     * directory.
     *
     * @param downloadSpecs specifications of files to download.
     * @throws Exception On any download error.
     */
    private void transferRemoteStateToLocalDirectory(
            Collection<StateHandleDownloadSpec> downloadSpecs) throws Exception {
        logger.info(
                "Start downloading remote state to local directory in operator {} for target key-group range {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval());
        try (RocksDBStateDownloader rocksDBStateDownloader =
                new RocksDBStateDownloader(
                        RocksDBStateDataTransferHelper.forThreadNumIfSpecified(
                                numberOfTransferringThreads, ioExecutor))) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    downloadSpecs, cancelStreamRegistryForRestore);
            logger.info(
                    "Finished downloading remote state to local directory in operator {} for target key-group range {}.",
                    operatorIdentifier,
                    keyGroupRange.prettyPrintInterval());
        }
    }

    /**
     * Helper method to copy all data from the given local state handles to the base DB by using
     * temporary DB instances.
     *
     * @param toImport the state handles to import.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any copy error.
     */
    private void copyToBaseDBUsingTempDBs(
            List<IncrementalLocalKeyedStateHandle> toImport,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        if (toImport.isEmpty()) {
            return;
        }

        logger.info(
                "Starting to copy state handles for backend with range {} in operator {} using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(this.rocksHandle.getDb(), writeBatchSize);
                Closeable ignored =
                        cancelStreamRegistryForRestore.registerCloseableTemporarily(
                                writeBatchWrapper.getCancelCloseable())) {
            for (IncrementalLocalKeyedStateHandle handleToCopy : toImport) {
                try (RestoredDBInstance restoredDBInstance =
                        restoreTempDBInstanceFromLocalState(handleToCopy)) {
                    copyTempDbIntoBaseDb(
                            restoredDBInstance,
                            writeBatchWrapper,
                            startKeyGroupPrefixBytes,
                            stopKeyGroupPrefixBytes);
                }
            }
        }

        logger.info(
                "Competed copying state handles for backend with range {} in operator {} using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);
    }

    /**
     * Helper method tp copy all data from an open temporary DB to the base DB.
     *
     * @param tmpRestoreDBInfo the temporary instance.
     * @param writeBatchWrapper write batch wrapper for writes against the base DB.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any copy error.
     */
    private void copyTempDbIntoBaseDb(
            RestoredDBInstance tmpRestoreDBInfo,
            RocksDBWriteBatchWrapper writeBatchWrapper,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        logger.debug(
                "Starting copy of state handle {} for backend with range {} in operator {} to base DB using temporary instance.",
                tmpRestoreDBInfo.srcStateHandle,
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors =
                tmpRestoreDBInfo.columnFamilyDescriptors;
        List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

        // iterating only the requested descriptors automatically skips the default
        // column
        // family handle
        for (int descIdx = 0; descIdx < tmpColumnFamilyDescriptors.size(); ++descIdx) {
            ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(descIdx);

            ColumnFamilyHandle targetColumnFamilyHandle =
                    this.rocksHandle.getOrRegisterStateColumnFamilyHandle(
                                    null,
                                    tmpRestoreDBInfo.stateMetaInfoSnapshots.get(descIdx),
                                    cancelStreamRegistryForRestore)
                            .columnFamilyHandle;

            try (RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIterator(
                            tmpRestoreDBInfo.db,
                            tmpColumnFamilyHandle,
                            tmpRestoreDBInfo.readOptions)) {

                iterator.seek(startKeyGroupPrefixBytes);

                while (iterator.isValid()) {

                    if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(
                            iterator.key(), stopKeyGroupPrefixBytes)) {
                        writeBatchWrapper.put(
                                targetColumnFamilyHandle, iterator.key(), iterator.value());
                    } else {
                        // Since the iterator will visit the record according to the
                        // sorted
                        // order,
                        // we can just break here.
                        break;
                    }

                    iterator.next();
                }
            } // releases native iterator resources
        }
        logger.debug(
                "Finished copy of state handle {} for backend with range {} in operator {} using temporary instance.",
                tmpRestoreDBInfo.srcStateHandle,
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);
    }

    private void cleanUpPathQuietly(@Nonnull Path path) {
        try {
            FileUtils.deleteDirectory(path.toFile());
        } catch (IOException ex) {
            logger.warn("Failed to clean up path " + path, ex);
        }
    }

    /** Entity to hold the temporary RocksDB instance created for restore. */
    private static class RestoredDBInstance implements AutoCloseable {

        @Nonnull private final RocksDB db;

        @Nonnull private final ColumnFamilyHandle defaultColumnFamilyHandle;

        @Nonnull private final List<ColumnFamilyHandle> columnFamilyHandles;

        @Nonnull private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        private final ReadOptions readOptions;

        private final IncrementalLocalKeyedStateHandle srcStateHandle;

        private RestoredDBInstance(
                @Nonnull RocksDB db,
                @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
                @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                @Nonnull IncrementalLocalKeyedStateHandle srcStateHandle) {
            this.db = db;
            this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
            this.columnFamilyHandles = columnFamilyHandles;
            this.columnFamilyDescriptors = columnFamilyDescriptors;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.readOptions = new ReadOptions();
            this.srcStateHandle = srcStateHandle;
        }

        @Override
        public void close() {
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(columnFamilyDescriptors.size() + 1);
            columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeAllQuietly(columnFamilyHandles);
            IOUtils.closeQuietly(db);
            IOUtils.closeAllQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(readOptions);
        }
    }

    private RestoredDBInstance restoreTempDBInstanceFromLocalState(
            IncrementalLocalKeyedStateHandle stateHandle) throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(stateHandle.getMetaDataStateHandle());
        // read meta data
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                createColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

        List<ColumnFamilyHandle> columnFamilyHandles =
                new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

        RocksDB restoreDb =
                RocksDBOperationUtils.openDB(
                        stateHandle.getDirectoryStateHandle().getDirectory().toString(),
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        RocksDBOperationUtils.createColumnFamilyOptions(
                                this.rocksHandle.getColumnFamilyOptionsFactory(), "default"),
                        this.rocksHandle.getDbOptions());

        return new RestoredDBInstance(
                restoreDb,
                columnFamilyHandles,
                columnFamilyDescriptors,
                stateMetaInfoSnapshots,
                stateHandle);
    }

    /**
     * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state
     * metadata snapshot.
     */
    private List<ColumnFamilyDescriptor> createColumnFamilyDescriptors(
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots, boolean registerTtlCompactFilter) {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(stateMetaInfoSnapshots.size());

        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            RegisteredStateMetaInfoBase metaInfoBase =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

            ColumnFamilyDescriptor columnFamilyDescriptor =
                    RocksDBOperationUtils.createColumnFamilyDescriptor(
                            metaInfoBase,
                            this.rocksHandle.getColumnFamilyOptionsFactory(),
                            registerTtlCompactFilter
                                    ? this.rocksHandle.getTtlCompactFiltersManager()
                                    : null,
                            this.rocksHandle.getWriteBufferManagerCapacity());

            columnFamilyDescriptors.add(columnFamilyDescriptor);
        }
        return columnFamilyDescriptors;
    }

    private void runAndReportDuration(RunnableWithException runnable, String metricName)
            throws Exception {
        final SystemClock clock = SystemClock.getInstance();
        final long startTime = clock.relativeTimeMillis();
        runnable.run();
        customInitializationMetrics.addMetric(metricName, clock.relativeTimeMillis() - startTime);
    }

    /** Reads Flink's state meta data file from the state handle. */
    private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle)
            throws Exception {

        InputStream inputStream = null;

        try {
            inputStream = metaStateHandle.openInputStream();
            cancelStreamRegistryForRestore.registerCloseable(inputStream);
            DataInputView in = new DataInputViewStreamWrapper(inputStream);
            return readMetaData(in);
        } finally {
            if (cancelStreamRegistryForRestore.unregisterCloseable(inputStream)) {
                inputStream.close();
            }
        }
    }

    KeyedBackendSerializationProxy<K> readMetaData(DataInputView dataInputView)
            throws IOException, StateMigrationException {
        // isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
        // deserialization of state happens lazily during runtime; we depend on the fact
        // that the new serializer for states could be compatible, and therefore the restore can
        // continue
        // without old serializers required to be present.
        KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(userCodeClassLoader);
        serializationProxy.read(dataInputView);
        if (!isKeySerializerCompatibilityChecked) {
            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<K> currentSerializer = keySerializerProvider.currentSchemaSerializer();
            // check for key serializer compatibility; this also reconfigures the
            // key serializer to be compatible, if it is required and is possible
            TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
                    keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                            serializationProxy.getKeySerializerSnapshot());
            if (keySerializerSchemaCompat.isCompatibleAfterMigration()
                    || keySerializerSchemaCompat.isIncompatible()) {
                throw new StateMigrationException(
                        "The new key serializer ("
                                + currentSerializer
                                + ") must be compatible with the previous key serializer ("
                                + keySerializerProvider.previousSchemaSerializer()
                                + ").");
            }

            isKeySerializerCompatibilityChecked = true;
        }

        return serializationProxy;
    }

    @Override
    public void close() throws Exception {
        this.rocksHandle.close();
    }
}
