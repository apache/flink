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

package org.apache.flink.state.forst.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateBackend.CustomInitializationMetrics;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStDBWriteBatchWrapper;
import org.apache.flink.state.forst.ForStIncrementalCheckpointUtils;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.state.forst.datatransfer.ForStStateDataTransfer;
import org.apache.flink.state.forst.sync.ForStIteratorWrapper;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.RunnableWithException;

import org.forstdb.Checkpoint;
import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.ExportImportFilesMetaData;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.MetricNames.DOWNLOAD_STATE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.RESTORE_STATE_DURATION;
import static org.apache.flink.state.forst.ForStIncrementalCheckpointUtils.beforeThePrefixBytes;
import static org.apache.flink.state.forst.ForStIncrementalCheckpointUtils.checkSstDataAgainstKeyGroupRange;
import static org.apache.flink.state.forst.ForStIncrementalCheckpointUtils.clipDBWithKeyGroupRange;
import static org.apache.flink.state.forst.ForStIncrementalCheckpointUtils.findTheBestStateHandleForInitial;
import static org.apache.flink.state.forst.ForStOperationUtils.createColumnFamilyOptions;
import static org.apache.flink.state.forst.ForStResourceContainer.DB_DIR_STRING;

/** Encapsulates the process of restoring a ForSt instance from an incremental snapshot. */
public class ForStIncrementalRestoreOperation<K> implements ForStRestoreOperation {

    private static final Logger logger =
            LoggerFactory.getLogger(ForStIncrementalRestoreOperation.class);

    private final String operatorIdentifier;
    private final SortedMap<Long, Collection<HandleAndLocalPath>> restoredSstFiles;
    private final ForStHandle forstHandle;
    private final List<IncrementalRemoteKeyedStateHandle> restoreStateHandles;
    private final CloseableRegistry cancelStreamRegistry;
    private final KeyGroupRange keyGroupRange;
    private final int keyGroupPrefixBytes;
    private final ForStResourceContainer optionsContainer;
    private final Path forstBasePath;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final ClassLoader userCodeClassLoader;
    private final CustomInitializationMetrics customInitializationMetrics;
    private long lastCompletedCheckpointId;

    private final long writeBatchSize;

    private final double overlapFractionThreshold;

    private final boolean useIngestDbRestoreMode;

    private final boolean useDeleteFilesInRange;

    private UUID backendUID;

    private boolean isKeySerializerCompatibilityChecked;

    private final RecoveryClaimMode recoveryClaimMode;

    private final Function<StateMetaInfoSnapshot, RegisteredStateMetaInfoBase> stateMetaInfoFactory;

    public ForStIncrementalRestoreOperation(
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            ForStResourceContainer optionsContainer,
            Path forstBasePath,
            Path instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull ForStDBTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity,
            CustomInitializationMetrics customInitializationMetrics,
            @Nonnull Collection<IncrementalRemoteKeyedStateHandle> restoreStateHandles,
            double overlapFractionThreshold,
            boolean useIngestDbRestoreMode,
            boolean useDeleteFilesInRange,
            RecoveryClaimMode recoveryClaimMode,
            Function<StateMetaInfoSnapshot, RegisteredStateMetaInfoBase> stateMetaInfoFactory) {

        this.forstHandle =
                new ForStHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity,
                        stateMetaInfoFactory);
        this.operatorIdentifier = operatorIdentifier;
        this.restoredSstFiles = new TreeMap<>();
        this.lastCompletedCheckpointId = -1L;
        this.backendUID = UUID.randomUUID();
        this.customInitializationMetrics = customInitializationMetrics;
        this.restoreStateHandles = restoreStateHandles.stream().collect(Collectors.toList());
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.optionsContainer = optionsContainer;
        this.forstBasePath = forstBasePath;
        this.keySerializerProvider = keySerializerProvider;
        this.userCodeClassLoader = userCodeClassLoader;
        this.writeBatchSize = writeBatchSize;
        this.overlapFractionThreshold = overlapFractionThreshold;
        this.useIngestDbRestoreMode = useIngestDbRestoreMode;
        this.useDeleteFilesInRange = useDeleteFilesInRange;
        this.recoveryClaimMode = recoveryClaimMode;
        this.stateMetaInfoFactory = stateMetaInfoFactory;
    }

    /**
     * Root method that branches for different implementations of {@link
     * IncrementalKeyedStateHandle}.
     */
    @Override
    public ForStRestoreResult restore() throws Exception {

        if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
            return null;
        }

        logger.info(
                "Starting RocksDB incremental recovery in operator {} "
                        + "target key-group range {}. Use IngestDB={}, State Handles={}",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval(),
                useIngestDbRestoreMode,
                restoreStateHandles);

        List<StateHandleTransferSpec> toTransferSpecs = new ArrayList<>();
        int bestStateHandleForInit = -1;
        if (!useIngestDbRestoreMode || restoreStateHandles.size() == 1) {
            bestStateHandleForInit =
                    findTheBestStateHandleForInitial(
                            restoreStateHandles, keyGroupRange, overlapFractionThreshold);
            toTransferSpecs.add(
                    new StateHandleTransferSpec(
                            restoreStateHandles.get(bestStateHandleForInit),
                            new Path(forstBasePath, DB_DIR_STRING)));
        }
        for (int i = 0; i < restoreStateHandles.size(); i++) {
            if (i != bestStateHandleForInit) {
                IncrementalRemoteKeyedStateHandle handle = restoreStateHandles.get(i);
                toTransferSpecs.add(
                        new StateHandleTransferSpec(
                                handle, new Path(forstBasePath, UUID.randomUUID().toString())));
            }
        }

        try {
            runAndReportDuration(
                    () -> transferAllStateHandles(toTransferSpecs),
                    // TODO: use new metric name, such as "TransferStateDurationMs"
                    DOWNLOAD_STATE_DURATION);

            runAndReportDuration(() -> innerRestore(toTransferSpecs), RESTORE_STATE_DURATION);

            return new ForStRestoreResult(
                    this.forstHandle.getDb(),
                    this.forstHandle.getDefaultColumnFamilyHandle(),
                    this.forstHandle.getNativeMetricMonitor(),
                    lastCompletedCheckpointId,
                    backendUID,
                    restoredSstFiles);
        } finally {
            if (!useIngestDbRestoreMode || restoreStateHandles.size() == 1) {
                toTransferSpecs.remove(0);
            }
            // Delete the transfer destination quietly.
            toTransferSpecs.stream()
                    .map(StateHandleTransferSpec::getTransferDestination)
                    .forEach(
                            dir -> {
                                try {
                                    FileSystem fs = getFileSystem(dir);
                                    fs.delete(dir, true);
                                } catch (IOException ignored) {
                                    logger.warn("Failed to delete transfer destination {}", dir);
                                }
                            });
        }
    }

    private void transferAllStateHandles(List<StateHandleTransferSpec> specs) throws Exception {
        try (ForStStateDataTransfer transfer =
                new ForStStateDataTransfer(
                        ForStStateDataTransfer.DEFAULT_THREAD_NUM,
                        optionsContainer.getFileSystem())) {
            transfer.transferAllStateDataToDirectory(
                    specs, cancelStreamRegistry, recoveryClaimMode);
        }
    }

    private void innerRestore(List<StateHandleTransferSpec> stateHandles) throws Exception {
        if (stateHandles.size() == 1) {
            // This happens if we don't rescale and for some scale out scenarios.
            initBaseDBFromSingleStateHandle(stateHandles.get(0));
        } else {
            // This happens for all scale ins and some scale outs.
            restoreFromMultipleStateHandles(stateHandles);
        }
    }

    /**
     * Initializes the base DB that we restore from a single local state handle.
     *
     * @param stateHandleSpec the state handle to restore the base DB from.
     * @throws Exception on any error during restore.
     */
    private void initBaseDBFromSingleStateHandle(StateHandleTransferSpec stateHandleSpec)
            throws Exception {
        IncrementalRemoteKeyedStateHandle stateHandle = stateHandleSpec.getStateHandle();
        logger.info(
                "Starting opening base ForSt instance in operator {} with target key-group range {} from state handle {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval(),
                stateHandleSpec);

        // Restore base DB from selected initial handle
        restoreBaseDBFromMainHandle(stateHandleSpec);

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
                clipDBWithKeyGroupRange(
                        this.forstHandle.getDb(),
                        this.forstHandle.getColumnFamilyHandles(),
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
                "Finished opening base ForSt instance in operator {} with target key-group range {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval());
    }

    /**
     * Initializes the base DB that we restore from a list of multiple local state handles.
     *
     * @param stateHandles the list of state handles to restore the base DB from.
     * @throws Exception on any error during restore.
     */
    private void restoreFromMultipleStateHandles(List<StateHandleTransferSpec> stateHandles)
            throws Exception {

        logger.info(
                "Starting to restore backend with range {} in operator {} from multiple state handles {} with useIngestDbRestoreMode = {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                stateHandles,
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
                    stateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
        } else {
            // Optimized path for single handle and legacy path for merging multiple handles.
            StateHandleTransferSpec baseSpec = stateHandles.remove(0);
            mergeStateHandlesWithCopyFromTemporaryInstance(
                    baseSpec, stateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);
        }
        logger.info(
                "Completed restoring backend with range {} in operator {} from multiple state handles with useIngestDbRestoreMode = {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                useIngestDbRestoreMode);
    }

    private void restoreBaseDBFromMainHandle(StateHandleTransferSpec handleSpec) throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(handleSpec.getStateHandle().getMetaDataStateHandle());
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();
        this.forstHandle.openDB(
                createColumnFamilyDescriptors(stateMetaInfoSnapshots, true),
                stateMetaInfoSnapshots,
                cancelStreamRegistry);
    }

    /**
     * Restores the checkpointing status and state for this backend. This can only be done if the
     * backend was not rescaled and is therefore identical to the source backend in the previous
     * run.
     *
     * @param incrementalHandle the single state handle from which the backend is restored.
     */
    private void restorePreviousIncrementalFilesStatus(
            IncrementalKeyedStateHandle incrementalHandle) {
        backendUID = incrementalHandle.getBackendIdentifier();
        restoredSstFiles.put(
                incrementalHandle.getCheckpointId(), incrementalHandle.getSharedStateHandles());
        lastCompletedCheckpointId = incrementalHandle.getCheckpointId();
        logger.info(
                "Restored previous incremental files status in backend with range {} in operator {}: backend uuid {}, last checkpoint id {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier,
                backendUID,
                lastCompletedCheckpointId);
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
                    stateMetaInfoFactory.apply(stateMetaInfoSnapshot);

            ColumnFamilyDescriptor columnFamilyDescriptor =
                    ForStOperationUtils.createColumnFamilyDescriptor(
                            metaInfoBase,
                            this.forstHandle.getColumnFamilyOptionsFactory(),
                            registerTtlCompactFilter
                                    ? this.forstHandle.getTtlCompactFiltersManager()
                                    : null,
                            this.forstHandle.getWriteBufferManagerCapacity());

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
            cancelStreamRegistry.registerCloseable(inputStream);
            DataInputView in = new DataInputViewStreamWrapper(inputStream);
            return readMetaData(in);
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
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
        this.forstHandle.close();
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
            ForStDBWriteBatchWrapper writeBatchWrapper,
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

        // iterating only the requested descriptors automatically skips the default column family
        // handle
        for (int descIdx = 0; descIdx < tmpColumnFamilyDescriptors.size(); ++descIdx) {
            ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(descIdx);

            ColumnFamilyHandle targetColumnFamilyHandle =
                    this.forstHandle.getOrRegisterStateColumnFamilyHandle(
                                    null,
                                    tmpRestoreDBInfo.stateMetaInfoSnapshots.get(descIdx),
                                    cancelStreamRegistry)
                            .columnFamilyHandle;

            try (ForStIteratorWrapper iterator =
                    ForStOperationUtils.getForStIterator(
                            tmpRestoreDBInfo.db,
                            tmpColumnFamilyHandle,
                            tmpRestoreDBInfo.readOptions)) {

                iterator.seek(startKeyGroupPrefixBytes);

                while (iterator.isValid()) {

                    if (beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
                        writeBatchWrapper.put(
                                targetColumnFamilyHandle, iterator.key(), iterator.value());
                    } else {
                        // Since the iterator will visit the record according to the sorted order,
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
            getFileSystem(forstBasePath).delete(path, true);
        } catch (IOException ex) {
            logger.warn("Failed to clean up path " + path, ex);
        }
    }

    /**
     * Helper method to copy all data from the given local state handles to the base DB by using
     * temporary DB instances.
     *
     * @param toImportSpecs the state handles to import.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any copy error.
     */
    private void copyToBaseDBUsingTempDBs(
            List<StateHandleTransferSpec> toImportSpecs,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {
        if (toImportSpecs.isEmpty()) {
            return;
        }

        logger.info(
                "Starting to copy state handles for backend with range {} in operator {} using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        try (ForStDBWriteBatchWrapper writeBatchWrapper =
                        new ForStDBWriteBatchWrapper(this.forstHandle.getDb(), writeBatchSize);
                Closeable ignored =
                        cancelStreamRegistry.registerCloseableTemporarily(
                                writeBatchWrapper.getCancelCloseable())) {
            for (StateHandleTransferSpec handleToCopy : toImportSpecs) {
                try (RestoredDBInstance restoredDBInstance = restoreTempDBInstance(handleToCopy)) {
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
     * Helper method that merges the data from multiple state handles into the restoring base DB by
     * the help of copying through temporary RocksDB instances.
     *
     * @param keyedStateHandles the state handles to merge into the base DB.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any merge error.
     */
    private void mergeStateHandlesWithCopyFromTemporaryInstance(
            StateHandleTransferSpec baseSpec,
            List<StateHandleTransferSpec> keyedStateHandles,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        logger.info(
                "Starting to merge state for backend with range {} in operator {} from multiple state handles using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        // Init the base DB instance with the initial state
        initBaseDBFromSingleStateHandle(baseSpec);

        // Copy remaining handles using temporary RocksDB instances
        copyToBaseDBUsingTempDBs(
                keyedStateHandles, startKeyGroupPrefixBytes, stopKeyGroupPrefixBytes);

        logger.info(
                "Completed merging state for backend with range {} in operator {} from multiple state handles using temporary instances.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);
    }

    // ---------------------------------------------------------------------------
    //  ingestDB related
    // ---------------------------------------------------------------------------

    /**
     * Restores the base DB by merging multiple state handles into one. This method first checks if
     * all data to import is in the expected key-groups range and then uses import/export.
     * Otherwise, this method falls back to copying the data using a temporary DB.
     *
     * @param keyedStateHandles the list of state handles to restore the base DB from.
     * @param startKeyGroupPrefixBytes the min/start key of the key groups range as bytes.
     * @param stopKeyGroupPrefixBytes the max+1/end key of the key groups range as bytes.
     * @throws Exception on any restore error.
     */
    public void mergeStateHandlesWithClipAndIngest(
            List<StateHandleTransferSpec> keyedStateHandles,
            byte[] startKeyGroupPrefixBytes,
            byte[] stopKeyGroupPrefixBytes)
            throws Exception {

        Path exportCfBasePath = new Path(forstBasePath, "export-cfs");
        getFileSystem(forstBasePath).mkdirs(exportCfBasePath);

        final Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                exportedColumnFamilyMetaData = new HashMap<>(keyedStateHandles.size());

        final List<StateHandleTransferSpec> notImportableHandles =
                new ArrayList<>(keyedStateHandles.size());
        try {
            KeyGroupRange exportedSstKeyGroupsRange =
                    exportColumnFamiliesWithSstDataInKeyGroupsRange(
                            exportCfBasePath,
                            keyedStateHandles,
                            exportedColumnFamilyMetaData,
                            notImportableHandles);

            if (exportedColumnFamilyMetaData.isEmpty()) {
                // Nothing coule be exported, so we fall back to
                // #mergeStateHandlesWithCopyFromTemporaryInstance
                int bestStateHandleForInit =
                        findTheBestStateHandleForInitial(
                                restoreStateHandles, keyGroupRange, overlapFractionThreshold);
                notImportableHandles.remove(bestStateHandleForInit);
                StateHandleTransferSpec baseSpec =
                        new StateHandleTransferSpec(
                                restoreStateHandles.get(bestStateHandleForInit),
                                new Path(forstBasePath, DB_DIR_STRING));
                transferAllStateHandles(Collections.singletonList(baseSpec));
                mergeStateHandlesWithCopyFromTemporaryInstance(
                        baseSpec,
                        notImportableHandles,
                        startKeyGroupPrefixBytes,
                        stopKeyGroupPrefixBytes);
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
     * Exports the data of the given column families in the given DB.
     *
     * @param db the DB to export from.
     * @param columnFamilyHandles the column families to export.
     * @param registeredStateMetaInfoBases meta information about the registered states in the DB.
     * @param exportBasePath the path to which the export files go.
     * @param resultOutput output parameter for the metadata of the export.
     * @throws RocksDBException on problems inside RocksDB.
     */
    public void exportColumnFamilies(
            RocksDB db,
            List<ColumnFamilyHandle> columnFamilyHandles,
            List<RegisteredStateMetaInfoBase> registeredStateMetaInfoBases,
            Path exportBasePath,
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>> resultOutput)
            throws RocksDBException, IOException {

        Preconditions.checkArgument(
                columnFamilyHandles.size() == registeredStateMetaInfoBases.size(),
                "Lists are aligned by index and must be of the same size!");

        try (final Checkpoint checkpoint = Checkpoint.create(db)) {
            for (int i = 0; i < columnFamilyHandles.size(); i++) {
                RegisteredStateMetaInfoBase.Key stateMetaInfoAsKey =
                        registeredStateMetaInfoBases.get(i).asMapKey();

                String uuid = UUID.randomUUID().toString();

                String subPathStr =
                        optionsContainer.getRemoteBasePath() != null
                                ? exportBasePath.getName() + "/" + uuid
                                : exportBasePath.toString() + "/" + uuid;
                ExportImportFilesMetaData exportedColumnFamilyMetaData =
                        checkpoint.exportColumnFamily(columnFamilyHandles.get(i), subPathStr);

                FileStatus[] exportedSstFiles =
                        getFileSystem(exportBasePath).listStatus(new Path(exportBasePath, uuid));

                if (exportedSstFiles != null) {
                    int sstFileCount = 0;
                    for (FileStatus exportedSstFile : exportedSstFiles) {
                        if (exportedSstFile.getPath().getName().endsWith(".sst")) {
                            sstFileCount++;
                        }
                    }
                    if (sstFileCount > 0) {
                        resultOutput
                                .computeIfAbsent(stateMetaInfoAsKey, (key) -> new ArrayList<>())
                                .add(exportedColumnFamilyMetaData);
                    }
                } else {
                    // Close unused empty export result
                    IOUtils.closeQuietly(exportedColumnFamilyMetaData);
                }
            }
        }
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
        forstHandle.openDB();
        for (Map.Entry<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>> entry :
                exportedColumnFamilyMetaData.entrySet()) {
            forstHandle.registerStateColumnFamilyHandleWithImport(
                    entry.getKey(), entry.getValue(), cancelStreamRegistry);
        }

        // Use Range delete to clip the temp db to the target range of the backend
        clipDBWithKeyGroupRange(
                forstHandle.getDb(),
                forstHandle.getColumnFamilyHandles(),
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
     * Prepares the data for importing by exporting from temporary RocksDB instances. We can only
     * import data that does not exceed the target key-groups range and skip state handles that
     * exceed their range.
     *
     * @param exportCfBasePath the base path for the export files.
     * @param stateHandleSpecs the state handles to prepare for import.
     * @param exportedColumnFamiliesOut output parameter for the metadata of completed exports.
     * @param skipped output parameter for state handles that could not be exported because the data
     *     exceeded the proclaimed range.
     * @return the total key-groups range of the exported data.
     * @throws Exception on any export error.
     */
    private KeyGroupRange exportColumnFamiliesWithSstDataInKeyGroupsRange(
            Path exportCfBasePath,
            List<StateHandleTransferSpec> stateHandleSpecs,
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                    exportedColumnFamiliesOut,
            List<StateHandleTransferSpec> skipped)
            throws Exception {

        logger.info(
                "Starting restore export for backend with range {} in operator {}.",
                keyGroupRange.prettyPrintInterval(),
                operatorIdentifier);

        int minExportKeyGroup = Integer.MAX_VALUE;
        int maxExportKeyGroup = Integer.MIN_VALUE;
        int index = 0;
        for (StateHandleTransferSpec stateHandleSpec : stateHandleSpecs) {
            IncrementalRemoteKeyedStateHandle stateHandle = stateHandleSpec.getStateHandle();

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
            try (ForStIncrementalRestoreOperation.RestoredDBInstance tmpRestoreDBInfo =
                    restoreTempDBInstance(stateHandleSpec)) {

                List<ColumnFamilyHandle> tmpColumnFamilyHandles =
                        tmpRestoreDBInfo.columnFamilyHandles;

                logger.debug("Checking actual keys of sst files" + logLineSuffix);

                // Check if the data in all SST files referenced in the handle is within the
                // proclaimed key-groups range of the handle.
                ForStIncrementalCheckpointUtils.RangeCheckResult rangeCheckResult =
                        checkSstDataAgainstKeyGroupRange(
                                tmpRestoreDBInfo.db,
                                keyGroupPrefixBytes,
                                stateHandle.getKeyGroupRange());

                logger.info("{}" + logLineSuffix, rangeCheckResult);

                if (rangeCheckResult.allInRange()) {

                    logger.debug("Start exporting" + logLineSuffix);

                    List<RegisteredStateMetaInfoBase> registeredStateMetaInfoBases =
                            tmpRestoreDBInfo.stateMetaInfoSnapshots.stream()
                                    .map(stateMetaInfoFactory)
                                    .collect(Collectors.toList());

                    // Export all the Column Families and store the result in
                    // exportedColumnFamiliesOut
                    exportColumnFamilies(
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
                    skipped.add(stateHandleSpec);
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
                stateHandleSpecs.size() - skipped.size(),
                exportedKeyGroupsRange.prettyPrintInterval(),
                skipped.size(),
                skipped);

        return exportedKeyGroupsRange;
    }

    private RestoredDBInstance restoreTempDBInstance(StateHandleTransferSpec stateHandleSpec)
            throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(stateHandleSpec.getStateHandle().getMetaDataStateHandle());
        // read meta data
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                createColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

        List<ColumnFamilyHandle> columnFamilyHandles =
                new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

        String dbName =
                optionsContainer.getRemoteBasePath() != null
                        ? "/" + stateHandleSpec.getTransferDestination().getName()
                        : stateHandleSpec.getTransferDestination().toString();

        // do not relocate log file for temp db
        DBOptions dbOptions = new DBOptions(this.forstHandle.getDbOptions());
        dbOptions.setDbLogDir("");

        RocksDB restoreDb =
                ForStOperationUtils.openDB(
                        dbName,
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        createColumnFamilyOptions(
                                this.forstHandle.getColumnFamilyOptionsFactory(), "default"),
                        dbOptions);

        return new RestoredDBInstance(
                restoreDb,
                columnFamilyHandles,
                columnFamilyDescriptors,
                stateMetaInfoSnapshots,
                stateHandleSpec.getStateHandle(),
                stateHandleSpec.getTransferDestination().toString());
    }

    private FileSystem getFileSystem(Path path) throws IOException {
        if (optionsContainer.getFileSystem() != null) {
            return optionsContainer.getFileSystem();
        } else {
            return path.getFileSystem();
        }
    }

    /** Entity to hold the temporary RocksDB instance created for restore. */
    public static class RestoredDBInstance implements AutoCloseable {

        @Nonnull final RocksDB db;

        @Nonnull final ColumnFamilyHandle defaultColumnFamilyHandle;

        @Nonnull final List<ColumnFamilyHandle> columnFamilyHandles;

        @Nonnull final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

        @Nonnull final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        final ReadOptions readOptions;

        final IncrementalRemoteKeyedStateHandle srcStateHandle;

        final String instancePath;

        RestoredDBInstance(
                @Nonnull RocksDB db,
                @Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
                @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                @Nonnull IncrementalRemoteKeyedStateHandle srcStateHandle,
                @Nonnull String instancePath) {
            this.db = db;
            this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
            this.columnFamilyHandles = columnFamilyHandles;
            this.columnFamilyDescriptors = columnFamilyDescriptors;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.readOptions = new ReadOptions();
            this.srcStateHandle = srcStateHandle;
            this.instancePath = instancePath;
        }

        @Override
        public void close() {
            List<ColumnFamilyOptions> columnFamilyOptions =
                    new ArrayList<>(columnFamilyDescriptors.size() + 1);
            columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
            ForStOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeAllQuietly(columnFamilyHandles);
            IOUtils.closeQuietly(db);
            IOUtils.closeAllQuietly(columnFamilyOptions);
            IOUtils.closeQuietly(readOptions);
        }
    }
}
