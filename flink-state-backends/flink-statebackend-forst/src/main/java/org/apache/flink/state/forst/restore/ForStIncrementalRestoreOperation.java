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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
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
import org.apache.flink.state.forst.ForStKeyedStateBackend.ForStKvStateInfo;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.ForStStateDataTransfer;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.util.StateMigrationException;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.RunnableWithException;

import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import static org.apache.flink.state.forst.ForStResourceContainer.DB_DIR_STRING;

/** Encapsulates the process of restoring a ForSt instance from an incremental snapshot. */
public class ForStIncrementalRestoreOperation<K> implements ForStRestoreOperation {

    private static final Logger logger =
            LoggerFactory.getLogger(ForStIncrementalRestoreOperation.class);

    private final String operatorIdentifier;
    private final SortedMap<Long, Collection<HandleAndLocalPath>> restoredSstFiles;
    private final ForStHandle forstHandle;
    private final Collection<IncrementalRemoteKeyedStateHandle> restoreStateHandles;
    private final CloseableRegistry cancelStreamRegistry;
    private final KeyGroupRange keyGroupRange;
    private final ForStResourceContainer optionsContainer;
    private final Path forstBasePath;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final ClassLoader userCodeClassLoader;
    private final CustomInitializationMetrics customInitializationMetrics;
    private long lastCompletedCheckpointId;
    private UUID backendUID;

    private boolean isKeySerializerCompatibilityChecked;

    public ForStIncrementalRestoreOperation(
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Map<String, ForStKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            ForStResourceContainer optionsContainer,
            Path forstBasePath,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            CustomInitializationMetrics customInitializationMetrics,
            @Nonnull Collection<IncrementalRemoteKeyedStateHandle> restoreStateHandles) {

        this.forstHandle =
                new ForStHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup);
        this.operatorIdentifier = operatorIdentifier;
        this.restoredSstFiles = new TreeMap<>();
        this.lastCompletedCheckpointId = -1L;
        this.backendUID = UUID.randomUUID();
        this.customInitializationMetrics = customInitializationMetrics;
        this.restoreStateHandles = restoreStateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyGroupRange = keyGroupRange;
        this.optionsContainer = optionsContainer;
        this.forstBasePath = forstBasePath;
        this.keySerializerProvider = keySerializerProvider;
        this.userCodeClassLoader = userCodeClassLoader;
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
                "Starting ForSt incremental recovery in operator {}, target key-group range {}. State Handles={}",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval(),
                restoreStateHandles);

        List<IncrementalRemoteKeyedStateHandle> otherHandles =
                new ArrayList<>(restoreStateHandles.size() - 1);
        IncrementalRemoteKeyedStateHandle mainHandle =
                chooseMainHandleAndCollectOthers(otherHandles);

        // Use default db directory name as in main db instance
        StateHandleTransferSpec mainSpec =
                new StateHandleTransferSpec(mainHandle, new Path(forstBasePath, DB_DIR_STRING));

        List<StateHandleTransferSpec> otherSpecs =
                otherHandles.stream()
                        .map(
                                handle ->
                                        new StateHandleTransferSpec(
                                                handle,
                                                new Path(
                                                        forstBasePath,
                                                        UUID.randomUUID().toString())))
                        .collect(Collectors.toList());

        try {
            runAndReportDuration(
                    () -> transferAllStateHandles(mainSpec, otherSpecs),
                    // TODO: use new metric name, such as "TransferStateDurationMs"
                    DOWNLOAD_STATE_DURATION);

            runAndReportDuration(
                    () -> restoreFromTransferredHandles(mainSpec, otherSpecs),
                    RESTORE_STATE_DURATION);

            return new ForStRestoreResult(
                    this.forstHandle.getDb(),
                    this.forstHandle.getDefaultColumnFamilyHandle(),
                    this.forstHandle.getNativeMetricMonitor(),
                    lastCompletedCheckpointId,
                    backendUID,
                    restoredSstFiles);
        } finally {
            // Delete the transfer destination quietly.
            otherSpecs.stream()
                    .map(StateHandleTransferSpec::getTransferDestination)
                    .forEach(
                            dir -> {
                                try {
                                    FileSystem fs = dir.getFileSystem();
                                    fs.delete(dir, true);
                                } catch (IOException ignored) {

                                }
                            });
        }
    }

    private IncrementalRemoteKeyedStateHandle chooseMainHandleAndCollectOthers(
            final List<IncrementalRemoteKeyedStateHandle> otherHandlesCollector) {

        // TODO: When implementing rescale, refer to
        // RocksDBIncrementalCheckpointUtils.findTheBestStateHandleForInitial to implement this
        // method. Currently, it can be assumed that there is only one state handle to restore.
        return restoreStateHandles.iterator().next();
    }

    private void transferAllStateHandles(
            StateHandleTransferSpec mainSpecs, List<StateHandleTransferSpec> otherSpecs)
            throws Exception {

        // TODO: Now not support rescale, so now ignore otherSpecs. Before implement transfer
        // otherSpecs, we may need reconsider the implementation of ForStFlinkFileSystem.

        FileSystem forStFs =
                optionsContainer.getRemoteForStPath() != null
                        ? ForStFlinkFileSystem.get(optionsContainer.getRemoteForStPath().toUri())
                        : FileSystem.getLocalFileSystem();

        try (ForStStateDataTransfer transfer =
                new ForStStateDataTransfer(ForStStateDataTransfer.DEFAULT_THREAD_NUM, forStFs)) {
            transfer.transferAllStateDataToDirectory(
                    Collections.singleton(mainSpecs), cancelStreamRegistry);
        }
    }

    private void restoreFromTransferredHandles(
            StateHandleTransferSpec mainHandle, List<StateHandleTransferSpec> temporaryHandles)
            throws Exception {

        restoreFromMainTransferredHandle(mainHandle);

        mergeOtherTransferredHandles(temporaryHandles);
    }

    private void restoreFromMainTransferredHandle(StateHandleTransferSpec mainHandle)
            throws Exception {
        IncrementalRemoteKeyedStateHandle handle = mainHandle.getStateHandle();

        restoreBaseDBFromMainHandle(handle);

        // Check if the key-groups range has changed.
        if (Objects.equals(handle.getKeyGroupRange(), keyGroupRange)) {
            // This is the case if we didn't rescale, so we can restore all the info from the
            // previous backend instance (backend id and incremental checkpoint history).
            restorePreviousIncrementalFilesStatus(handle);
        } else {
            // If the key-groups don't match, this was a scale out, and we need to clip the
            // key-groups range of the db to the target range for this backend.
            throw new UnsupportedOperationException("ForStateBackend not support rescale yet.");
        }
        logger.info(
                "Finished opening base ForSt instance in operator {} with target key-group range {}.",
                operatorIdentifier,
                keyGroupRange.prettyPrintInterval());
    }

    private void mergeOtherTransferredHandles(List<StateHandleTransferSpec> otherHandles) {
        if (otherHandles != null && !otherHandles.isEmpty()) {
            // TODO: implement rescale
            throw new UnsupportedOperationException("ForStateBackend not support rescale yet.");
        }
    }

    private void restoreBaseDBFromMainHandle(IncrementalRemoteKeyedStateHandle handle)
            throws Exception {
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(handle.getMetaDataStateHandle());
        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        this.forstHandle.openDB(
                createColumnFamilyDescriptors(stateMetaInfoSnapshots, true),
                stateMetaInfoSnapshots);
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
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

            ColumnFamilyDescriptor columnFamilyDescriptor =
                    ForStOperationUtils.createColumnFamilyDescriptor(
                            metaInfoBase.getName(),
                            this.forstHandle.getColumnFamilyOptionsFactory());

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
}
