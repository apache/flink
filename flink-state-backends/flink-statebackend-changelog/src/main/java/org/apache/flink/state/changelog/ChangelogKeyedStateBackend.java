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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestableKeyedStateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendLocalHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.changelog.restore.ChangelogRestoreTarget;
import org.apache.flink.state.changelog.restore.FunctionDelegationHelper;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationTarget;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform;
import static org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationRunnable;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link KeyedStateBackend} that keeps state on the underlying delegated keyed state backend as
 * well as on the state change log.
 *
 * @param <K> The key by which state is keyed.
 */
@Internal
public class ChangelogKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>,
                CheckpointListener,
                TestableKeyedStateBackend<K>,
                InternalCheckpointListener,
                MaterializationTarget {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogKeyedStateBackend.class);

    /**
     * ChangelogStateBackend only supports CheckpointType.CHECKPOINT; The rest of information in
     * CheckpointOptions is not used in Snapshotable#snapshot(). More details in FLINK-23441.
     */
    private static final CheckpointOptions CHECKPOINT_OPTIONS =
            new CheckpointOptions(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

    /** delegated keyedStateBackend. */
    private final AbstractKeyedStateBackend<K> keyedStateBackend;

    /**
     * This is the cache maintained by the DelegateKeyedStateBackend itself. It is not the same as
     * the underlying delegated keyedStateBackend. InternalKvState is a delegated state.
     */
    private final Map<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    private final ChangelogStateFactory changelogStateFactory;

    private final ExecutionConfig executionConfig;

    private final TtlTimeProvider ttlTimeProvider;

    private final StateChangelogWriter<? extends ChangelogStateHandle> stateChangelogWriter;

    private final Closer closer = Closer.create();

    private final CheckpointStreamFactory streamFactory;

    private ChangelogSnapshotState changelogSnapshotState;

    private long lastCheckpointId = -1L;

    private long materializedId = 0;

    /** last accessed partitioned state. */
    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** For caching the last accessed partitioned state. */
    private String lastName;

    private final FunctionDelegationHelper functionDelegationHelper =
            new FunctionDelegationHelper();

    private final ChangelogStateBackendMetricGroup metrics;

    /**
     * {@link SequenceNumber} denoting last upload range <b>start</b>, inclusive. Updated to {@link
     * ChangelogSnapshotState#materializedTo} when {@link #snapshot(long, long,
     * CheckpointStreamFactory, CheckpointOptions) starting snapshot}. Used to notify {@link
     * #stateChangelogWriter} about changelog ranges that were confirmed or aborted by JM.
     */
    @Nullable private SequenceNumber lastUploadedFrom;
    /**
     * {@link SequenceNumber} denoting last upload range <b>end</b>, exclusive. Updated to {@link
     * StateChangelogWriter#nextSequenceNumber()} when {@link #snapshot(long, long,
     * CheckpointStreamFactory, CheckpointOptions) starting snapshot}. Used to notify {@link
     * #stateChangelogWriter} about changelog ranges that were confirmed or aborted by JM.
     */
    @Nullable private SequenceNumber lastUploadedTo;

    private final String subtaskName;

    /**
     * Provides a unique ID for each state created by this backend instance. A mapping from this ID
     * to state name is written once along with metadata; afterwards, only ID is written with each
     * state change for efficiency.
     */
    private short lastCreatedStateId = -1;

    /** Checkpoint ID mapped to Materialization ID - used to notify nested backend of completion. */
    private final NavigableMap<Long, Long> materializationIdByCheckpointId = new TreeMap<>();

    private long lastConfirmedMaterializationId = -1L;

    /** last failed or cancelled materialization. */
    private long lastFailedMaterializationId = -1L;

    private final ChangelogTruncateHelper changelogTruncateHelper;

    public ChangelogKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            String subtaskName,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            StateChangelogWriter<? extends ChangelogStateHandle> stateChangelogWriter,
            Collection<ChangelogStateBackendHandle> initialState,
            CheckpointStorageWorkerView checkpointStorageWorkerView) {
        this(
                keyedStateBackend,
                subtaskName,
                executionConfig,
                ttlTimeProvider,
                new ChangelogStateBackendMetricGroup(metricGroup),
                stateChangelogWriter,
                initialState,
                checkpointStorageWorkerView,
                new ChangelogStateFactory());
    }

    public ChangelogKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            String subtaskName,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            ChangelogStateBackendMetricGroup metricGroup,
            StateChangelogWriter<? extends ChangelogStateHandle> stateChangelogWriter,
            Collection<ChangelogStateBackendHandle> initialState,
            CheckpointStorageWorkerView checkpointStorageWorkerView,
            ChangelogStateFactory changelogStateFactory) {
        this.keyedStateBackend = keyedStateBackend;
        this.subtaskName = subtaskName;
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.keyValueStatesByName = new HashMap<>();
        this.metrics = metricGroup;
        this.changelogStateFactory = changelogStateFactory;
        this.stateChangelogWriter = stateChangelogWriter;
        this.lastUploadedTo = stateChangelogWriter.initialSequenceNumber();
        this.closer.register(() -> stateChangelogWriter.truncateAndClose(lastUploadedTo));
        this.changelogSnapshotState = completeRestore(initialState);
        this.streamFactory =
                new CheckpointStreamFactory() {

                    @Override
                    public CheckpointStateOutputStream createCheckpointStateOutputStream(
                            CheckpointedStateScope scope) throws IOException {
                        return checkpointStorageWorkerView.createTaskOwnedStateStream();
                    }

                    @Override
                    public boolean canFastDuplicate(
                            StreamStateHandle stateHandle, CheckpointedStateScope scope)
                            throws IOException {
                        return false;
                    }

                    @Override
                    public List<StreamStateHandle> duplicate(
                            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope)
                            throws IOException {
                        return null;
                    }
                };
        this.closer.register(keyedStateBackend);
        this.changelogTruncateHelper = new ChangelogTruncateHelper(stateChangelogWriter);
    }

    // -------------------- CheckpointableKeyedStateBackend --------------------------------
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @Override
    public void setCurrentKey(K newKey) {
        keyedStateBackend.setCurrentKey(newKey);
    }

    @Override
    public K getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keyedStateBackend.getKeySerializer();
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return keyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return keyedStateBackend.getKeysAndNamespaces(state);
    }

    @Override
    public void dispose() {
        keyedStateBackend.dispose();
        lastName = null;
        lastState = null;
        keyValueStatesByName.clear();
        changelogStateFactory.dispose();
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {

        keyedStateBackend.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                function,
                this::getPartitionedState);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        checkNotNull(namespace, "Namespace");

        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }

        final InternalKvState<K, ?, ?> previous =
                keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            functionDelegationHelper.addOrUpdate(stateDescriptor);
            return (S) previous;
        }

        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

        lastName = stateDescriptor.getName();
        lastState = kvState;
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        // The range to upload may overlap with the previous one(s). To reuse them, we could store
        // the previous results either here in the backend or in the writer. However,
        // materialization may truncate only a part of the previous result and the backend would
        // have to split it somehow for the former option, so the latter is used.
        lastCheckpointId = checkpointId;
        lastUploadedFrom = changelogSnapshotState.lastMaterializedTo();
        lastUploadedTo = stateChangelogWriter.nextSequenceNumber();
        changelogTruncateHelper.checkpoint(checkpointId, lastUploadedTo);

        LOG.info(
                "snapshot of {} for checkpoint {}, change range: {}..{}, materialization ID {}",
                subtaskName,
                checkpointId,
                lastUploadedFrom,
                lastUploadedTo,
                changelogSnapshotState.getMaterializationID());

        ChangelogSnapshotState changelogStateBackendStateCopy = changelogSnapshotState;

        materializationIdByCheckpointId.put(
                checkpointId, changelogStateBackendStateCopy.materializationID);

        return toRunnableFuture(
                stateChangelogWriter
                        .persist(lastUploadedFrom, checkpointId)
                        .thenApply(
                                delta ->
                                        buildSnapshotResult(
                                                checkpointId,
                                                delta,
                                                changelogStateBackendStateCopy))
                        .whenComplete(
                                (snapshotResult, throwable) ->
                                        metrics.reportSnapshotResult(snapshotResult))
                        .thenApply(this::castSnapshotResult));
    }

    @SuppressWarnings("unchecked")
    private SnapshotResult<KeyedStateHandle> castSnapshotResult(SnapshotResult<?> snapshotResult) {
        return (SnapshotResult<KeyedStateHandle>) snapshotResult;
    }

    private SnapshotResult<ChangelogStateBackendHandle> buildSnapshotResult(
            long checkpointId,
            SnapshotResult<? extends ChangelogStateHandle> delta,
            ChangelogSnapshotState changelogStateBackendStateCopy) {

        // collections don't change once started and handles are immutable
        List<ChangelogStateHandle> prevDeltaCopy =
                new ArrayList<>(changelogStateBackendStateCopy.getRestoredNonMaterialized());
        long persistedSizeOfThisCheckpoint = 0L;
        if (delta != null
                && delta.getJobManagerOwnedSnapshot() != null
                && delta.getJobManagerOwnedSnapshot().getStateSize() > 0) {
            prevDeltaCopy.add(delta.getJobManagerOwnedSnapshot());
            persistedSizeOfThisCheckpoint +=
                    delta.getJobManagerOwnedSnapshot().getCheckpointedSize();
        }

        if (prevDeltaCopy.isEmpty()
                && changelogStateBackendStateCopy.getMaterializedSnapshot().isEmpty()) {
            return SnapshotResult.empty();
        } else if (!changelogStateBackendStateCopy.getLocalMaterializedSnapshot().isEmpty()
                || delta.getTaskLocalSnapshot() != null) {
            List<ChangelogStateHandle> localDeltaCopy =
                    new ArrayList<>(
                            changelogStateBackendStateCopy.getLocalRestoredNonMaterialized());
            if (delta != null
                    && delta.getTaskLocalSnapshot() != null
                    && delta.getTaskLocalSnapshot().getStateSize() > 0) {
                localDeltaCopy.add(delta.getTaskLocalSnapshot());
            }
            ChangelogStateBackendHandleImpl jmHandle =
                    new ChangelogStateBackendHandleImpl(
                            changelogStateBackendStateCopy.getMaterializedSnapshot(),
                            prevDeltaCopy,
                            getKeyGroupRange(),
                            checkpointId,
                            changelogStateBackendStateCopy.materializationID,
                            persistedSizeOfThisCheckpoint);
            return SnapshotResult.withLocalState(
                    jmHandle,
                    new ChangelogStateBackendLocalHandle(
                            changelogStateBackendStateCopy.getLocalMaterializedSnapshot(),
                            localDeltaCopy,
                            jmHandle));
        } else {
            return SnapshotResult.of(
                    new ChangelogStateBackendHandleImpl(
                            changelogStateBackendStateCopy.getMaterializedSnapshot(),
                            prevDeltaCopy,
                            getKeyGroupRange(),
                            checkpointId,
                            changelogStateBackendStateCopy.materializationID,
                            persistedSizeOfThisCheckpoint));
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        KeyGroupedInternalPriorityQueue<T> internalPriorityQueue =
                keyedStateBackend.create(stateName, byteOrderedElementSerializer);
        ChangelogKeyGroupedPriorityQueue<T> queue =
                (ChangelogKeyGroupedPriorityQueue<T>)
                        changelogStateFactory.getExistingState(
                                stateName, BackendStateType.PRIORITY_QUEUE);
        if (queue == null) {
            queue =
                    changelogStateFactory.create(
                            stateName,
                            internalPriorityQueue,
                            getPqStateChangeLogger(stateName, byteOrderedElementSerializer),
                            byteOrderedElementSerializer);
        } else {
            updateChangelogState(
                    queue, internalPriorityQueue, stateName, byteOrderedElementSerializer);
        }
        return queue;
    }

    private <T> void updateChangelogState(
            ChangelogKeyGroupedPriorityQueue<T> queue,
            KeyGroupedInternalPriorityQueue<T> priorityQueue,
            String stateName,
            TypeSerializer<T> byteOrderedElementSerializer) {
        RegisteredPriorityQueueStateBackendMetaInfo<T> pqMetaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                        stateName, byteOrderedElementSerializer);
        PriorityQueueStateChangeLoggerImpl<K, T> stateChangeLogger =
                (PriorityQueueStateChangeLoggerImpl<K, T>) queue.getStateChangeLogger();
        stateChangeLogger.setMetaInfo(pqMetaInfo);
        queue.setDelegatedState(priorityQueue);
    }

    private <T> StateChangeLogger<T, Void> getPqStateChangeLogger(
            String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        PriorityQueueStateChangeLoggerImpl<K, T> priorityQueueStateChangeLogger =
                new PriorityQueueStateChangeLoggerImpl<>(
                        byteOrderedElementSerializer,
                        keyedStateBackend.getKeyContext(),
                        stateChangelogWriter,
                        new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                stateName, byteOrderedElementSerializer),
                        ++lastCreatedStateId);
        closer.register(priorityQueueStateChangeLogger);
        return priorityQueueStateChangeLogger;
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        return keyedStateBackend.numKeyValueStateEntries();
    }

    @Override
    public boolean isSafeToReuseKVState() {
        return keyedStateBackend.isSafeToReuseKVState();
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        return keyedStateBackend.savepoint();
    }

    // -------------------- CheckpointListener --------------------------------
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (lastCheckpointId == checkpointId) {
            // Notify the writer so that it can re-use the previous uploads. Do NOT notify it about
            // a range status change if it is not relevant anymore. Otherwise, it could CONFIRM a
            // newer upload instead of the previous one. This newer upload could then be re-used
            // while in fact JM has discarded its results.
            // This might change if the log ownership changes (the method won't likely be needed).
            stateChangelogWriter.confirm(lastUploadedFrom, lastUploadedTo, checkpointId);
        }
        Long materializationID = materializationIdByCheckpointId.remove(checkpointId);
        if (materializationID != null) {
            if (materializationID > lastConfirmedMaterializationId) {
                keyedStateBackend.notifyCheckpointComplete(materializationID);
                lastConfirmedMaterializationId = materializationID;
            }
        }
        materializationIdByCheckpointId.headMap(checkpointId, true).clear();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (lastCheckpointId == checkpointId) {
            // Notify the writer so that it can clean up. Do NOT notify it about a range status
            // change if it is not relevant anymore. Otherwise, it could DISCARD a newer upload
            // instead of the previous one. Rely on truncation for the cleanup in this case.
            // This might change if the log ownership changes (the method won't likely be needed).
            stateChangelogWriter.reset(lastUploadedFrom, lastUploadedTo, checkpointId);
        }
        // TODO: Consider notifying nested state backend about checkpoint abortion (FLINK-25850)
    }

    // -------- Methods not simply delegating to wrapped state backend ---------
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                getKeySerializer(),
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
        if (kvState == null) {
            if (!stateDescriptor.isSerializerInitialized()) {
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            kvState =
                    LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                            TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                                    namespaceSerializer, stateDescriptor, this, ttlTimeProvider),
                            stateDescriptor,
                            keyedStateBackend.getLatencyTrackingStateConfig());
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            keyedStateBackend.publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        functionDelegationHelper.addOrUpdate(stateDescriptor);
        return (S) kvState;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        InternalKvState<K, N, SV> state =
                keyedStateBackend.createOrUpdateInternalState(
                        namespaceSerializer, stateDesc, snapshotTransformFactory);
        ChangelogState changelogState =
                changelogStateFactory.getExistingState(
                        stateDesc.getName(), BackendStateType.KEY_VALUE);
        if (changelogState == null) {
            changelogState =
                    changelogStateFactory.create(
                            stateDesc,
                            state,
                            getKvStateChangeLogger(state, stateDesc, snapshotTransformFactory),
                            keyedStateBackend /* pass the nested backend as key context so that it get key updates on recovery*/);
        } else {
            updateChangelogState(changelogState, state, stateDesc, snapshotTransformFactory);
        }

        return (IS) changelogState;
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV, S extends State, IS extends InternalKvState<K, N, SV>, N>
            void updateChangelogState(
                    ChangelogState changelogState,
                    InternalKvState<K, N, SV> state,
                    StateDescriptor<S, SV> stateDesc,
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory) {
        RegisteredKeyValueStateBackendMetaInfo<N, SV> meta =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        stateDesc.getType(),
                        stateDesc.getName(),
                        state.getNamespaceSerializer(),
                        state.getValueSerializer(),
                        (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>)
                                snapshotTransformFactory);

        AbstractChangelogState<K, N, SV, IS> kvChangelogState =
                (AbstractChangelogState<K, N, SV, IS>) changelogState;
        kvChangelogState.setDelegatedState(state);
        KvStateChangeLoggerImpl<K, SV, N> stateChangeLogger =
                (KvStateChangeLoggerImpl<K, SV, N>) kvChangelogState.getStateChangeLogger();
        stateChangeLogger
                .setMetaInfo(meta)
                .setStateTtlConfig(stateDesc.getTtlConfig())
                .setDefaultValue(stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV, S extends State, N> KvStateChangeLogger<SV, N> getKvStateChangeLogger(
            InternalKvState<K, N, SV> state,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        RegisteredKeyValueStateBackendMetaInfo<N, SV> meta =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        stateDesc.getType(),
                        stateDesc.getName(),
                        state.getNamespaceSerializer(),
                        state.getValueSerializer(),
                        (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>)
                                snapshotTransformFactory);

        KvStateChangeLoggerImpl<K, SV, N> kvStateChangeLogger =
                new KvStateChangeLoggerImpl<>(
                        state.getKeySerializer(),
                        state.getNamespaceSerializer(),
                        state.getValueSerializer(),
                        keyedStateBackend.getKeyContext(),
                        stateChangelogWriter,
                        meta,
                        stateDesc.getTtlConfig(),
                        stateDesc.getDefaultValue(),
                        ++lastCreatedStateId);
        closer.register(kvStateChangeLogger);
        return kvStateChangeLogger;
    }

    public void registerCloseable(@Nullable Closeable closeable) {
        closer.register(closeable);
    }

    private ChangelogSnapshotState completeRestore(
            Collection<ChangelogStateBackendHandle> stateHandles) {
        long materializationId = 0L;

        List<KeyedStateHandle> materialized = new ArrayList<>();
        List<ChangelogStateHandle> restoredNonMaterialized = new ArrayList<>();

        List<KeyedStateHandle> localMaterialized = new ArrayList<>();
        List<ChangelogStateHandle> localRestoredNonMaterialized = new ArrayList<>();

        for (ChangelogStateBackendHandle h : stateHandles) {
            if (h != null) {
                if (h instanceof ChangelogStateBackendLocalHandle) {
                    ChangelogStateBackendLocalHandle localHandle =
                            (ChangelogStateBackendLocalHandle) h;
                    materialized.addAll(localHandle.getRemoteMaterializedStateHandles());
                    restoredNonMaterialized.addAll(
                            localHandle.getRemoteNonMaterializedStateHandles());
                    localMaterialized.addAll(localHandle.getMaterializedStateHandles());
                    localRestoredNonMaterialized.addAll(
                            localHandle.getNonMaterializedStateHandles());
                } else {
                    materialized.addAll(h.getMaterializedStateHandles());
                    restoredNonMaterialized.addAll(h.getNonMaterializedStateHandles());
                }
                // choose max materializationID to handle rescaling
                materializationId = Math.max(materializationId, h.getMaterializationID());
            }
        }
        this.lastConfirmedMaterializationId = materializationId;
        this.materializedId = materializationId + 1;

        if (!localMaterialized.isEmpty() || !localRestoredNonMaterialized.isEmpty()) {
            return new ChangelogSnapshotState(
                    materialized,
                    localMaterialized,
                    restoredNonMaterialized,
                    localRestoredNonMaterialized,
                    stateChangelogWriter.initialSequenceNumber(),
                    materializationId);
        }
        return new ChangelogSnapshotState(
                materialized,
                restoredNonMaterialized,
                stateChangelogWriter.initialSequenceNumber(),
                materializationId);
    }

    /**
     * Initialize state materialization so that materialized data can be persisted durably and
     * included into the checkpoint.
     *
     * <p>This method is not thread safe. It should be called either under a lock or through task
     * mailbox executor.
     *
     * @return a tuple of - future snapshot result from the underlying state backend - a {@link
     *     SequenceNumber} identifying the latest change in the changelog
     */
    @Override
    public Optional<MaterializationRunnable> initMaterialization() throws Exception {
        if (lastConfirmedMaterializationId < materializedId - 1
                && lastFailedMaterializationId < materializedId - 1) {
            // SharedStateRegistry potentially requires that the checkpoint's dependency on the
            // shared file be continuous, it will be broken if we trigger a new materialization
            // before the previous one has either confirmed or failed. See discussion in
            // https://github.com/apache/flink/pull/22669#issuecomment-1593370772 .
            LOG.info(
                    "materialization:{} not confirmed or failed or cancelled, skip trigger new one.",
                    materializedId - 1);
            return Optional.empty();
        }

        SequenceNumber upTo = stateChangelogWriter.nextSequenceNumber();
        SequenceNumber lastMaterializedTo = changelogSnapshotState.lastMaterializedTo();

        LOG.info(
                "Initialize Materialization. Current changelog writers last append to sequence number {}",
                upTo);

        if (upTo.compareTo(lastMaterializedTo) > 0) {

            LOG.info("Starting materialization from {} : {}", lastMaterializedTo, upTo);

            // This ID is not needed for materialization; But since we are re-using the
            // streamFactory that is designed for state backend snapshot, which requires unique
            // checkpoint ID. A faked materialized Id is provided here.
            long materializationID = materializedId++;

            MaterializationRunnable materializationRunnable =
                    new MaterializationRunnable(
                            keyedStateBackend.snapshot(
                                    materializationID,
                                    System.currentTimeMillis(),
                                    // TODO: implement its own streamFactory.
                                    streamFactory,
                                    CHECKPOINT_OPTIONS),
                            materializationID,
                            upTo);

            // log metadata after materialization is triggered
            changelogStateFactory.resetAllWritingMetaFlags();

            return Optional.of(materializationRunnable);
        } else {
            LOG.debug(
                    "Skip materialization, last materialized to {} : last log to {}",
                    lastMaterializedTo,
                    upTo);

            return Optional.empty();
        }
    }

    /**
     * This method is not thread safe. It should be called either under a lock or through task
     * mailbox executor.
     */
    @Override
    public void handleMaterializationResult(
            SnapshotResult<KeyedStateHandle> materializedSnapshot,
            long materializationID,
            SequenceNumber upTo) {

        LOG.info(
                "Task {} finishes materialization, updates the snapshotState upTo {} : {}",
                subtaskName,
                upTo,
                materializedSnapshot);
        changelogSnapshotState =
                materializedSnapshot.getTaskLocalSnapshot() == null
                        ? new ChangelogSnapshotState(
                                getMaterializedResult(materializedSnapshot),
                                Collections.emptyList(),
                                upTo,
                                materializationID)
                        : new ChangelogSnapshotState(
                                getMaterializedResult(materializedSnapshot),
                                getLocalMaterializedResult(materializedSnapshot),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                upTo,
                                materializationID);

        changelogTruncateHelper.materialized(upTo);
    }

    @Override
    public void handleMaterializationFailureOrCancellation(
            long materializationID, SequenceNumber upTo, Throwable cause) {

        LOG.info(
                "Task {} failed or cancelled materialization:{} which is upTo:{}",
                subtaskName,
                materializationID,
                upTo);
        lastFailedMaterializationId = Math.max(lastFailedMaterializationId, materializationID);
    }

    // TODO: this method may change after the ownership PR
    private List<KeyedStateHandle> getMaterializedResult(
            @Nonnull SnapshotResult<KeyedStateHandle> materializedSnapshot) {
        KeyedStateHandle jobManagerOwned = materializedSnapshot.getJobManagerOwnedSnapshot();
        return jobManagerOwned == null ? emptyList() : singletonList(jobManagerOwned);
    }

    private List<KeyedStateHandle> getLocalMaterializedResult(
            @Nonnull SnapshotResult<KeyedStateHandle> materializedSnapshot) {
        KeyedStateHandle taskLocalSnapshot = materializedSnapshot.getTaskLocalSnapshot();
        return taskLocalSnapshot == null ? emptyList() : singletonList(taskLocalSnapshot);
    }

    @Override
    public KeyedStateBackend<K> getDelegatedKeyedStateBackend(boolean recursive) {
        return keyedStateBackend.getDelegatedKeyedStateBackend(recursive);
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        changelogTruncateHelper.checkpointSubsumed(checkpointId);
    }

    public ChangelogRestoreTarget<K> getChangelogRestoreTarget() {
        return new ChangelogRestoreTarget<K>() {
            @Override
            public KeyGroupRange getKeyGroupRange() {
                return ChangelogKeyedStateBackend.this.getKeyGroupRange();
            }

            @Override
            @SuppressWarnings("unchecked")
            public <N, S extends State, V> S createKeyedState(
                    TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
                    throws Exception {
                InternalKvState<K, N, V> kvState =
                        keyedStateBackend.createOrUpdateInternalState(
                                namespaceSerializer, stateDescriptor, noTransform(), true);
                ChangelogState changelogState =
                        changelogStateFactory.getExistingState(
                                stateDescriptor.getName(),
                                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
                if (changelogState == null) {
                    changelogState =
                            changelogStateFactory.create(
                                    stateDescriptor,
                                    kvState,
                                    getKvStateChangeLogger(kvState, stateDescriptor, noTransform()),
                                    keyedStateBackend /* pass the nested backend as key context so that it get key updates on recovery*/);
                } else {
                    updateChangelogState(changelogState, kvState, stateDescriptor, noTransform());
                }
                functionDelegationHelper.addOrUpdate(stateDescriptor);
                return (S) changelogState;
            }

            @Nonnull
            @Override
            @SuppressWarnings("unchecked")
            public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
                    KeyGroupedInternalPriorityQueue<T> createPqState(
                            @Nonnull String stateName,
                            @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
                KeyGroupedInternalPriorityQueue<T> internalPriorityQueue =
                        keyedStateBackend.create(stateName, byteOrderedElementSerializer, true);
                ChangelogKeyGroupedPriorityQueue<T> queue =
                        (ChangelogKeyGroupedPriorityQueue<T>)
                                changelogStateFactory.getExistingState(
                                        stateName,
                                        StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);
                if (queue == null) {
                    queue =
                            changelogStateFactory.create(
                                    stateName,
                                    internalPriorityQueue,
                                    getPqStateChangeLogger(stateName, byteOrderedElementSerializer),
                                    byteOrderedElementSerializer);
                } else {
                    updateChangelogState(
                            queue, internalPriorityQueue, stateName, byteOrderedElementSerializer);
                }
                return queue;
            }

            @Override
            public ChangelogState getExistingState(String name, BackendStateType type) {
                return changelogStateFactory.getExistingState(name, type);
            }

            @Override
            public CheckpointableKeyedStateBackend<K> getRestoredKeyedStateBackend() {
                return ChangelogKeyedStateBackend.this;
            }
        };
    }

    private static <T> RunnableFuture<T> toRunnableFuture(CompletableFuture<T> f) {
        return new RunnableFuture<T>() {
            @Override
            public void run() {
                f.join();
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return f.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return f.isCancelled();
            }

            @Override
            public boolean isDone() {
                return f.isDone();
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                return f.get();
            }

            @Override
            public T get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                return f.get(timeout, unit);
            }
        };
    }

    /**
     * Snapshot State for ChangelogKeyedStateBackend, a wrapper over {@link SnapshotResult}.
     *
     * <p>It includes three parts: - materialized snapshot from the underlying delegated state
     * backend - non-materialized part in the current changelog - non-materialized changelog, from
     * previous logs (before failover or rescaling)
     */
    private class ChangelogSnapshotState {

        /** Set initially on restore and later upon materialization. */
        private final SnapshotResult<ChangelogStateBackendHandle> changelogSnapshot;

        /**
         * The {@link SequenceNumber} up to which the state is materialized, exclusive. This
         * indicates the non-materialized part of the current changelog.
         */
        private final SequenceNumber materializedTo;

        /** ID of this materialization corresponding to the nested backend checkpoint ID. */
        private final long materializationID;

        public ChangelogSnapshotState(
                List<KeyedStateHandle> materializedSnapshot,
                List<ChangelogStateHandle> restoredNonMaterialized,
                SequenceNumber materializedTo,
                long materializationID) {
            this.changelogSnapshot =
                    SnapshotResult.of(
                            new ChangelogStateBackendHandleImpl(
                                    materializedSnapshot,
                                    restoredNonMaterialized,
                                    getKeyGroupRange(),
                                    lastCheckpointId,
                                    materializationID,
                                    0L));
            this.materializedTo = materializedTo;
            this.materializationID = materializationID;
        }

        public ChangelogSnapshotState(
                List<KeyedStateHandle> materializedSnapshot,
                List<KeyedStateHandle> localMaterializedSnapshot,
                List<ChangelogStateHandle> restoredNonMaterialized,
                List<ChangelogStateHandle> localRestoredNonMaterialized,
                SequenceNumber materializedTo,
                long materializationID) {
            ChangelogStateBackendHandleImpl jmHandle =
                    new ChangelogStateBackendHandleImpl(
                            materializedSnapshot,
                            restoredNonMaterialized,
                            getKeyGroupRange(),
                            lastCheckpointId,
                            materializationID,
                            0L);
            this.changelogSnapshot =
                    SnapshotResult.withLocalState(
                            jmHandle,
                            new ChangelogStateBackendLocalHandle(
                                    localMaterializedSnapshot,
                                    localRestoredNonMaterialized,
                                    jmHandle));
            this.materializedTo = materializedTo;
            this.materializationID = materializationID;
        }

        public List<KeyedStateHandle> getMaterializedSnapshot() {
            return changelogSnapshot.getJobManagerOwnedSnapshot() != null
                    ? changelogSnapshot.getJobManagerOwnedSnapshot().getMaterializedStateHandles()
                    : Collections.emptyList();
        }

        public List<KeyedStateHandle> getLocalMaterializedSnapshot() {
            return changelogSnapshot.getTaskLocalSnapshot() != null
                    ? changelogSnapshot.getTaskLocalSnapshot().getMaterializedStateHandles()
                    : Collections.emptyList();
        }

        public List<ChangelogStateHandle> getRestoredNonMaterialized() {
            return changelogSnapshot.getJobManagerOwnedSnapshot() != null
                    ? changelogSnapshot
                            .getJobManagerOwnedSnapshot()
                            .getNonMaterializedStateHandles()
                    : Collections.emptyList();
        }

        public List<ChangelogStateHandle> getLocalRestoredNonMaterialized() {
            return changelogSnapshot.getTaskLocalSnapshot() != null
                    ? changelogSnapshot.getTaskLocalSnapshot().getNonMaterializedStateHandles()
                    : Collections.emptyList();
        }

        public SequenceNumber lastMaterializedTo() {
            return materializedTo;
        }

        public long getMaterializationID() {
            return materializationID;
        }
    }

    @VisibleForTesting
    StateChangelogWriter<? extends ChangelogStateHandle> getChangelogWriter() {
        return stateChangelogWriter;
    }
}
