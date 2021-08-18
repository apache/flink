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
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
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
import org.apache.flink.runtime.state.TestableKeyedStateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.changelog.restore.FunctionDelegationHelper;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                TestableKeyedStateBackend<K> {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogKeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateFactory) ChangelogValueState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateFactory) ChangelogListState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateFactory) ChangelogReducingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateFactory) ChangelogAggregatingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateFactory) ChangelogMapState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** delegated keyedStateBackend. */
    private final AbstractKeyedStateBackend<K> keyedStateBackend;

    /**
     * This is the cache maintained by the DelegateKeyedStateBackend itself. It is not the same as
     * the underlying delegated keyedStateBackend. InternalKvState is a delegated state.
     */
    private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    /**
     * Unwrapped changelog states used for recovery (not wrapped into e.g. TTL).
     *
     * <p>WARN: cleared upon recovery completion.
     */
    private final HashMap<String, ChangelogState> changelogStates;

    private final HashMap<String, ChangelogKeyGroupedPriorityQueue<?>> priorityQueueStatesByName;

    private final ExecutionConfig executionConfig;

    private final TtlTimeProvider ttlTimeProvider;

    private final StateChangelogWriter<ChangelogStateHandle> stateChangelogWriter;

    private long lastCheckpointId = -1L;

    /** last accessed partitioned state. */
    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** For caching the last accessed partitioned state. */
    private String lastName;

    private final FunctionDelegationHelper functionDelegationHelper =
            new FunctionDelegationHelper();

    /** Updated initially on restore and later upon materialization (in FLINK-21357). */
    @GuardedBy("materialized")
    private final List<KeyedStateHandle> materialized = new ArrayList<>();

    /** Updated initially on restore and later cleared upon materialization (in FLINK-21357). */
    @GuardedBy("materialized")
    private final List<ChangelogStateHandle> restoredNonMaterialized = new ArrayList<>();

    /**
     * {@link SequenceNumber} denoting last upload range <b>start</b>, inclusive. Updated to {@link
     * #materializedTo} when {@link #snapshot(long, long, CheckpointStreamFactory,
     * CheckpointOptions) starting snapshot}. Used to notify {@link #stateChangelogWriter} about
     * changelog ranges that were confirmed or aborted by JM.
     */
    @Nullable private SequenceNumber lastUploadedFrom;
    /**
     * {@link SequenceNumber} denoting last upload range <b>end</b>, exclusive. Updated to {@link
     * org.apache.flink.runtime.state.changelog.StateChangelogWriter#lastAppendedSequenceNumber}
     * when {@link #snapshot(long, long, CheckpointStreamFactory, CheckpointOptions) starting
     * snapshot}. Used to notify {@link #stateChangelogWriter} about changelog ranges that were
     * confirmed or aborted by JM.
     */
    @Nullable private SequenceNumber lastUploadedTo;
    /**
     * The {@link SequenceNumber} up to which the state is materialized, exclusive. The log should
     * be truncated accordingly.
     *
     * <p>WARN: currently not updated - to be changed in FLINK-21357.
     *
     * <p>WARN: this value needs to be updated for benchmarking, e.g. in notifyCheckpointComplete.
     */
    private final SequenceNumber materializedTo;

    private final MailboxExecutor mainMailboxExecutor;

    private final ExecutorService asyncOperationsThreadPool;

    public ChangelogKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            StateChangelogWriter<ChangelogStateHandle> stateChangelogWriter,
            Collection<ChangelogStateBackendHandle> initialState,
            MailboxExecutor mainMailboxExecutor,
            ExecutorService asyncOperationsThreadPool) {
        this.keyedStateBackend = keyedStateBackend;
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.keyValueStatesByName = new HashMap<>();
        this.priorityQueueStatesByName = new HashMap<>();
        this.stateChangelogWriter = stateChangelogWriter;
        this.materializedTo = stateChangelogWriter.initialSequenceNumber();
        this.changelogStates = new HashMap<>();
        this.mainMailboxExecutor = checkNotNull(mainMailboxExecutor);
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.completeRestore(initialState);
    }

    // -------------------- CheckpointableKeyedStateBackend --------------------------------
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    public void close() throws IOException {
        keyedStateBackend.close();
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
        lastUploadedFrom = materializedTo;
        lastUploadedTo = stateChangelogWriter.lastAppendedSequenceNumber().next();

        LOG.debug(
                "snapshot for checkpoint {}, change range: {}..{}",
                checkpointId,
                lastUploadedFrom,
                lastUploadedTo);
        return toRunnableFuture(
                stateChangelogWriter
                        .persist(lastUploadedFrom)
                        .thenApply(this::buildSnapshotResult));
    }

    private SnapshotResult<KeyedStateHandle> buildSnapshotResult(ChangelogStateHandle delta) {
        // Can be called by either task thread during the sync checkpoint phase (if persist future
        // was already completed); or by the writer thread otherwise. So need to synchronize.
        // todo: revisit after FLINK-21357 - use mailbox action?
        synchronized (materialized) {
            // collections don't change once started and handles are immutable
            List<ChangelogStateHandle> prevDeltaCopy = new ArrayList<>(restoredNonMaterialized);
            if (delta != null && delta.getStateSize() > 0) {
                prevDeltaCopy.add(delta);
            }
            if (prevDeltaCopy.isEmpty() && materialized.isEmpty()) {
                return SnapshotResult.empty();
            } else {
                return SnapshotResult.of(
                        new ChangelogStateBackendHandleImpl(
                                materialized, prevDeltaCopy, getKeyGroupRange()));
            }
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        ChangelogKeyGroupedPriorityQueue<T> queue =
                (ChangelogKeyGroupedPriorityQueue<T>) priorityQueueStatesByName.get(stateName);
        if (queue == null) {
            PriorityQueueStateChangeLoggerImpl<K, T> priorityQueueStateChangeLogger =
                    new PriorityQueueStateChangeLoggerImpl<>(
                            byteOrderedElementSerializer,
                            keyedStateBackend.getKeyContext(),
                            stateChangelogWriter,
                            new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                    stateName, byteOrderedElementSerializer));
            queue =
                    new ChangelogKeyGroupedPriorityQueue<>(
                            keyedStateBackend.create(stateName, byteOrderedElementSerializer),
                            priorityQueueStateChangeLogger,
                            byteOrderedElementSerializer);
            priorityQueueStatesByName.put(stateName, queue);
        }
        return queue;
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        return keyedStateBackend.numKeyValueStateEntries();
    }

    @Override
    public boolean isStateImmutableInStateBackend(CheckpointType checkpointOptions) {
        return keyedStateBackend.isStateImmutableInStateBackend(checkpointOptions);
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
            stateChangelogWriter.confirm(lastUploadedFrom, lastUploadedTo);
        }
        keyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        if (lastCheckpointId == checkpointId) {
            // Notify the writer so that it can clean up. Do NOT notify it about a range status
            // change if it is not relevant anymore. Otherwise, it could DISCARD a newer upload
            // instead of the previous one. Rely on truncation for the cleanup in this case.
            // This might change if the log ownership changes (the method won't likely be needed).
            stateChangelogWriter.reset(lastUploadedFrom, lastUploadedTo);
        }
        keyedStateBackend.notifyCheckpointAborted(checkpointId);
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
        // todo: support state migration (in FLINK-23143)
        //     This method is currently called both on recovery and on user access.
        //     So keyValueStatesByName may contain an entry for user-requested state which will
        //     prevent state migration (in contrast to other backends).
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
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }
        RegisteredKeyValueStateBackendMetaInfo<N, SV> meta =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        stateDesc.getType(),
                        stateDesc.getName(),
                        namespaceSerializer,
                        stateDesc.getSerializer(),
                        (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>)
                                snapshotTransformFactory);

        InternalKvState<K, N, SV> state =
                keyedStateBackend.createInternalState(
                        namespaceSerializer, stateDesc, snapshotTransformFactory);
        KvStateChangeLoggerImpl<K, SV, N> kvStateChangeLogger =
                new KvStateChangeLoggerImpl<>(
                        state.getKeySerializer(),
                        state.getNamespaceSerializer(),
                        state.getValueSerializer(),
                        keyedStateBackend.getKeyContext(),
                        stateChangelogWriter,
                        meta,
                        stateDesc.getTtlConfig(),
                        stateDesc.getDefaultValue());
        IS is =
                stateFactory.create(
                        state,
                        kvStateChangeLogger,
                        keyedStateBackend /* pass the nested backend as key context so that it get key updates on recovery*/);
        changelogStates.put(stateDesc.getName(), (ChangelogState) is);
        return is;
    }

    private void completeRestore(Collection<ChangelogStateBackendHandle> stateHandles) {
        if (!stateHandles.isEmpty()) {
            synchronized (materialized) { // ensure visibility
                for (ChangelogStateBackendHandle h : stateHandles) {
                    if (h != null) {
                        materialized.addAll(h.getMaterializedStateHandles());
                        restoredNonMaterialized.addAll(h.getNonMaterializedStateHandles());
                    }
                }
            }
        }
        changelogStates.clear();
    }

    @Override
    public KeyedStateBackend<K> getDelegatedKeyedStateBackend(boolean recursive) {
        return keyedStateBackend.getDelegatedKeyedStateBackend(recursive);
    }

    // Factory function interface
    private interface StateFactory {
        <K, N, SV, S extends State, IS extends S> IS create(
                InternalKvState<K, N, SV> kvState,
                KvStateChangeLogger<SV, N> changeLogger,
                InternalKeyContext<K> keyContext)
                throws Exception;
    }

    /**
     * @param name state name
     * @param type state type (the only supported type currently are: {@link
     *     BackendStateType#KEY_VALUE key value}, {@link BackendStateType#PRIORITY_QUEUE priority
     *     queue})
     * @return an existing state, i.e. the one that was already created. The returned state will not
     *     apply TTL to the passed values, regardless of the TTL settings. This prevents double
     *     applying of TTL (recovered values are TTL values if TTL was enabled). The state will,
     *     however, use TTL serializer if TTL is enabled. WARN: only valid during the recovery.
     * @throws NoSuchElementException if the state wasn't created
     * @throws UnsupportedOperationException if state type is not supported
     */
    public ChangelogState getExistingStateForRecovery(String name, BackendStateType type)
            throws NoSuchElementException, UnsupportedOperationException {
        ChangelogState state;
        switch (type) {
            case KEY_VALUE:
                state = changelogStates.get(name);
                break;
            case PRIORITY_QUEUE:
                state = priorityQueueStatesByName.get(name);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown state type %s (%s)", type, name));
        }
        if (state == null) {
            throw new NoSuchElementException(String.format("%s state %s not found", type, name));
        }
        return state;
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
}
