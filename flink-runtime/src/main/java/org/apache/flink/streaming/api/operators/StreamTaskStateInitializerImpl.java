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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.SubtaskFileMergingManagerRestoreOperation;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Disposable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.SystemClock;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.state.StateBackendLoader.loadStateBackendFromKeyedStateHandles;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/**
 * This class is the main implementation of a {@link StreamTaskStateInitializer}. This class obtains
 * the state to create {@link StreamOperatorStateContext} objects for stream operators from the
 * {@link TaskStateManager} of the task that runs the stream task and hence the operator.
 *
 * <p>This implementation operates on top a {@link TaskStateManager}, from which it receives
 * everything required to restore state in the backends from checkpoints or savepoints.
 */
public class StreamTaskStateInitializerImpl implements StreamTaskStateInitializer {

    /**
     * The environment of the task. This is required as parameter to construct state backends via
     * their factory.
     */
    private final Environment environment;

    /**
     * The state manager of the tasks provides the information used to restore potential previous
     * state.
     */
    private final TaskStateManager taskStateManager;

    /** This object is the factory for everything related to state backends and checkpointing. */
    private final StateBackend stateBackend;

    private final SubTaskInitializationMetricsBuilder initializationMetrics;
    private final TtlTimeProvider ttlTimeProvider;

    private final InternalTimeServiceManager.Provider timeServiceManagerProvider;

    private final StreamTaskCancellationContext cancellationContext;

    public StreamTaskStateInitializerImpl(Environment environment, StateBackend stateBackend) {

        this(
                environment,
                stateBackend,
                new SubTaskInitializationMetricsBuilder(
                        SystemClock.getInstance().absoluteTimeMillis()),
                TtlTimeProvider.DEFAULT,
                InternalTimeServiceManagerImpl::create,
                StreamTaskCancellationContext.alwaysRunning());
    }

    public StreamTaskStateInitializerImpl(
            Environment environment,
            StateBackend stateBackend,
            SubTaskInitializationMetricsBuilder initializationMetrics,
            TtlTimeProvider ttlTimeProvider,
            InternalTimeServiceManager.Provider timeServiceManagerProvider,
            StreamTaskCancellationContext cancellationContext) {

        this.environment = environment;
        this.taskStateManager = Preconditions.checkNotNull(environment.getTaskStateManager());
        this.stateBackend = Preconditions.checkNotNull(stateBackend);
        this.initializationMetrics = initializationMetrics;
        this.ttlTimeProvider = ttlTimeProvider;
        this.timeServiceManagerProvider = Preconditions.checkNotNull(timeServiceManagerProvider);
        this.cancellationContext = cancellationContext;
    }

    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public StreamOperatorStateContext streamOperatorStateContext(
            @Nonnull OperatorID operatorID,
            @Nonnull String operatorClassName,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull KeyContext keyContext,
            @Nullable TypeSerializer<?> keySerializer,
            @Nonnull CloseableRegistry streamTaskCloseableRegistry,
            @Nonnull MetricGroup metricGroup,
            double managedMemoryFraction,
            boolean isUsingCustomRawKeyedState,
            boolean isAsyncState)
            throws Exception {

        TaskInfo taskInfo = environment.getTaskInfo();
        registerRestoredStateToFileMergingManager(environment.getJobID(), taskInfo, operatorID);

        OperatorSubtaskDescriptionText operatorSubtaskDescription =
                new OperatorSubtaskDescriptionText(
                        operatorID,
                        operatorClassName,
                        taskInfo.getIndexOfThisSubtask(),
                        taskInfo.getNumberOfParallelSubtasks());

        final String operatorIdentifierText = operatorSubtaskDescription.toString();
        final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
                taskStateManager.prioritizedOperatorState(operatorID);

        CheckpointableKeyedStateBackend<?> keyedStatedBackend = null;
        AsyncKeyedStateBackend asyncKeyedStateBackend = null;
        OperatorStateBackend operatorStateBackend = null;
        CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = null;
        CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs = null;
        InternalTimeServiceManager<?> timeServiceManager = null;
        InternalTimeServiceManager<?> asyncTimeServiceManager = null;

        final StateObject.StateObjectSizeStatsCollector statsCollector =
                StateObject.StateObjectSizeStatsCollector.create();

        try {

            // -------------- Keyed State Backend --------------
            if (isAsyncState) {
                if (stateBackend.supportsAsyncKeyedStateBackend()) {
                    asyncKeyedStateBackend =
                            keyedStatedBackend(
                                    keySerializer,
                                    operatorIdentifierText,
                                    prioritizedOperatorSubtaskStates,
                                    streamTaskCloseableRegistry,
                                    metricGroup,
                                    managedMemoryFraction,
                                    statsCollector,
                                    StateBackend::createAsyncKeyedStateBackend);
                } else {
                    asyncKeyedStateBackend =
                            new AsyncKeyedStateBackendAdaptor<>(
                                    keyedStatedBackend(
                                            keySerializer,
                                            operatorIdentifierText,
                                            prioritizedOperatorSubtaskStates,
                                            streamTaskCloseableRegistry,
                                            metricGroup,
                                            managedMemoryFraction,
                                            statsCollector,
                                            StateBackend::createKeyedStateBackend));
                }
            } else {
                keyedStatedBackend =
                        keyedStatedBackend(
                                keySerializer,
                                operatorIdentifierText,
                                prioritizedOperatorSubtaskStates,
                                streamTaskCloseableRegistry,
                                metricGroup,
                                managedMemoryFraction,
                                statsCollector,
                                StateBackend::createKeyedStateBackend);
            }

            // -------------- Operator State Backend --------------
            operatorStateBackend =
                    operatorStateBackend(
                            operatorIdentifierText,
                            prioritizedOperatorSubtaskStates,
                            streamTaskCloseableRegistry,
                            statsCollector);

            // -------------- Raw State Streams --------------
            rawKeyedStateInputs =
                    rawKeyedStateInputs(
                            prioritizedOperatorSubtaskStates
                                    .getPrioritizedRawKeyedState()
                                    .iterator(),
                            statsCollector);
            streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);

            rawOperatorStateInputs =
                    rawOperatorStateInputs(
                            prioritizedOperatorSubtaskStates
                                    .getPrioritizedRawOperatorState()
                                    .iterator(),
                            statsCollector);
            streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

            // -------------- Internal Timer Service Manager --------------
            // if the operator indicates that it is using custom raw keyed state,
            // then whatever was written in the raw keyed state snapshot was NOT written
            // by the internal timer services (because there is only ever one user of raw keyed
            // state);
            // in this case, timers should not attempt to restore timers from the raw keyed
            // state.
            final Iterable<KeyGroupStatePartitionStreamProvider> restoredRawKeyedStateTimers =
                    (prioritizedOperatorSubtaskStates.isRestored() && !isUsingCustomRawKeyedState)
                            ? rawKeyedStateInputs
                            : Collections.emptyList();
            if (isAsyncState) {
                if (asyncKeyedStateBackend != null) {
                    asyncTimeServiceManager =
                            timeServiceManagerProvider.create(
                                    environment.getMetricGroup().getIOMetricGroup(),
                                    asyncKeyedStateBackend,
                                    asyncKeyedStateBackend.getKeyGroupRange(),
                                    environment.getUserCodeClassLoader().asClassLoader(),
                                    keyContext,
                                    processingTimeService,
                                    restoredRawKeyedStateTimers,
                                    cancellationContext);
                }
            } else {
                if (keyedStatedBackend != null) {
                    timeServiceManager =
                            timeServiceManagerProvider.create(
                                    environment.getMetricGroup().getIOMetricGroup(),
                                    keyedStatedBackend,
                                    keyedStatedBackend.getKeyGroupRange(),
                                    environment.getUserCodeClassLoader().asClassLoader(),
                                    keyContext,
                                    processingTimeService,
                                    restoredRawKeyedStateTimers,
                                    cancellationContext);
                }
            }
            // Add stats for input channel and result partition state
            Stream.concat(
                            prioritizedOperatorSubtaskStates.getPrioritizedInputChannelState()
                                    .stream(),
                            prioritizedOperatorSubtaskStates.getPrioritizedResultSubpartitionState()
                                    .stream())
                    .filter(Objects::nonNull)
                    .forEach(channelHandle -> channelHandle.collectSizeStats(statsCollector));

            // Report collected stats to metrics
            statsCollector
                    .getStats()
                    .forEach(
                            (location, metricValue) ->
                                    initializationMetrics.addDurationMetric(
                                            MetricNames.RESTORED_STATE_SIZE + "." + location,
                                            metricValue));

            // -------------- Preparing return value --------------

            return new StreamOperatorStateContextImpl(
                    prioritizedOperatorSubtaskStates.getRestoredCheckpointId(),
                    operatorStateBackend,
                    keySerializer,
                    keyedStatedBackend,
                    asyncKeyedStateBackend,
                    timeServiceManager,
                    asyncTimeServiceManager,
                    rawOperatorStateInputs,
                    rawKeyedStateInputs);
        } catch (Exception ex) {

            // cleanup if something went wrong before results got published.
            if (keyedStatedBackend != null) {
                if (streamTaskCloseableRegistry.unregisterCloseable(keyedStatedBackend)) {
                    IOUtils.closeQuietly(keyedStatedBackend);
                }
                // release resource (e.g native resource)
                keyedStatedBackend.dispose();
            }

            if (asyncKeyedStateBackend != null) {
                if (streamTaskCloseableRegistry.unregisterCloseable(asyncKeyedStateBackend)) {
                    IOUtils.closeQuietly(asyncKeyedStateBackend);
                }
                // release resource (e.g native resource)
                asyncKeyedStateBackend.dispose();
            }

            if (operatorStateBackend != null) {
                if (streamTaskCloseableRegistry.unregisterCloseable(operatorStateBackend)) {
                    IOUtils.closeQuietly(operatorStateBackend);
                }
                operatorStateBackend.dispose();
            }

            if (streamTaskCloseableRegistry.unregisterCloseable(rawKeyedStateInputs)) {
                IOUtils.closeQuietly(rawKeyedStateInputs);
            }

            if (streamTaskCloseableRegistry.unregisterCloseable(rawOperatorStateInputs)) {
                IOUtils.closeQuietly(rawOperatorStateInputs);
            }

            throw new Exception("Exception while creating StreamOperatorStateContext.", ex);
        }
    }

    private void registerRestoredStateToFileMergingManager(
            JobID jobID, TaskInfo taskInfo, OperatorID operatorID) {
        FileMergingSnapshotManager fileMergingSnapshotManager =
                taskStateManager.getFileMergingSnapshotManager();
        Optional<Long> restoredCheckpointId = taskStateManager.getRestoreCheckpointId();
        if (fileMergingSnapshotManager == null || !restoredCheckpointId.isPresent()) {
            return;
        }
        Optional<OperatorSubtaskState> subtaskState =
                taskStateManager.getSubtaskJobManagerRestoredState(operatorID);
        if (subtaskState.isPresent()) {
            SubtaskFileMergingManagerRestoreOperation restoreOperation =
                    new SubtaskFileMergingManagerRestoreOperation(
                            restoredCheckpointId.get(),
                            fileMergingSnapshotManager,
                            jobID,
                            taskInfo,
                            operatorID,
                            subtaskState.get());
            restoreOperation.restore();
        }
    }

    protected OperatorStateBackend operatorStateBackend(
            String operatorIdentifierText,
            PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
            CloseableRegistry backendCloseableRegistry,
            StateObject.StateObjectSizeStatsCollector statsCollector)
            throws Exception {

        String logDescription = "operator state backend for " + operatorIdentifierText;

        // Now restore processing is included in backend building/constructing process, so we need
        // to make sure
        // each stream constructed in restore could also be closed in case of task cancel, for
        // example the data
        // input stream opened for serDe during restore.
        CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
        backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
        BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
                new BackendRestorerProcedure<>(
                        (stateHandles) ->
                                stateBackend.createOperatorStateBackend(
                                        new OperatorStateBackendParametersImpl(
                                                environment,
                                                operatorIdentifierText,
                                                stateHandles,
                                                cancelStreamRegistryForRestore)),
                        backendCloseableRegistry,
                        logDescription);

        try {
            return backendRestorer.createAndRestore(
                    prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState(),
                    statsCollector);
        } finally {
            if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
                IOUtils.closeQuietly(cancelStreamRegistryForRestore);
            }
        }
    }

    protected <K, R extends Disposable & Closeable> R keyedStatedBackend(
            TypeSerializer<K> keySerializer,
            String operatorIdentifierText,
            PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
            CloseableRegistry backendCloseableRegistry,
            MetricGroup metricGroup,
            double managedMemoryFraction,
            StateObject.StateObjectSizeStatsCollector statsCollector,
            KeyedStateBackendCreator<K, R> keyedStateBackendCreator)
            throws Exception {

        if (keySerializer == null) {
            return null;
        }

        String logDescription = "keyed state backend for " + operatorIdentifierText;

        TaskInfo taskInfo = environment.getTaskInfo();

        final KeyGroupRange keyGroupRange =
                KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                        taskInfo.getMaxNumberOfParallelSubtasks(),
                        taskInfo.getNumberOfParallelSubtasks(),
                        taskInfo.getIndexOfThisSubtask());

        // Now restore processing is included in backend building/constructing process, so we need
        // to make sure
        // each stream constructed in restore could also be closed in case of task cancel, for
        // example the data
        // input stream opened for serDe during restore.
        CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
        backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
        BackendRestorerProcedure<R, KeyedStateHandle> backendRestorer =
                new BackendRestorerProcedure<>(
                        (stateHandles) -> {
                            KeyedStateBackendParametersImpl<K> parameters =
                                    new KeyedStateBackendParametersImpl<>(
                                            environment,
                                            environment.getJobID(),
                                            operatorIdentifierText,
                                            keySerializer,
                                            taskInfo.getMaxNumberOfParallelSubtasks(),
                                            keyGroupRange,
                                            environment.getTaskKvStateRegistry(),
                                            ttlTimeProvider,
                                            metricGroup,
                                            initializationMetrics::addDurationMetric,
                                            stateHandles,
                                            cancelStreamRegistryForRestore,
                                            managedMemoryFraction);
                            return keyedStateBackendCreator.create(
                                    loadStateBackendFromKeyedStateHandles(
                                            stateBackend,
                                            environment.getUserCodeClassLoader().asClassLoader(),
                                            stateHandles),
                                    parameters);
                        },
                        backendCloseableRegistry,
                        logDescription);

        try {
            return backendRestorer.createAndRestore(
                    prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState(),
                    statsCollector);
        } finally {
            if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
                IOUtils.closeQuietly(cancelStreamRegistryForRestore);
            }
        }
    }

    /** Functional interface to create the keyed state backend. */
    @FunctionalInterface
    protected interface KeyedStateBackendCreator<K, R extends Disposable & Closeable> {

        /** Create the keyed state backend. */
        R create(
                StateBackend stateBackend,
                StateBackend.KeyedStateBackendParameters<K> keyedStateBackendParameters)
                throws Exception;
    }

    protected CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs(
            @Nonnull Iterator<StateObjectCollection<OperatorStateHandle>> restoreStateAlternatives,
            @Nonnull StateObject.StateObjectSizeStatsCollector statsCollector) {

        if (restoreStateAlternatives.hasNext()) {

            Collection<OperatorStateHandle> rawOperatorState = restoreStateAlternatives.next();
            // TODO currently this does not support local state recovery, so we expect there is only
            // one handle.
            Preconditions.checkState(
                    !restoreStateAlternatives.hasNext(),
                    "Local recovery is currently not implemented for raw operator state, but found state alternative.");

            if (rawOperatorState != null) {
                // Report restore size stats
                rawOperatorState.forEach(
                        stateObject -> stateObject.collectSizeStats(statsCollector));

                return new CloseableIterable<StatePartitionStreamProvider>() {

                    final CloseableRegistry closeableRegistry = new CloseableRegistry();

                    @Override
                    public void close() throws IOException {
                        closeableRegistry.close();
                    }

                    @Nonnull
                    @Override
                    public Iterator<StatePartitionStreamProvider> iterator() {
                        return new OperatorStateStreamIterator(
                                DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                                rawOperatorState.iterator(),
                                closeableRegistry);
                    }
                };
            }
        }

        return CloseableIterable.empty();
    }

    protected CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs(
            @Nonnull Iterator<StateObjectCollection<KeyedStateHandle>> restoreStateAlternatives,
            @Nonnull StateObject.StateObjectSizeStatsCollector statsCollector) {

        if (restoreStateAlternatives.hasNext()) {
            Collection<KeyedStateHandle> rawKeyedState = restoreStateAlternatives.next();

            // TODO currently this does not support local state recovery, so we expect there is only
            // one handle.
            Preconditions.checkState(
                    !restoreStateAlternatives.hasNext(),
                    "Local recovery is currently not implemented for raw keyed state, but found state alternative.");

            if (rawKeyedState != null) {
                Collection<KeyGroupsStateHandle> keyGroupsStateHandles = transform(rawKeyedState);
                // Report restore size stats
                keyGroupsStateHandles.forEach(
                        stateObject -> stateObject.collectSizeStats(statsCollector));
                final CloseableRegistry closeableRegistry = new CloseableRegistry();

                return new CloseableIterable<KeyGroupStatePartitionStreamProvider>() {
                    @Override
                    public void close() throws IOException {
                        closeableRegistry.close();
                    }

                    @Override
                    public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
                        return new KeyGroupStreamIterator(
                                keyGroupsStateHandles.iterator(), closeableRegistry);
                    }
                };
            }
        }

        return CloseableIterable.empty();
    }

    // =================================================================================================================

    private static class KeyGroupStreamIterator
            extends AbstractStateStreamIterator<
                    KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

        private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

        KeyGroupStreamIterator(
                Iterator<KeyGroupsStateHandle> stateHandleIterator,
                CloseableRegistry closableRegistry) {

            super(stateHandleIterator, closableRegistry);
        }

        @Override
        public boolean hasNext() {

            if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {

                return true;
            }

            closeCurrentStream();

            while (stateHandleIterator.hasNext()) {
                currentStateHandle = stateHandleIterator.next();
                if (currentStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
                    currentOffsetsIterator = unsetOffsetsSkippingIterator(currentStateHandle);

                    if (currentOffsetsIterator.hasNext()) {
                        return true;
                    }
                }
            }

            return false;
        }

        private static Iterator<Tuple2<Integer, Long>> unsetOffsetsSkippingIterator(
                KeyGroupsStateHandle keyGroupsStateHandle) {
            return StreamSupport.stream(
                            keyGroupsStateHandle.getGroupRangeOffsets().spliterator(), false)
                    .filter(
                            keyGroupIdAndOffset ->
                                    keyGroupIdAndOffset.f1
                                            != KeyedStateCheckpointOutputStream.NO_OFFSET_SET)
                    .iterator();
        }

        @Override
        public KeyGroupStatePartitionStreamProvider next() {

            if (!hasNext()) {

                throw new NoSuchElementException("Iterator exhausted");
            }

            Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
            try {
                if (null == currentStream) {
                    openCurrentStream();
                }

                currentStream.seek(keyGroupOffset.f1);
                return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);

            } catch (IOException ioex) {
                return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
            }
        }
    }

    private static class OperatorStateStreamIterator
            extends AbstractStateStreamIterator<StatePartitionStreamProvider, OperatorStateHandle> {

        private final String
                stateName; // TODO since we only support a single named state in raw, this could be
        // dropped
        private long[] offsets;
        private int offPos;

        OperatorStateStreamIterator(
                String stateName,
                Iterator<OperatorStateHandle> stateHandleIterator,
                CloseableRegistry closableRegistry) {

            super(stateHandleIterator, closableRegistry);
            this.stateName = Preconditions.checkNotNull(stateName);
        }

        @Override
        public boolean hasNext() {

            if (null != offsets && offPos < offsets.length) {

                return true;
            }

            closeCurrentStream();

            while (stateHandleIterator.hasNext()) {
                currentStateHandle = stateHandleIterator.next();
                OperatorStateHandle.StateMetaInfo metaInfo =
                        currentStateHandle.getStateNameToPartitionOffsets().get(stateName);

                if (null != metaInfo) {
                    long[] metaOffsets = metaInfo.getOffsets();
                    if (null != metaOffsets && metaOffsets.length > 0) {
                        this.offsets = metaOffsets;
                        this.offPos = 0;

                        if (closableRegistry.unregisterCloseable(currentStream)) {
                            IOUtils.closeQuietly(currentStream);
                            currentStream = null;
                        }

                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public StatePartitionStreamProvider next() {

            if (!hasNext()) {

                throw new NoSuchElementException("Iterator exhausted");
            }

            long offset = offsets[offPos++];

            try {
                if (null == currentStream) {
                    openCurrentStream();
                }

                currentStream.seek(offset);
                return new StatePartitionStreamProvider(currentStream);

            } catch (IOException ioex) {
                return new StatePartitionStreamProvider(ioex);
            }
        }
    }

    private abstract static class AbstractStateStreamIterator<
                    T extends StatePartitionStreamProvider, H extends StreamStateHandle>
            implements Iterator<T> {

        protected final Iterator<H> stateHandleIterator;
        protected final CloseableRegistry closableRegistry;

        protected H currentStateHandle;
        protected FSDataInputStream currentStream;

        AbstractStateStreamIterator(
                Iterator<H> stateHandleIterator, CloseableRegistry closableRegistry) {

            this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
            this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
        }

        protected void openCurrentStream() throws IOException {

            Preconditions.checkState(currentStream == null);

            FSDataInputStream stream = currentStateHandle.openInputStream();
            closableRegistry.registerCloseable(stream);
            currentStream = stream;
        }

        protected void closeCurrentStream() {
            if (closableRegistry.unregisterCloseable(currentStream)) {
                IOUtils.closeQuietly(currentStream);
            }
            currentStream = null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Read only Iterator");
        }
    }

    private static Collection<KeyGroupsStateHandle> transform(
            Collection<KeyedStateHandle> keyedStateHandles) {

        if (keyedStateHandles == null) {
            return null;
        }

        List<KeyGroupsStateHandle> keyGroupsStateHandles =
                new ArrayList<>(keyedStateHandles.size());

        for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {

            if (keyedStateHandle instanceof KeyGroupsStateHandle) {
                keyGroupsStateHandles.add((KeyGroupsStateHandle) keyedStateHandle);
            } else if (keyedStateHandle != null) {
                throw unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, keyedStateHandle.getClass());
            }
        }

        return keyGroupsStateHandles;
    }

    private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

        private final @Nullable Long restoredCheckpointId;

        private final OperatorStateBackend operatorStateBackend;
        private final @Nullable TypeSerializer<?> keySerializer;
        private final CheckpointableKeyedStateBackend<?> keyedStateBackend;
        private final AsyncKeyedStateBackend<?> asyncKeyedStateBackend;
        private final InternalTimeServiceManager<?> internalTimeServiceManager;
        private final InternalTimeServiceManager<?> asyncInternalTimeServiceManager;

        private final CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
        private final CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

        StreamOperatorStateContextImpl(
                @Nullable Long restoredCheckpointId,
                OperatorStateBackend operatorStateBackend,
                @Nullable TypeSerializer<?> keySerializer,
                CheckpointableKeyedStateBackend<?> keyedStateBackend,
                AsyncKeyedStateBackend<?> asyncKeyedStateBackend,
                InternalTimeServiceManager<?> internalTimeServiceManager,
                InternalTimeServiceManager<?> asyncInternalTimeServiceManager,
                CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs,
                CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs) {

            this.restoredCheckpointId = restoredCheckpointId;
            this.operatorStateBackend = operatorStateBackend;
            this.keySerializer = keySerializer;
            this.keyedStateBackend = keyedStateBackend;
            this.asyncKeyedStateBackend = asyncKeyedStateBackend;
            this.internalTimeServiceManager = internalTimeServiceManager;
            this.asyncInternalTimeServiceManager = asyncInternalTimeServiceManager;
            this.rawOperatorStateInputs = rawOperatorStateInputs;
            this.rawKeyedStateInputs = rawKeyedStateInputs;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(restoredCheckpointId);
        }

        @Override
        public TypeSerializer<?> keySerializer() {
            return keySerializer;
        }

        @Override
        public CheckpointableKeyedStateBackend<?> keyedStateBackend() {
            return keyedStateBackend;
        }

        @Override
        public AsyncKeyedStateBackend<?> asyncKeyedStateBackend() {
            return asyncKeyedStateBackend;
        }

        @Override
        public OperatorStateBackend operatorStateBackend() {
            return operatorStateBackend;
        }

        @Override
        public InternalTimeServiceManager<?> internalTimerServiceManager() {
            return internalTimeServiceManager;
        }

        @Override
        public InternalTimeServiceManager<?> asyncInternalTimerServiceManager() {
            return asyncInternalTimeServiceManager;
        }

        @Override
        public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
            return rawOperatorStateInputs;
        }

        @Override
        public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
            return rawKeyedStateInputs;
        }
    }
}
