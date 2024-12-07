/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsIntermediateNoMoreSplits;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.shutdownExecutorForcefully;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A context class for the {@link OperatorCoordinator}. Compared with {@link SplitEnumeratorContext}
 * this class allows interaction with state and sending {@link OperatorEvent} to the SourceOperator
 * while {@link SplitEnumeratorContext} only allows sending {@link SourceEvent}.
 *
 * <p>The context serves a few purposes:
 *
 * <ul>
 *   <li>Information provider - The context provides necessary information to the enumerator for it
 *       to know what is the status of the source readers and their split assignments. These
 *       information allows the split enumerator to do the coordination.
 *   <li>Action taker - The context also provides a few actions that the enumerator can take to
 *       carry out the coordination. So far there are two actions: 1) assign splits to the source
 *       readers. and 2) sens a custom {@link SourceEvent SourceEvents} to the source readers.
 *   <li>Thread model enforcement - The context ensures that all the manipulations to the
 *       coordinator state are handled by the same thread.
 * </ul>
 *
 * @param <SplitT> the type of the splits.
 */
@Internal
public class SourceCoordinatorContext<SplitT extends SourceSplit>
        implements SplitEnumeratorContext<SplitT>, SupportsIntermediateNoMoreSplits, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorContext.class);

    private final ScheduledExecutorService workerExecutor;
    private final ScheduledExecutorService coordinatorExecutor;
    private final ExecutorNotifier notifier;
    private final OperatorCoordinator.Context operatorCoordinatorContext;
    private final SimpleVersionedSerializer<SplitT> splitSerializer;
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, ReaderInfo>> registeredReaders;
    private final SplitAssignmentTracker<SplitT> assignmentTracker;
    private final SourceCoordinatorProvider.CoordinatorExecutorThreadFactory
            coordinatorThreadFactory;
    private SubtaskGateways subtaskGateways;
    private final String coordinatorThreadName;
    private final boolean supportsConcurrentExecutionAttempts;
    private boolean[] subtaskHasNoMoreSplits;
    private volatile boolean closed;
    private volatile TernaryBoolean backlog = TernaryBoolean.UNDEFINED;

    public SourceCoordinatorContext(
            JobID jobID,
            SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            int numWorkerThreads,
            OperatorCoordinator.Context operatorCoordinatorContext,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            boolean supportsConcurrentExecutionAttempts) {
        this(
                jobID,
                Executors.newScheduledThreadPool(1, coordinatorThreadFactory),
                Executors.newScheduledThreadPool(
                        numWorkerThreads,
                        new ExecutorThreadFactory(
                                coordinatorThreadFactory.getCoordinatorThreadName() + "-worker")),
                coordinatorThreadFactory,
                operatorCoordinatorContext,
                splitSerializer,
                new SplitAssignmentTracker<>(),
                supportsConcurrentExecutionAttempts);
    }

    // Package private method for unit test.
    @VisibleForTesting
    SourceCoordinatorContext(
            JobID jobID,
            ScheduledExecutorService coordinatorExecutor,
            ScheduledExecutorService workerExecutor,
            SourceCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            SplitAssignmentTracker<SplitT> splitAssignmentTracker,
            boolean supportsConcurrentExecutionAttempts) {
        this.workerExecutor = workerExecutor;
        this.coordinatorExecutor = MdcUtils.scopeToJob(jobID, coordinatorExecutor);
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.splitSerializer = splitSerializer;
        this.registeredReaders = new ConcurrentHashMap<>();
        this.assignmentTracker = splitAssignmentTracker;
        this.coordinatorThreadName = coordinatorThreadFactory.getCoordinatorThreadName();
        this.supportsConcurrentExecutionAttempts = supportsConcurrentExecutionAttempts;

        final Executor errorHandlingCoordinatorExecutor =
                (runnable) ->
                        this.coordinatorExecutor.execute(
                                new ThrowableCatchingRunnable(
                                        this::handleUncaughtExceptionFromAsyncCall, runnable));

        this.notifier = new ExecutorNotifier(workerExecutor, errorHandlingCoordinatorExecutor);
    }

    boolean isConcurrentExecutionAttemptsSupported() {
        return supportsConcurrentExecutionAttempts;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return new InternalSplitEnumeratorMetricGroup(operatorCoordinatorContext.metricGroup());
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        checkAndLazyInitialize();
        checkState(
                !supportsConcurrentExecutionAttempts,
                "The split enumerator must invoke SplitEnumeratorContext"
                        + "#sendEventToSourceReader(int, int, SourceEvent) instead of "
                        + "SplitEnumeratorContext#sendEventToSourceReader(int, SourceEvent) "
                        + "to send custom source events in concurrent execution attempts scenario "
                        + "(e.g. if speculative execution is enabled).");
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndCheckReady(subtaskId);
                    gateway.sendEvent(new SourceEventWrapper(event));
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getGatewayAndCheckReady(subtaskId, attemptNumber);
                    gateway.sendEvent(new SourceEventWrapper(event));
                    return null;
                },
                String.format(
                        "Failed to send event %s to subtask %d (#%d)",
                        event, subtaskId, attemptNumber));
    }

    void sendEventToSourceOperator(int subtaskId, OperatorEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndCheckReady(subtaskId);
                    gateway.sendEvent(event);
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    @VisibleForTesting
    ScheduledExecutorService getCoordinatorExecutor() {
        return coordinatorExecutor;
    }

    void sendEventToSourceOperatorIfTaskReady(int subtaskId, OperatorEvent event) {
        checkAndLazyInitialize();
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            subtaskGateways.getOnlyGatewayAndNotCheckReady(subtaskId);
                    if (gateway != null) {
                        gateway.sendEvent(event);
                    }

                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    @Override
    public int currentParallelism() {
        return operatorCoordinatorContext.currentParallelism();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        final Map<Integer, ReaderInfo> readers = new HashMap<>();
        for (Map.Entry<Integer, ConcurrentMap<Integer, ReaderInfo>> entry :
                registeredReaders.entrySet()) {
            final int subtaskIndex = entry.getKey();
            final Map<Integer, ReaderInfo> attemptReaders = entry.getValue();
            int earliestAttempt = Integer.MAX_VALUE;
            for (int attemptNumber : attemptReaders.keySet()) {
                if (attemptNumber < earliestAttempt) {
                    earliestAttempt = attemptNumber;
                }
            }
            readers.put(subtaskIndex, attemptReaders.get(earliestAttempt));
        }
        return Collections.unmodifiableMap(readers);
    }

    @Override
    public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
        return Collections.unmodifiableMap(registeredReaders);
    }

    @Override
    public void assignSplits(SplitsAssignment<SplitT> assignment) {
        // Ensure the split assignment is done by the coordinator executor.
        callInCoordinatorThread(
                () -> {
                    // Ensure all the subtasks in the assignment have registered.
                    assignment
                            .assignment()
                            .forEach(
                                    (id, splits) -> {
                                        if (!registeredReaders.containsKey(id)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Cannot assign splits %s to subtask %d because the subtask is not registered.",
                                                            splits, id));
                                        }
                                    });

                    assignmentTracker.recordSplitAssignment(assignment);
                    assignSplitsToAttempts(assignment);
                    return null;
                },
                String.format("Failed to assign splits %s due to ", assignment));
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        checkSubtaskIndex(subtask);

        // Ensure the split assignment is done by the coordinator executor.
        callInCoordinatorThread(
                () -> {
                    subtaskHasNoMoreSplits[subtask] = true;
                    signalNoMoreSplitsToAttempts(subtask);
                    return null; // void return value
                },
                "Failed to send 'NoMoreSplits' to reader " + subtask);
    }

    @Override
    public void signalIntermediateNoMoreSplits(int subtask) {
        checkSubtaskIndex(subtask);

        // It's an intermediate noMoreSplit event, notify subtask to deal with this event.
        callInCoordinatorThread(
                () -> {
                    signalNoMoreSplitsToAttempts(subtask);
                    return null;
                },
                "Failed to send 'IntermediateNoMoreSplits' to reader " + subtask);
    }

    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        notifier.notifyReadyAsync(callable, handler, initialDelay, period);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        notifier.notifyReadyAsync(callable, handler);
    }

    /** {@inheritDoc} If the runnable throws an Exception, the corresponding job is failed. */
    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        // when using a ScheduledThreadPool, uncaught exception handler catches only
        // exceptions thrown by the threadPool, so manually call it when the exception is
        // thrown by the runnable
        coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        runnable));
    }

    @Override
    public void close() throws InterruptedException {
        closed = true;
        // Close quietly so the closing sequence will be executed completely.
        shutdownExecutorForcefully(workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        shutdownExecutorForcefully(coordinatorExecutor, Duration.ofNanos(Long.MAX_VALUE));
    }

    @VisibleForTesting
    boolean isClosed() {
        return closed;
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {
        CheckpointCoordinator checkpointCoordinator =
                getCoordinatorContext().getCheckpointCoordinator();
        OperatorID operatorID = getCoordinatorContext().getOperatorId();
        if (checkpointCoordinator != null) {
            checkpointCoordinator.setIsProcessingBacklog(operatorID, isProcessingBacklog);
        }
        backlog = TernaryBoolean.fromBoolean(isProcessingBacklog);
        callInCoordinatorThread(
                () -> {
                    final IsProcessingBacklogEvent isProcessingBacklogEvent =
                            new IsProcessingBacklogEvent(isProcessingBacklog);
                    for (int i = 0; i < getCoordinatorContext().currentParallelism(); i++) {
                        sendEventToSourceOperatorIfTaskReady(i, isProcessingBacklogEvent);
                    }
                    return null;
                },
                "Failed to send BacklogEvent to reader.");
    }

    // --------- Package private additional methods for the SourceCoordinator ------------

    void attemptReady(OperatorCoordinator.SubtaskGateway gateway) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread());

        subtaskGateways.registerSubtaskGateway(gateway);
    }

    void attemptFailed(int subtaskIndex, int attemptNumber) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread());

        subtaskGateways.unregisterSubtaskGateway(subtaskIndex, attemptNumber);
    }

    void subtaskReset(int subtaskIndex) {
        checkAndLazyInitialize();
        checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread());

        subtaskGateways.reset(subtaskIndex);
        registeredReaders.remove(subtaskIndex);
        subtaskHasNoMoreSplits[subtaskIndex] = false;
    }

    boolean hasNoMoreSplits(int subtaskIndex) {
        checkAndLazyInitialize();
        return subtaskHasNoMoreSplits[subtaskIndex];
    }

    /**
     * Fail the job with the given cause.
     *
     * @param cause the cause of the job failure.
     */
    void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause);
    }

    void handleUncaughtExceptionFromAsyncCall(Throwable t) {
        if (closed) {
            return;
        }

        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
        LOG.error(
                "Exception while handling result from async call in {}. Triggering job failover.",
                coordinatorThreadName,
                t);
        failJob(t);
    }

    /**
     * Behavior of SourceCoordinatorContext on checkpoint.
     *
     * @param checkpointId The id of the ongoing checkpoint.
     */
    void onCheckpoint(long checkpointId) throws Exception {
        assignmentTracker.onCheckpoint(checkpointId);
    }

    /**
     * Register a source reader.
     *
     * @param subtaskId the subtask id of the source reader.
     * @param attemptNumber the attempt number of the source reader.
     * @param location the location of the source reader.
     */
    void registerSourceReader(int subtaskId, int attemptNumber, String location) {
        final Map<Integer, ReaderInfo> attemptReaders =
                registeredReaders.computeIfAbsent(subtaskId, k -> new ConcurrentHashMap<>());
        checkState(
                !attemptReaders.containsKey(attemptNumber),
                "ReaderInfo of subtask %s (#%s) already exists.",
                subtaskId,
                attemptNumber);
        attemptReaders.put(attemptNumber, new ReaderInfo(subtaskId, location));

        sendCachedSplitsToNewlyRegisteredReader(subtaskId, attemptNumber);
    }

    /**
     * Unregister a source reader.
     *
     * @param subtaskId the subtask id of the source reader.
     * @param attemptNumber the attempt number of the source reader.
     */
    void unregisterSourceReader(int subtaskId, int attemptNumber) {
        final Map<Integer, ReaderInfo> attemptReaders = registeredReaders.get(subtaskId);
        if (attemptReaders != null) {
            attemptReaders.remove(attemptNumber);

            if (attemptReaders.isEmpty()) {
                registeredReaders.remove(subtaskId);
            }
        }
    }

    /**
     * Get the split to put back. This only happens when a source reader subtask has failed.
     *
     * @param subtaskId the failed subtask id.
     * @param restoredCheckpointId the checkpoint that the task is recovered to.
     * @return A list of splits that needs to be added back to the {@link SplitEnumerator}.
     */
    List<SplitT> getAndRemoveUncheckpointedAssignment(int subtaskId, long restoredCheckpointId) {
        return assignmentTracker.getAndRemoveUncheckpointedAssignment(
                subtaskId, restoredCheckpointId);
    }

    /**
     * Invoked when a successful checkpoint has been taken.
     *
     * @param checkpointId the id of the successful checkpoint.
     */
    void onCheckpointComplete(long checkpointId) {
        assignmentTracker.onCheckpointComplete(checkpointId);
    }

    OperatorCoordinator.Context getCoordinatorContext() {
        return operatorCoordinatorContext;
    }

    SplitAssignmentTracker<SplitT> getAssignmentTracker() {
        return assignmentTracker;
    }

    // ---------------- Executor methods to avoid use coordinatorExecutor directly -----------------

    Future<?> submitTask(Runnable task) {
        return coordinatorExecutor.submit(task);
    }

    /** To avoid period task lost, we should handle the potential exception throw by task. */
    ScheduledFuture<?> schedulePeriodTask(
            Runnable command, long initDelay, long period, TimeUnit unit) {
        return coordinatorExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        command.run();
                    } catch (Throwable t) {
                        handleUncaughtExceptionFromAsyncCall(t);
                    }
                },
                initDelay,
                period,
                unit);
    }

    CompletableFuture<?> supplyAsync(Supplier<?> task) {
        return CompletableFuture.supplyAsync(task, coordinatorExecutor);
    }

    // ---------------- private helper methods -----------------

    private void checkSubtaskIndex(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= getCoordinatorContext().currentParallelism()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Subtask index %d is out of bounds [0, %s)",
                            subtaskIndex, getCoordinatorContext().currentParallelism()));
        }
    }

    private void checkAndLazyInitialize() {
        if (subtaskGateways == null) {
            final int parallelism = operatorCoordinatorContext.currentParallelism();
            checkState(parallelism != ExecutionConfig.PARALLELISM_DEFAULT);
            this.subtaskGateways = new SubtaskGateways(parallelism);
            this.subtaskHasNoMoreSplits = new boolean[parallelism];
            Arrays.fill(subtaskHasNoMoreSplits, false);
        }
    }

    /**
     * A helper method that delegates the callable to the coordinator thread if the current thread
     * is not the coordinator thread, otherwise call the callable right away.
     *
     * @param callable the callable to delegate.
     */
    private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // Ensure the split assignment is done by the coordinator executor.
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
            try {
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                return callable.call();
                            } catch (Throwable t) {
                                LOG.error("Uncaught Exception in Source Coordinator Executor", t);
                                ExceptionUtils.rethrowException(t);
                                return null;
                            }
                        };

                return coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FlinkRuntimeException(errorMessage, e);
            }
        }

        try {
            return callable.call();
        } catch (Throwable t) {
            LOG.error("Uncaught Exception in Source Coordinator Executor", t);
            throw new FlinkRuntimeException(errorMessage, t);
        }
    }

    private void assignSplitsToAttempts(SplitsAssignment<SplitT> assignment) {
        assignment.assignment().forEach((index, splits) -> assignSplitsToAttempts(index, splits));
    }

    private void assignSplitsToAttempts(int subtaskIndex, List<SplitT> splits) {
        getRegisteredAttempts(subtaskIndex)
                .forEach(attempt -> assignSplitsToAttempt(subtaskIndex, attempt, splits));
    }

    private void assignSplitsToAttempt(int subtaskIndex, int attemptNumber, List<SplitT> splits) {
        checkAndLazyInitialize();
        if (splits.isEmpty()) {
            return;
        }

        checkAttemptReaderReady(subtaskIndex, attemptNumber);

        final AddSplitEvent<SplitT> addSplitEvent;
        try {
            addSplitEvent = new AddSplitEvent<>(splits, splitSerializer);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to serialize splits.", e);
        }

        final OperatorCoordinator.SubtaskGateway gateway =
                subtaskGateways.getGatewayAndCheckReady(subtaskIndex, attemptNumber);
        gateway.sendEvent(addSplitEvent);
    }

    private void signalNoMoreSplitsToAttempts(int subtaskIndex) {
        getRegisteredAttempts(subtaskIndex)
                .forEach(attemptNumber -> signalNoMoreSplitsToAttempt(subtaskIndex, attemptNumber));
    }

    private void signalNoMoreSplitsToAttempt(int subtaskIndex, int attemptNumber) {
        checkAndLazyInitialize();
        checkAttemptReaderReady(subtaskIndex, attemptNumber);

        final OperatorCoordinator.SubtaskGateway gateway =
                subtaskGateways.getGatewayAndCheckReady(subtaskIndex, attemptNumber);
        gateway.sendEvent(new NoMoreSplitsEvent());
    }

    private void checkAttemptReaderReady(int subtaskIndex, int attemptNumber) {
        checkState(registeredReaders.containsKey(subtaskIndex));
        checkState(getRegisteredAttempts(subtaskIndex).contains(attemptNumber));
    }

    private Set<Integer> getRegisteredAttempts(int subtaskIndex) {
        return registeredReaders.get(subtaskIndex).keySet();
    }

    private void sendCachedSplitsToNewlyRegisteredReader(int subtaskIndex, int attemptNumber) {
        // For batch jobs, checkpoints will never happen so that the un-checkpointed assignments in
        // assignmentTracker can be seen as cached splits. For streaming jobs,
        // #supportsConcurrentExecutionAttempts should be false and un-checkpointed assignments
        // should be empty when a new reader is registered (cleared when the last reader failed).
        final LinkedHashSet<SplitT> cachedSplits =
                assignmentTracker.uncheckpointedAssignments().get(subtaskIndex);

        if (cachedSplits != null) {
            if (!supportsConcurrentExecutionAttempts) {
                throw new IllegalStateException("No cached split is expected.");
            }
            assignSplitsToAttempt(subtaskIndex, attemptNumber, new ArrayList<>(cachedSplits));
        }

        if (supportsConcurrentExecutionAttempts && hasNoMoreSplits(subtaskIndex)) {
            signalNoMoreSplitsToAttempt(subtaskIndex, attemptNumber);
        }
    }

    /**
     * Returns whether the Source is processing backlog data. UNDEFINED is returned if it is not set
     * by the {@link #setIsProcessingBacklog} method.
     */
    public TernaryBoolean isBacklog() {
        return backlog;
    }

    /** Maintains the subtask gateways for different execution attempts of different subtasks. */
    private static class SubtaskGateways {
        private final Map<Integer, OperatorCoordinator.SubtaskGateway>[] gateways;

        private SubtaskGateways(int parallelism) {
            gateways = new Map[parallelism];
            for (int i = 0; i < parallelism; i++) {
                gateways[i] = new HashMap<>();
            }
        }

        private void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
            final int subtaskIndex = gateway.getSubtask();
            final int attemptNumber = gateway.getExecution().getAttemptNumber();

            checkState(
                    !gateways[subtaskIndex].containsKey(attemptNumber),
                    "Already have a subtask gateway for %s (#%s).",
                    subtaskIndex,
                    attemptNumber);
            gateways[subtaskIndex].put(attemptNumber, gateway);
        }

        private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
            gateways[subtaskIndex].remove(attemptNumber);
        }

        private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndCheckReady(int subtaskIndex) {
            checkState(
                    gateways[subtaskIndex].size() > 0,
                    "Subtask %s is not ready yet to receive events.",
                    subtaskIndex);

            return Iterables.getOnlyElement(gateways[subtaskIndex].values());
        }

        private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndNotCheckReady(
                int subtaskIndex) {
            if (gateways[subtaskIndex].size() > 0) {
                return Iterables.getOnlyElement(gateways[subtaskIndex].values());
            } else {
                return null;
            }
        }

        private OperatorCoordinator.SubtaskGateway getGatewayAndCheckReady(
                int subtaskIndex, int attemptNumber) {
            final OperatorCoordinator.SubtaskGateway gateway =
                    gateways[subtaskIndex].get(attemptNumber);
            if (gateway != null) {
                return gateway;
            }

            throw new IllegalStateException(
                    String.format(
                            "Subtask %d (#%d) is not ready yet to receive events.",
                            subtaskIndex, attemptNumber));
        }

        private void reset(int subtaskIndex) {
            gateways[subtaskIndex].clear();
        }
    }
}
