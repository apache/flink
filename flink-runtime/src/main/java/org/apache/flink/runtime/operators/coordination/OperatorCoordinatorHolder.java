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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@code OperatorCoordinatorHolder} holds the {@link OperatorCoordinator} and manages all its
 * interactions with the remaining components. It provides the context and is responsible for
 * checkpointing and exactly once semantics.
 *
 * <h3>Exactly-one Semantics</h3>
 *
 * <p>The semantics are described under {@link OperatorCoordinator#checkpointCoordinator(long,
 * CompletableFuture)}.
 *
 * <h3>Exactly-one Mechanism</h3>
 *
 * <p>This implementation can handle one checkpoint being triggered at a time. If another checkpoint
 * is triggered while the triggering of the first one was not completed or aborted, this class will
 * throw an exception. That is in line with the capabilities of the Checkpoint Coordinator, which
 * can handle multiple concurrent checkpoints on the TaskManagers, but only one concurrent
 * triggering phase.
 *
 * <p>The mechanism for exactly once semantics is as follows:
 *
 * <ul>
 *   <li>Events pass through a special channel, the {@link OperatorEventValve}. If we are not
 *       currently triggering a checkpoint, then events simply pass through.
 *   <li>With the completion of the checkpoint future for the coordinator, this operator event valve
 *       is closed. Events coming after that are held back (buffered), because they belong to the
 *       epoch after the checkpoint.
 *   <li>Once all coordinators in the job have completed the checkpoint, the barriers to the sources
 *       are injected. After that (see {@link #afterSourceBarrierInjection(long)}) the valves are
 *       opened again and the events are sent.
 *   <li>If a task fails in the meantime, the events are dropped from the valve. From the
 *       coordinator's perspective, these events are lost, because they were sent to a failed
 *       subtask after it's latest complete checkpoint.
 * </ul>
 *
 * <p><b>IMPORTANT:</b> A critical assumption is that all events from the scheduler to the Tasks are
 * transported strictly in order. Events being sent from the coordinator after the checkpoint
 * barrier was injected must not overtake the checkpoint barrier. This is currently guaranteed by
 * Flink's RPC mechanism.
 *
 * <p>Consider this example:
 *
 * <pre>
 * Coordinator one events: => a . . b . |trigger| . . |complete| . . c . . d . |barrier| . e . f
 * Coordinator two events: => . . x . . |trigger| . . . . . . . . . .|complete||barrier| . . y . . z
 * </pre>
 *
 * <p>Two coordinators trigger checkpoints at the same time. 'Coordinator Two' takes longer to
 * complete, and in the meantime 'Coordinator One' sends more events.
 *
 * <p>'Coordinator One' emits events 'c' and 'd' after it finished its checkpoint, meaning the
 * events must take place after the checkpoint. But they are before the barrier injection, meaning
 * the runtime task would see them before the checkpoint, if they were immediately transported.
 *
 * <p>'Coordinator One' closes its valve as soon as the checkpoint future completes. Events 'c' and
 * 'd' get held back in the valve. Once 'Coordinator Two' completes its checkpoint, the barriers are
 * sent to the sources. Then the valves are opened, and events 'c' and 'd' can flow to the tasks
 * where they are received after the barrier.
 *
 * <h3>Concurrency and Threading Model</h3>
 *
 * <p>This component runs strictly in the Scheduler's main-thread-executor. All calls "from the
 * outside" are either already in the main-thread-executor (when coming from Scheduler) or put into
 * the main-thread-executor (when coming from the CheckpointCoordinator). We rely on the executor to
 * preserve strict order of the calls.
 *
 * <p>Actions from the coordinator to the "outside world" (like completing a checkpoint and sending
 * an event) are also enqueued back into the scheduler main-thread executor, strictly in order.
 */
public class OperatorCoordinatorHolder
        implements OperatorCoordinatorCheckpointContext, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorCoordinatorHolder.class);

    private final OperatorCoordinator coordinator;
    private final OperatorID operatorId;
    private final LazyInitializedCoordinatorContext context;
    private final SubtaskAccess.SubtaskAccessFactory taskAccesses;
    private final OperatorEventValve eventValve;
    private final IncompleteFuturesTracker unconfirmedEvents;

    private final int operatorParallelism;
    private final int operatorMaxParallelism;

    private Consumer<Throwable> globalFailureHandler;
    private ComponentMainThreadExecutor mainThreadExecutor;

    private OperatorCoordinatorHolder(
            final OperatorID operatorId,
            final OperatorCoordinator coordinator,
            final LazyInitializedCoordinatorContext context,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses,
            final int operatorParallelism,
            final int operatorMaxParallelism) {

        this.operatorId = checkNotNull(operatorId);
        this.coordinator = checkNotNull(coordinator);
        this.context = checkNotNull(context);
        this.taskAccesses = checkNotNull(taskAccesses);
        this.operatorParallelism = operatorParallelism;
        this.operatorMaxParallelism = operatorMaxParallelism;

        this.unconfirmedEvents = new IncompleteFuturesTracker();
        this.eventValve = new OperatorEventValve();
    }

    public void lazyInitialize(
            Consumer<Throwable> globalFailureHandler,
            ComponentMainThreadExecutor mainThreadExecutor) {

        this.globalFailureHandler = globalFailureHandler;
        this.mainThreadExecutor = mainThreadExecutor;

        eventValve.setMainThreadExecutorForValidation(mainThreadExecutor);
        context.lazyInitialize(globalFailureHandler, mainThreadExecutor);

        setupAllSubtaskGateways();
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public OperatorCoordinator coordinator() {
        return coordinator;
    }

    @Override
    public OperatorID operatorId() {
        return operatorId;
    }

    @Override
    public int maxParallelism() {
        return operatorMaxParallelism;
    }

    @Override
    public int currentParallelism() {
        return operatorParallelism;
    }

    // ------------------------------------------------------------------------
    //  OperatorCoordinator Interface
    // ------------------------------------------------------------------------

    public void start() throws Exception {
        mainThreadExecutor.assertRunningInMainThread();
        checkState(context.isInitialized(), "Coordinator Context is not yet initialized");
        coordinator.start();
    }

    @Override
    public void close() throws Exception {
        coordinator.close();
        context.unInitialize();
    }

    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        mainThreadExecutor.assertRunningInMainThread();
        coordinator.handleEventFromOperator(subtask, event);
    }

    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        mainThreadExecutor.assertRunningInMainThread();
        coordinator.subtaskFailed(subtask, reason);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        mainThreadExecutor.assertRunningInMainThread();

        // this needs to happen first, so that the coordinator may access the gateway
        // in the 'subtaskReset()' function (even though they cannot send events, yet).
        setupSubtaskGateway(subtask);

        coordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(() -> checkpointCoordinatorInternal(checkpointId, result));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(() -> coordinator.notifyCheckpointComplete(checkpointId));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(() -> coordinator.notifyCheckpointAborted(checkpointId));
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        // the first time this method is called is early during execution graph construction,
        // before the main thread executor is set. hence this conditional check.
        if (mainThreadExecutor != null) {
            mainThreadExecutor.assertRunningInMainThread();
        }

        eventValve.openValveAndUnmarkCheckpoint();
        context.resetFailed();

        // when initial savepoints are restored, this call comes before the mainThreadExecutor
        // is available, which is needed to set up these gateways. So during the initial restore,
        // we ignore this, and instead the gateways are set up in the "lazyInitialize" method, which
        // is called when the scheduler is properly set up.
        // this is a bit clumsy, but it is caused by the non-straightforward initialization of the
        // ExecutionGraph and Scheduler.
        if (mainThreadExecutor != null) {
            setupAllSubtaskGateways();
        }

        coordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    private void checkpointCoordinatorInternal(
            final long checkpointId, final CompletableFuture<byte[]> result) {
        mainThreadExecutor.assertRunningInMainThread();

        final CompletableFuture<byte[]> coordinatorCheckpoint = new CompletableFuture<>();

        FutureUtils.assertNoException(
                coordinatorCheckpoint.handleAsync(
                        (success, failure) -> {
                            if (failure != null) {
                                result.completeExceptionally(failure);
                            } else if (eventValve.tryShutValve(checkpointId)) {
                                completeCheckpointOnceEventsAreDone(checkpointId, result, success);
                            } else {
                                // if we cannot shut the valve, this means the checkpoint
                                // has been aborted before, so the future is already
                                // completed exceptionally. but we try to complete it here
                                // again, just in case, as a safety net.
                                result.completeExceptionally(
                                        new FlinkException("Cannot shut event valve"));
                            }
                            return null;
                        },
                        mainThreadExecutor));

        try {
            eventValve.markForCheckpoint(checkpointId);
            coordinator.checkpointCoordinator(checkpointId, coordinatorCheckpoint);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            result.completeExceptionally(t);
            globalFailureHandler.accept(t);
        }
    }

    private void completeCheckpointOnceEventsAreDone(
            final long checkpointId,
            final CompletableFuture<byte[]> checkpointFuture,
            final byte[] checkpointResult) {

        final Collection<CompletableFuture<?>> pendingEvents =
                unconfirmedEvents.getCurrentIncompleteAndReset();
        if (pendingEvents.isEmpty()) {
            checkpointFuture.complete(checkpointResult);
            return;
        }

        LOG.info(
                "Coordinator checkpoint {} for coordinator {} is awaiting {} pending events",
                checkpointId,
                operatorId,
                pendingEvents.size());

        final CompletableFuture<?> conjunct = FutureUtils.waitForAll(pendingEvents);
        conjunct.whenComplete(
                (success, failure) -> {
                    if (failure == null) {
                        checkpointFuture.complete(checkpointResult);
                    } else {
                        // if we reach this situation, then anyways the checkpoint cannot
                        // complete because
                        // (a) the target task really is down
                        // (b) we have a potentially lost RPC message and need to
                        //     do a task failover for the receiver to restore consistency
                        checkpointFuture.completeExceptionally(
                                new FlinkException(
                                        "Failing OperatorCoordinator checkpoint because some OperatorEvents "
                                                + "before this checkpoint barrier were not received by the target tasks."));
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  Checkpointing Callbacks
    // ------------------------------------------------------------------------

    @Override
    public void afterSourceBarrierInjection(long checkpointId) {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(() -> eventValve.openValveAndUnmarkCheckpoint(checkpointId));
    }

    @Override
    public void abortCurrentTriggering() {
        // unfortunately, this method does not run in the scheduler executor, but in the
        // checkpoint coordinator time thread.
        // we can remove the delegation once the checkpoint coordinator runs fully in the
        // scheduler's main thread executor
        mainThreadExecutor.execute(eventValve::openValveAndUnmarkCheckpoint);
    }

    // ------------------------------------------------------------------------
    //  miscellaneous helpers
    // ------------------------------------------------------------------------

    private void setupAllSubtaskGateways() {
        for (int i = 0; i < operatorParallelism; i++) {
            setupSubtaskGateway(i);
        }
    }

    private void setupSubtaskGateway(int subtask) {
        // this gets an access to the latest task execution attempt.
        final SubtaskAccess sta = taskAccesses.getAccessForSubtask(subtask);

        final OperatorCoordinator.SubtaskGateway gateway =
                new SubtaskGatewayImpl(sta, eventValve, mainThreadExecutor, unconfirmedEvents);

        // We need to do this synchronously here, otherwise we violate the contract that
        // 'subtaskFailed()' will never overtake 'subtaskReady()'.
        // ---
        // It is also possible that by the time this method here is called, the task execution is in
        // a no-longer running state. That happens when the scheduler deals with overlapping global
        // failures and the restore method is in fact not yet restoring to the new execution
        // attempts, but still targeting the previous execution attempts (and is later subsumed
        // by another restore to the new execution attempt). This is tricky behavior that we need
        // to work around. So if the task is no longer running, we don't call the 'subtaskReady()'
        // method.
        FutureUtils.assertNoException(
                sta.hasSwitchedToRunning()
                        .thenAccept(
                                (ignored) -> {
                                    mainThreadExecutor.assertRunningInMainThread();

                                    // see bigger comment above
                                    if (sta.isStillRunning()) {
                                        notifySubtaskReady(subtask, gateway);
                                    }
                                }));
    }

    private void notifySubtaskReady(int subtask, OperatorCoordinator.SubtaskGateway gateway) {
        try {
            coordinator.subtaskReady(subtask, gateway);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            globalFailureHandler.accept(new FlinkException("Error from OperatorCoordinator", t));
        }
    }

    // ------------------------------------------------------------------------
    //  Factories
    // ------------------------------------------------------------------------

    public static OperatorCoordinatorHolder create(
            SerializedValue<OperatorCoordinator.Provider> serializedProvider,
            ExecutionJobVertex jobVertex,
            ClassLoader classLoader)
            throws Exception {

        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            final OperatorCoordinator.Provider provider =
                    serializedProvider.deserializeValue(classLoader);
            final OperatorID opId = provider.getOperatorId();

            final SubtaskAccess.SubtaskAccessFactory taskAccesses =
                    new ExecutionSubtaskAccess.ExecutionJobVertexSubtaskAccess(jobVertex, opId);

            return create(
                    opId,
                    provider,
                    jobVertex.getName(),
                    jobVertex.getGraph().getUserClassLoader(),
                    jobVertex.getParallelism(),
                    jobVertex.getMaxParallelism(),
                    taskAccesses);
        }
    }

    @VisibleForTesting
    static OperatorCoordinatorHolder create(
            final OperatorID opId,
            final OperatorCoordinator.Provider coordinatorProvider,
            final String operatorName,
            final ClassLoader userCodeClassLoader,
            final int operatorParallelism,
            final int operatorMaxParallelism,
            final SubtaskAccess.SubtaskAccessFactory taskAccesses)
            throws Exception {

        final LazyInitializedCoordinatorContext context =
                new LazyInitializedCoordinatorContext(
                        opId, operatorName, userCodeClassLoader, operatorParallelism);

        final OperatorCoordinator coordinator = coordinatorProvider.create(context);

        return new OperatorCoordinatorHolder(
                opId,
                coordinator,
                context,
                taskAccesses,
                operatorParallelism,
                operatorMaxParallelism);
    }

    // ------------------------------------------------------------------------
    //  Nested Classes
    // ------------------------------------------------------------------------

    /**
     * An implementation of the {@link OperatorCoordinator.Context}.
     *
     * <p>All methods are safe to be called from other threads than the Scheduler's and the
     * JobMaster's main threads.
     *
     * <p>Implementation note: Ideally, we would like to operate purely against the scheduler
     * interface, but it is not exposing enough information at the moment.
     */
    private static final class LazyInitializedCoordinatorContext
            implements OperatorCoordinator.Context {

        private static final Logger LOG =
                LoggerFactory.getLogger(LazyInitializedCoordinatorContext.class);

        private final OperatorID operatorId;
        private final String operatorName;
        private final ClassLoader userCodeClassLoader;
        private final int operatorParallelism;

        private Consumer<Throwable> globalFailureHandler;
        private Executor schedulerExecutor;

        private volatile boolean failed;

        public LazyInitializedCoordinatorContext(
                final OperatorID operatorId,
                final String operatorName,
                final ClassLoader userCodeClassLoader,
                final int operatorParallelism) {
            this.operatorId = checkNotNull(operatorId);
            this.operatorName = checkNotNull(operatorName);
            this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
            this.operatorParallelism = operatorParallelism;
        }

        void lazyInitialize(Consumer<Throwable> globalFailureHandler, Executor schedulerExecutor) {
            this.globalFailureHandler = checkNotNull(globalFailureHandler);
            this.schedulerExecutor = checkNotNull(schedulerExecutor);
        }

        void unInitialize() {
            this.globalFailureHandler = null;
            this.schedulerExecutor = null;
        }

        boolean isInitialized() {
            return schedulerExecutor != null;
        }

        private void checkInitialized() {
            checkState(isInitialized(), "Context was not yet initialized");
        }

        void resetFailed() {
            failed = false;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public void failJob(final Throwable cause) {
            checkInitialized();

            final FlinkException e =
                    new FlinkException(
                            "Global failure triggered by OperatorCoordinator for '"
                                    + operatorName
                                    + "' (operator "
                                    + operatorId
                                    + ").",
                            cause);

            if (failed) {
                LOG.debug(
                        "Ignoring the request to fail job because the job is already failing. "
                                + "The ignored failure cause is",
                        e);
                return;
            }
            failed = true;

            schedulerExecutor.execute(() -> globalFailureHandler.accept(e));
        }

        @Override
        public int currentParallelism() {
            return operatorParallelism;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return userCodeClassLoader;
        }
    }
}
