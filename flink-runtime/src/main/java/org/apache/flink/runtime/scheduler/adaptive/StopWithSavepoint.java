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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * Tracks a "stop with savepoint" operation. The incoming "savepointFuture" is coming from the
 * {@link CheckpointCoordinator}, which takes care of triggering a savepoint, and then shutting down
 * the job (on success).
 *
 * <p>This state is tracking the future to act accordingly on it. The savepoint path (= the result
 * of the operation) is made available via the "operationFuture" to the user. This operation is only
 * considered successfully if the "savepointFuture" completed successfully, and the job reached the
 * terminal state FINISHED.
 *
 * <p>This state has to cover several failure scenarios, depending on whether the savepoint
 * succeeeds/fails and the job succeeds/fails/keeps running.
 *
 * <ul>
 *   <li>Savepoint succeeds, job succeeds - The happy path we like to see.
 *   <li>Savepoint fails, job fails - The generic failure case. Something happened during
 *       checkpointing on the TM side that also failed the task; fail the savepoint operation and
 *       restart the job.
 *   <li>Savepoint succeeds, job fails - Some issue occurred in notifyCheckpointComplete or during
 *       the job shutdown. Fail the savepoint operation and job, but inform the user about the
 *       created savepoint.
 *   <li>Savepoint fails, job keeps running - The savepoint failed due to an error on the JM side,
 *       before we ever triggered anything on the TM side. Fail the savepoint operation, but keep
 *       the job running.
 * </ul>
 *
 * <p>This is further complicated by this information being transmitted via 2 separate RPCs from
 * TM->JM, with the {@code savepointFuture} not being completed in the main thread, introducing
 * ordering/lateness issues. Be careful to not liberally use {@link Context#runIfState(State,
 * Runnable, Duration)} because it can result in a message being lost if multiple operations are
 * queued and the first initiates a state transition.
 */
class StopWithSavepoint extends StateWithExecutionGraph {

    private final Context context;
    /**
     * The result future of this operation, containing the path to the savepoint. This is the future
     * that other components (e.g., the REST API) wait for.
     *
     * <p>Must only be completed successfully if the savepoint was created and the job has FINISHED.
     */
    private final CompletableFuture<String> operationFuture;

    private final CheckpointScheduling checkpointScheduling;

    @Nullable private Throwable operationFailureCause;
    private boolean hasPendingStateTransition = false;

    // be careful when applying operations on this future that can trigger state transitions,
    // as several other methods do the same and we mustn't trigger multiple transitions!
    private final CompletableFuture<String> internalSavepointFuture = new CompletableFuture<>();

    StopWithSavepoint(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            CheckpointScheduling checkpointScheduling,
            Logger logger,
            ClassLoader userCodeClassLoader,
            CompletableFuture<String> savepointFuture,
            List<ExceptionHistoryEntry> failureCollection) {
        super(
                context,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                logger,
                userCodeClassLoader,
                failureCollection);
        this.context = context;
        this.checkpointScheduling = checkpointScheduling;
        this.operationFuture = new CompletableFuture<>();

        FutureUtils.assertNoException(
                internalSavepointFuture.exceptionally(
                        cause -> {
                            onSavepointFailure(cause);
                            return null;
                        }));

        // this is a roundabout way of splicing the completion of the future into the main thread.
        // allows other methods to apply synchronous operations on the future without having to
        // worry about the main thread.
        savepointFuture.handle(
                (savepoint, error) -> {
                    context.runIfState(
                            this,
                            () -> {
                                if (error != null) {
                                    internalSavepointFuture.completeExceptionally(error);
                                } else {
                                    internalSavepointFuture.complete(savepoint);
                                }
                            },
                            Duration.ZERO);
                    return null;
                });
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        this.operationFuture.completeExceptionally(
                new FlinkException(
                        "Stop with savepoint operation could not be completed.",
                        operationFailureCause));

        super.onLeave(newState);
    }

    /**
     * Cancel the job and fail the savepoint operation future.
     *
     * <p>We don't wait for the {@link #internalSavepointFuture} here so that users can still cancel
     * a job if the savepoint takes too long (or gets stuck).
     *
     * <p>Since we don't actually cancel the savepoint (for which there is no API to do so), there
     * is a small risk that the job is cancelled at the very moment that the savepoint completes,
     * causing it to not be reported to the user. See FLINK-28127.
     */
    @Override
    public void cancel() {
        operationFailureCause = new FlinkException("The job was cancelled.");
        context.goToCanceling(
                getExecutionGraph(),
                getExecutionGraphHandler(),
                getOperatorCoordinatorHandler(),
                getFailures());
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RUNNING;
    }

    /**
     * Restarts the checkpoint scheduler and, if only the savepoint failed without a task failure /
     * job termination, transitions back to {@link Executing}.
     *
     * <p>This method must assume that {@link #onFailure}/{@link #onGloballyTerminalState} MAY
     * already be waiting for the savepoint operation to complete, itching to trigger a state
     * transition (hence the {@link #hasPendingStateTransition} check).
     *
     * <p>If the above is violated (e.g., by always transitioning into another state), then
     * depending on other implementation details something very bad will happen, like the scheduler
     * crashing the JVM because it attempted multiple state transitions OR effectively dropping the
     * onFailure/onGloballyTerminalState call OR we trigger state transitions while we are already
     * in another state.
     *
     * <p>For maintainability reasons this method should not mutate any state that affects state
     * transitions in other methods.
     */
    private void onSavepointFailure(Throwable cause) {
        // revert side-effect of Executing#stopWithSavepoint
        checkpointScheduling.startCheckpointScheduler();
        // a task failed concurrently; defer the error handling to onFailure()
        // otherwise we will attempt 2 state transitions, which is forbidden
        if (!hasPendingStateTransition) {
            operationFailureCause = cause;
            context.goToExecuting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    getFailures());
        }
    }

    @Override
    void onFailure(Throwable cause) {
        if (hasPendingStateTransition) {
            // the error handling remains the same independent of how many tasks have failed
            // we don't want to initiate the same state transition multiple times, so we exit early
            // this could also be achieved via Context#runIfState, but that'd spam the logs
            return;
        }
        hasPendingStateTransition = true;

        FutureUtils.assertNoException(
                internalSavepointFuture.handle(
                        (savepoint, savepointError) -> {
                            // if savepointError is null then the savepoint has been created
                            // successfully, but the job failed while committing side effects,
                            // so we enrich the exception for the user
                            final Throwable ex =
                                    savepointError != null
                                            ? cause
                                            : new StopWithSavepointStoppingException(
                                                    savepoint, getJobId(), cause);
                            operationFailureCause = ex;
                            FailureResultUtil.restartOrFail(
                                    context.howToHandleFailure(ex), context, this);
                            return null;
                        }));
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        if (globallyTerminalState == JobStatus.FINISHED) {
            // do not set this in other cases
            // handleGlobalFailure circles back to onFailure()
            hasPendingStateTransition = true;
            FutureUtils.assertNoException(
                    internalSavepointFuture.handle(
                            (savepoint, error) -> {
                                Preconditions.checkState(
                                        error == null,
                                        "A savepoint should never fail after a job has been terminated via stop-with-savepoint.");
                                completeOperationAndGoToFinished(savepoint);
                                return null;
                            }));
        } else {
            context.handleGlobalFailure(
                    new FlinkException(
                            "Job did not reach the FINISHED state while performing stop-with-savepoint."));
        }
    }

    private void completeOperationAndGoToFinished(String savepoint) {
        operationFuture.complete(savepoint);
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    CompletableFuture<String> getOperationFuture() {
        return operationFuture;
    }

    interface Context
            extends StateWithExecutionGraph.Context,
                    StateTransitions.ToCancelling,
                    StateTransitions.ToExecuting,
                    StateTransitions.ToFailing,
                    StateTransitions.ToRestarting,
                    GlobalFailureHandler {

        /**
         * Asks how to handle the failure.
         *
         * @param failure failure describing the failure cause
         * @return {@link FailureResult} which describes how to handle the failure
         */
        FailureResult howToHandleFailure(Throwable failure);

        /**
         * Runs the given action after the specified delay if the state is the expected state at
         * this time.
         *
         * @param expectedState expectedState describes the required state to run the action after
         *     the delay
         * @param action action to run if the state equals the expected state
         * @param delay delay after which the action should be executed
         * @return a ScheduledFuture representing pending completion of the task
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<StopWithSavepoint> {
        private final Context context;

        private final ExecutionGraph executionGraph;

        private final ExecutionGraphHandler executionGraphHandler;

        private final OperatorCoordinatorHandler operatorCoordinatorHandler;

        private final CheckpointScheduling checkpointScheduling;

        private final Logger logger;

        private final ClassLoader userCodeClassLoader;

        private final CompletableFuture<String> savepointFuture;

        private final List<ExceptionHistoryEntry> failureCollection;

        Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                Logger logger,
                ClassLoader userCodeClassLoader,
                CompletableFuture<String> savepointFuture,
                List<ExceptionHistoryEntry> failureCollection) {
            this.context = context;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.checkpointScheduling = checkpointScheduling;
            this.logger = logger;
            this.userCodeClassLoader = userCodeClassLoader;
            this.savepointFuture = savepointFuture;
            this.failureCollection = failureCollection;
        }

        @Override
        public Class<StopWithSavepoint> getStateClass() {
            return StopWithSavepoint.class;
        }

        @Override
        public StopWithSavepoint getState() {
            return new StopWithSavepoint(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    checkpointScheduling,
                    logger,
                    userCodeClassLoader,
                    savepointFuture,
                    failureCollection);
        }
    }
}
