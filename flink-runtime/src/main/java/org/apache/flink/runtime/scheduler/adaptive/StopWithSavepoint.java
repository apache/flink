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
 */
class StopWithSavepoint extends StateWithExecutionGraph {

    private final Context context;
    private final CompletableFuture<String> operationFuture;

    private final CheckpointScheduling checkpointScheduling;

    private boolean hasFullyFinished = false;

    @Nullable private String savepoint = null;

    @Nullable private Throwable operationFailureCause;

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
                savepointFuture.handle(
                        (savepointLocation, throwable) -> {
                            // make sure we handle the future completion in the main thread and
                            // outside the constructor (where state transitions are not allowed)
                            context.runIfState(
                                    this,
                                    () -> handleSavepointCompletion(savepointLocation, throwable),
                                    Duration.ZERO);
                            return null;
                        }));
    }

    private void handleSavepointCompletion(
            @Nullable String savepoint, @Nullable Throwable throwable) {
        if (hasFullyFinished) {
            Preconditions.checkState(
                    throwable == null,
                    "A savepoint should never fail after a job has been terminated via stop-with-savepoint.");
            completeOperationAndGoToFinished(savepoint);
        } else {
            if (throwable != null) {
                operationFailureCause = throwable;
                checkpointScheduling.startCheckpointScheduler();
                context.goToExecuting(
                        getExecutionGraph(),
                        getExecutionGraphHandler(),
                        getOperatorCoordinatorHandler(),
                        getFailures());
            } else {
                this.savepoint = savepoint;
            }
        }
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        this.operationFuture.completeExceptionally(
                new FlinkException(
                        "Stop with savepoint operation could not be completed.",
                        operationFailureCause));

        super.onLeave(newState);
    }

    @Override
    public void cancel() {
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

    @Override
    void onFailure(Throwable cause) {
        operationFailureCause = cause;
        if (savepoint == null) {
            FailureResultUtil.restartOrFail(context.howToHandleFailure(cause), context, this);
        } else {
            // savepoint has been create successfully, but the job failed while committing side
            // effects
            final StopWithSavepointStoppingException ex =
                    new StopWithSavepointStoppingException(savepoint, this.getJobId(), cause);
            this.operationFuture.completeExceptionally(ex);
            FailureResultUtil.restartOrFail(context.howToHandleFailure(ex), context, this);
        }
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        if (globallyTerminalState == JobStatus.FINISHED) {
            if (savepoint == null) {
                hasFullyFinished = true;
            } else {
                completeOperationAndGoToFinished(savepoint);
            }
        } else {
            handleGlobalFailure(
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
                    StateTransitions.ToRestarting {

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
