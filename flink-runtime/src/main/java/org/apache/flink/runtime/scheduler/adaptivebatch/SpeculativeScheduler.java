/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions.HybridPartitionDataConsumeConstraint;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.SpeculativeExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionOperations;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.slowtaskdetector.ExecutionTimeBasedSlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetectorListener;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The speculative scheduler. */
public class SpeculativeScheduler extends AdaptiveBatchScheduler
        implements SlowTaskDetectorListener {

    private final int maxConcurrentExecutions;

    private final Duration blockSlowNodeDuration;

    private final BlocklistOperations blocklistOperations;

    private final SlowTaskDetector slowTaskDetector;

    private long numSlowExecutionVertices;

    private final Counter numEffectiveSpeculativeExecutionsCounter;

    public SpeculativeScheduler(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction,
            final ScheduledExecutor delayExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final Collection<FailureEnricher> failureEnrichers,
            final ExecutionGraphFactory executionGraphFactory,
            final ShuffleMaster<?> shuffleMaster,
            final Time rpcTimeout,
            final VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider,
            final int defaultMaxParallelism,
            final BlocklistOperations blocklistOperations,
            final HybridPartitionDataConsumeConstraint hybridPartitionDataConsumeConstraint,
            final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId)
            throws Exception {

        super(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                startUpAction,
                delayExecutor,
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulingStrategyFactory,
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                failureEnrichers,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout,
                vertexParallelismAndInputInfosDecider,
                defaultMaxParallelism,
                hybridPartitionDataConsumeConstraint,
                forwardGroupsByJobVertexId);

        this.maxConcurrentExecutions =
                jobMasterConfiguration.getInteger(
                        BatchExecutionOptions.SPECULATIVE_MAX_CONCURRENT_EXECUTIONS);

        this.blockSlowNodeDuration =
                jobMasterConfiguration.get(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION);
        checkArgument(
                !blockSlowNodeDuration.isNegative(),
                "The blocking duration should not be negative.");

        this.blocklistOperations = checkNotNull(blocklistOperations);

        this.slowTaskDetector = new ExecutionTimeBasedSlowTaskDetector(jobMasterConfiguration);

        this.numEffectiveSpeculativeExecutionsCounter = new SimpleCounter();
    }

    @Override
    protected void startSchedulingInternal() {
        registerMetrics(jobManagerJobMetricGroup);

        super.startSchedulingInternal();
        slowTaskDetector.start(getExecutionGraph(), this, getMainThreadExecutor());
    }

    private void registerMetrics(MetricGroup metricGroup) {
        metricGroup.gauge(MetricNames.NUM_SLOW_EXECUTION_VERTICES, () -> numSlowExecutionVertices);
        metricGroup.counter(
                MetricNames.NUM_EFFECTIVE_SPECULATIVE_EXECUTIONS,
                numEffectiveSpeculativeExecutionsCounter);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        slowTaskDetector.stop();
        return super.closeAsync();
    }

    @Override
    public SpeculativeExecutionVertex getExecutionVertex(ExecutionVertexID executionVertexId) {
        return (SpeculativeExecutionVertex) super.getExecutionVertex(executionVertexId);
    }

    @Override
    protected void onTaskFinished(final Execution execution, final IOMetrics ioMetrics) {
        if (!isOriginalAttempt(execution)) {
            numEffectiveSpeculativeExecutionsCounter.inc();
        }

        // cancel all un-terminated executions because the execution vertex has finished
        FutureUtils.assertNoException(cancelPendingExecutions(execution.getVertex().getID()));

        super.onTaskFinished(execution, ioMetrics);
    }

    private static boolean isOriginalAttempt(final Execution execution) {
        return ((SpeculativeExecutionVertex) execution.getVertex())
                .isOriginalAttempt(execution.getAttemptNumber());
    }

    private CompletableFuture<?> cancelPendingExecutions(
            final ExecutionVertexID executionVertexId) {
        final List<Execution> pendingExecutions =
                getExecutionVertex(executionVertexId).getCurrentExecutions().stream()
                        .filter(
                                e ->
                                        !e.getState().isTerminal()
                                                && e.getState() != ExecutionState.CANCELING)
                        .collect(Collectors.toList());
        if (pendingExecutions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        log.info(
                "Canceling {} un-finished executions of {} because one of its executions has finished.",
                pendingExecutions.size(),
                executionVertexId);

        final CompletableFuture<?> future =
                FutureUtils.combineAll(
                        pendingExecutions.stream()
                                .map(this::cancelExecution)
                                .collect(Collectors.toList()));
        cancelAllPendingSlotRequestsForVertex(executionVertexId);
        return future;
    }

    @Override
    protected void onTaskFailed(final Execution execution) {
        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(execution.getVertex().getID());

        // when an execution fails, remove it from current executions to make room for future
        // speculative executions
        executionVertex.archiveFailedExecution(execution.getAttemptId());

        super.onTaskFailed(execution);
    }

    @Override
    protected void handleTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {

        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(failedExecution.getVertex().getID());

        // if the execution vertex is not possible finish or a PartitionException occurred, trigger
        // an execution vertex failover to recover
        if (!isExecutionVertexPossibleToFinish(executionVertex)
                || ExceptionUtils.findThrowable(error, PartitionException.class).isPresent()) {
            super.handleTaskFailure(failedExecution, error);
        } else {
            // this is just a local failure and the execution vertex will not be fully restarted
            handleLocalExecutionAttemptFailure(failedExecution, error);
        }
    }

    private void handleLocalExecutionAttemptFailure(
            final Execution failedExecution, @Nullable final Throwable error) {
        executionSlotAllocator.cancel(failedExecution.getAttemptId());

        final FailureHandlingResult failureHandlingResult =
                recordTaskFailure(failedExecution, error);
        if (failureHandlingResult.canRestart()) {
            archiveFromFailureHandlingResult(
                    createFailureHandlingResultSnapshot(failureHandlingResult));
        } else {
            failJob(
                    error,
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels());
        }
    }

    private static boolean isExecutionVertexPossibleToFinish(
            final SpeculativeExecutionVertex executionVertex) {
        boolean anyExecutionPossibleToFinish = false;
        for (Execution execution : executionVertex.getCurrentExecutions()) {
            // if any execution has finished, no execution of the same execution vertex should fail
            // after that
            checkState(execution.getState() != ExecutionState.FINISHED);

            if (execution.getState() == ExecutionState.CREATED
                    || execution.getState() == ExecutionState.SCHEDULED
                    || execution.getState() == ExecutionState.DEPLOYING
                    || execution.getState() == ExecutionState.INITIALIZING
                    || execution.getState() == ExecutionState.RUNNING) {
                anyExecutionPossibleToFinish = true;
            }
        }
        return anyExecutionPossibleToFinish;
    }

    @Override
    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
        final Execution execution = executionVertex.getCurrentExecutionAttempt();
        if (execution.getState() == ExecutionState.FINISHED && !isOriginalAttempt(execution)) {
            numEffectiveSpeculativeExecutionsCounter.dec();
        }

        super.resetForNewExecution(executionVertexId);
    }

    @Override
    public void notifySlowTasks(Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks) {
        final long currentTimestamp = System.currentTimeMillis();
        numSlowExecutionVertices = slowTasks.size();

        // add slow nodes to blocklist before scheduling new speculative executions
        blockSlowNodes(slowTasks, currentTimestamp);

        final List<Execution> newSpeculativeExecutions = new ArrayList<>();
        final Set<ExecutionVertexID> verticesToDeploy = new HashSet<>();
        for (ExecutionVertexID executionVertexId : slowTasks.keySet()) {
            final SpeculativeExecutionVertex executionVertex =
                    getExecutionVertex(executionVertexId);

            if (!executionVertex.isSupportsConcurrentExecutionAttempts()) {
                continue;
            }

            final int currentConcurrentExecutions = executionVertex.getCurrentExecutions().size();
            final int newSpeculativeExecutionsToDeploy =
                    maxConcurrentExecutions - currentConcurrentExecutions;
            if (newSpeculativeExecutionsToDeploy > 0) {
                log.info(
                        "{} ({}) is detected as a slow vertex, create and deploy {} new speculative executions for it.",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        executionVertex.getID(),
                        newSpeculativeExecutionsToDeploy);

                final Collection<Execution> attempts =
                        IntStream.range(0, newSpeculativeExecutionsToDeploy)
                                .mapToObj(
                                        i ->
                                                executionVertex.createNewSpeculativeExecution(
                                                        currentTimestamp))
                                .collect(Collectors.toList());

                setupSubtaskGatewayForAttempts(executionVertex, attempts);
                verticesToDeploy.add(executionVertexId);
                newSpeculativeExecutions.addAll(attempts);
            }
        }

        executionDeployer.allocateSlotsAndDeploy(
                newSpeculativeExecutions,
                executionVertexVersioner.getExecutionVertexVersions(verticesToDeploy));
    }

    private void blockSlowNodes(
            Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks,
            long currentTimestamp) {
        if (!blockSlowNodeDuration.isZero()) {
            final long blockedEndTimestamp = currentTimestamp + blockSlowNodeDuration.toMillis();
            final Collection<BlockedNode> nodesToBlock =
                    getSlowNodeIds(slowTasks).stream()
                            .map(
                                    nodeId ->
                                            new BlockedNode(
                                                    nodeId,
                                                    "Node is detected to be slow.",
                                                    blockedEndTimestamp))
                            .collect(Collectors.toList());
            blocklistOperations.addNewBlockedNodes(nodesToBlock);
        }
    }

    private Set<String> getSlowNodeIds(
            Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks) {
        final Set<ExecutionAttemptID> slowExecutions =
                slowTasks.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

        return slowExecutions.stream()
                .map(id -> getExecutionGraph().getRegisteredExecutions().get(id))
                .map(
                        e -> {
                            checkNotNull(
                                    e.getAssignedResource(),
                                    "The reported slow node have not been assigned a slot. "
                                            + "This is unexpected and indicates that there is "
                                            + "something wrong with the slow task detector.");
                            return e.getAssignedResourceLocation();
                        })
                .map(TaskManagerLocation::getNodeId)
                .collect(Collectors.toSet());
    }

    private void setupSubtaskGatewayForAttempts(
            final SpeculativeExecutionVertex executionVertex,
            final Collection<Execution> attempts) {

        final Set<Integer> attemptNumbers =
                attempts.stream().map(Execution::getAttemptNumber).collect(Collectors.toSet());

        executionVertex
                .getJobVertex()
                .getOperatorCoordinators()
                .forEach(
                        operatorCoordinator ->
                                operatorCoordinator.setupSubtaskGatewayForAttempts(
                                        executionVertex.getParallelSubtaskIndex(), attemptNumbers));
    }

    @VisibleForTesting
    long getNumSlowExecutionVertices() {
        return numSlowExecutionVertices;
    }

    @VisibleForTesting
    long getNumEffectiveSpeculativeExecutions() {
        return numEffectiveSpeculativeExecutionsCounter.getCount();
    }
}
