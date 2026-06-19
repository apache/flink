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
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.SpeculativeExecutionVertex;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.scheduler.slowtaskdetector.ExecutionTimeBasedSlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetectorListener;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link SpeculativeExecutionHandler}. */
public class DefaultSpeculativeExecutionHandler
        implements SpeculativeExecutionHandler, SlowTaskDetectorListener {

    private final int maxConcurrentExecutions;

    private final Duration blockSlowNodeDuration;

    private final BlocklistOperations blocklistOperations;

    private final SlowTaskDetector slowTaskDetector;

    private long numSlowExecutionVertices;

    private final Counter numEffectiveSpeculativeExecutionsCounter;

    private final Function<ExecutionVertexID, ExecutionVertex> executionVertexRetriever;

    private final Supplier<Map<ExecutionAttemptID, Execution>> registerExecutionsSupplier;

    private final BiConsumer<List<Execution>, Collection<ExecutionVertexID>>
            allocateSlotsAndDeployFunction;

    private final Logger log;

    public DefaultSpeculativeExecutionHandler(
            Configuration jobMasterConfiguration,
            BlocklistOperations blocklistOperations,
            Function<ExecutionVertexID, ExecutionVertex> executionVertexRetriever,
            Supplier<Map<ExecutionAttemptID, Execution>> registerExecutionsSupplier,
            BiConsumer<List<Execution>, Collection<ExecutionVertexID>>
                    allocateSlotsAndDeployFunction,
            Logger log) {
        this.maxConcurrentExecutions =
                jobMasterConfiguration.get(
                        BatchExecutionOptions.SPECULATIVE_MAX_CONCURRENT_EXECUTIONS);
        this.blockSlowNodeDuration =
                jobMasterConfiguration.get(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION);
        checkArgument(
                !blockSlowNodeDuration.isNegative(),
                "The blocking duration should not be negative.");

        this.blocklistOperations = checkNotNull(blocklistOperations);
        this.slowTaskDetector = new ExecutionTimeBasedSlowTaskDetector(jobMasterConfiguration);
        this.numEffectiveSpeculativeExecutionsCounter = new SimpleCounter();
        this.executionVertexRetriever = checkNotNull(executionVertexRetriever);
        this.registerExecutionsSupplier = checkNotNull(registerExecutionsSupplier);
        this.allocateSlotsAndDeployFunction = checkNotNull(allocateSlotsAndDeployFunction);
        this.log = checkNotNull(log);
    }

    @Override
    public void init(
            ExecutionGraph executionGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            MetricGroup metricGroup) {
        metricGroup.gauge(MetricNames.NUM_SLOW_EXECUTION_VERTICES, () -> numSlowExecutionVertices);
        metricGroup.counter(
                MetricNames.NUM_EFFECTIVE_SPECULATIVE_EXECUTIONS,
                numEffectiveSpeculativeExecutionsCounter);

        slowTaskDetector.start(executionGraph, this, mainThreadExecutor);
    }

    @Override
    public void stopSlowTaskDetector() {
        slowTaskDetector.stop();
    }

    @Override
    public void notifyTaskFinished(
            final Execution execution,
            Function<ExecutionVertexID, CompletableFuture<?>> cancelPendingExecutionsFunction) {
        if (!isOriginalAttempt(execution)) {
            numEffectiveSpeculativeExecutionsCounter.inc();
        }

        // cancel all un-terminated executions because the execution vertex has finished
        FutureUtils.assertNoException(
                cancelPendingExecutionsFunction.apply(execution.getVertex().getID()));
    }

    private boolean isOriginalAttempt(final Execution execution) {
        return getExecutionVertex(execution.getVertex().getID())
                .isOriginalAttempt(execution.getAttemptNumber());
    }

    @Override
    public void notifyTaskFailed(final Execution execution) {
        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(execution.getVertex().getID());

        // when an execution fails, remove it from current executions to make room for future
        // speculative executions
        executionVertex.archiveFailedExecution(execution.getAttemptId());
    }

    @Override
    public boolean handleTaskFailure(
            final Execution failedExecution,
            @Nullable final Throwable error,
            BiConsumer<Execution, Throwable> handleLocalExecutionAttemptFailure) {
        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(failedExecution.getVertex().getID());

        // if the execution vertex is not possible finish or a PartitionException occurred,
        // trigger an execution vertex failover to recover
        if (!isExecutionVertexPossibleToFinish(executionVertex)
                || ExceptionUtils.findThrowable(error, PartitionException.class).isPresent()) {
            return false;
        } else {
            // this is just a local failure and the execution vertex will not be fully restarted
            handleLocalExecutionAttemptFailure.accept(failedExecution, error);
            return true;
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

        allocateSlotsAndDeployFunction.accept(newSpeculativeExecutions, verticesToDeploy);
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
                .map(id -> registerExecutionsSupplier.get().get(id))
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

    private SpeculativeExecutionVertex getExecutionVertex(
            final ExecutionVertexID executionVertexId) {
        return (SpeculativeExecutionVertex) executionVertexRetriever.apply(executionVertexId);
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

    @Override
    public void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
        final Execution execution = executionVertex.getCurrentExecutionAttempt();
        if (execution.getState() == ExecutionState.FINISHED && !isOriginalAttempt(execution)) {
            numEffectiveSpeculativeExecutionsCounter.dec();
        }
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
