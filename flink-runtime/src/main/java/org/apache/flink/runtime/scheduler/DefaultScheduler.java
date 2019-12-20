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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

	private final Logger log;

	private final ClassLoader userCodeLoader;

	private final ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutor delayExecutor;

	private final SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexOperations executionVertexOperations;

	public DefaultScheduler(
		final Logger log,
		final JobGraph jobGraph,
		final BackPressureStatsTracker backPressureStatsTracker,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final SlotProvider slotProvider,
		final ScheduledExecutorService futureExecutor,
		final ScheduledExecutor delayExecutor,
		final ClassLoader userCodeLoader,
		final CheckpointRecoveryFactory checkpointRecoveryFactory,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final JobManagerJobMetricGroup jobManagerJobMetricGroup,
		final Time slotRequestTimeout,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker,
		final SchedulingStrategyFactory schedulingStrategyFactory,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
		final ExecutionVertexOperations executionVertexOperations,
		final ExecutionVertexVersioner executionVertexVersioner,
		final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			slotProvider,
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
			blobWriter,
			jobManagerJobMetricGroup,
			slotRequestTimeout,
			shuffleMaster,
			partitionTracker,
			executionVertexVersioner,
			false);

		this.log = log;

		this.delayExecutor = checkNotNull(delayExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.executionVertexOperations = checkNotNull(executionVertexOperations);

		final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(
			getFailoverTopology(),
			getResultPartitionAvailabilityChecker());
		log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

		this.executionFailureHandler = new ExecutionFailureHandler(
			getFailoverTopology(),
			failoverStrategy,
			restartBackoffTimeStrategy);
		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());
		this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory).createInstance(getInputsLocationsRetriever());
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	protected long getNumberOfRestarts() {
		return executionFailureHandler.getNumberOfRestarts();
	}

	@Override
	protected void startSchedulingInternal() {
		log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());
		prepareExecutionGraphForNgScheduling();
		schedulingStrategy.startScheduling();
	}

	@Override
	protected void updateTaskExecutionStateInternal(final ExecutionVertexID executionVertexId, final TaskExecutionState taskExecutionState) {
		schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
		maybeHandleTaskFailure(taskExecutionState, executionVertexId);
	}

	private void maybeHandleTaskFailure(final TaskExecutionState taskExecutionState, final ExecutionVertexID executionVertexId) {
		if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
			final Throwable error = taskExecutionState.getError(userCodeLoader);
			handleTaskFailure(executionVertexId, error);
		}
	}

	private void handleTaskFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId, error);
		maybeRestartTasks(failureHandlingResult);
	}

	@Override
	public void handleGlobalFailure(final Throwable error) {
		setGlobalFailureCause(error);

		log.info("Trying to recover from a global failure.", error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			restartTasksWithDelay(failureHandlingResult);
		} else {
			failJob(failureHandlingResult.getError());
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

		final Set<ExecutionVertexVersion> executionVertexVersions =
			new HashSet<>(executionVertexVersioner.recordVertexModifications(verticesToRestart).values());

		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		delayExecutor.schedule(
			() -> FutureUtils.assertNoException(
				cancelFuture.thenRunAsync(restartTasks(executionVertexVersions), getMainThreadExecutor())),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private Runnable restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions) {
		return () -> {
			final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

			resetForNewExecutions(verticesToRestart);

			try {
				restoreState(verticesToRestart);
			} catch (Throwable t) {
				handleGlobalFailure(t);
				return;
			}

			schedulingStrategy.restartTasks(verticesToRestart);
		};
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		executionSlotAllocator.cancel(executionVertexId);
		return executionVertexOperations.cancel(getExecutionVertex(executionVertexId));
	}

	@Override
	protected void scheduleOrUpdateConsumersInternal(final ExecutionVertexID producerVertexId, final ResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(producerVertexId, partitionId);
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		validateDeploymentOptions(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(verticesToDeploy);

		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		if (isDeployIndividually()) {
			deployIndividually(deploymentHandles);
		} else {
			waitForAllSlotsAndDeploy(deploymentHandles);
		}
	}

	private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		deploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.forEach(v -> checkState(
				v.getExecutionState() == ExecutionState.CREATED,
				"expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.map(ExecutionVertexSchedulingRequirementsMapper::from)
			.collect(Collectors.toList()));
	}

	private static List<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	/**
	 * <b>HACK:</b> See <a href="https://issues.apache.org/jira/browse/FLINK-14162">FLINK-14162</a>
	 * for details.
	 */
	private boolean isDeployIndividually() {
		return schedulingStrategy instanceof LazyFromSourcesSchedulingStrategy;
	}

	private void deployIndividually(final List<DeploymentHandle> deploymentHandles) {
		for (final DeploymentHandle deploymentHandle : deploymentHandles) {
			FutureUtils.assertNoException(
				deploymentHandle
					.getSlotExecutionVertexAssignment()
					.getLogicalSlotFuture()
					.handle(assignResourceOrHandleError(deploymentHandle))
					.handle(deployOrHandleError(deploymentHandle)));
		}
	}

	private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
		FutureUtils.assertNoException(
			assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
	}

	private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle));
			slotAssignedFutures.add(slotAssigned);
		}
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
		return (ignored, throwable) -> {
			propagateIfNonNull(throwable);
			for (final DeploymentHandle deploymentHandle : deploymentHandles) {
				final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
				final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
				checkState(slotAssigned.isDone());

				FutureUtils.assertNoException(
					slotAssigned.handle(deployOrHandleError(deploymentHandle)));
			}
			return null;
		};
	}

	private static void propagateIfNonNull(final Throwable throwable) {
		if (throwable != null) {
			throw new CompletionException(throwable);
		}
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

		return (logicalSlot, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to assign slot to execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				releaseSlotIfPresent(logicalSlot);
				return null;
			}

			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption().sendScheduleOrUpdateConsumerMessage();
				executionVertex
					.getCurrentExecutionAttempt()
					.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
				executionVertex.tryAssignResource(logicalSlot);
			} else {
				handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
			}
			return null;
		};
	}

	private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
		if (logicalSlot != null) {
			logicalSlot.releaseSlot(null);
		}
	}

	private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
	}

	private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
		final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
		if (strippedThrowable instanceof TimeoutException) {
			return new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources.", failure);
		} else {
			return failure;
		}
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

		return (ignored, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to deploy execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				return null;
			}

			if (throwable == null) {
				deployTaskSafe(executionVertexId);
			} else {
				handleTaskDeploymentFailure(executionVertexId, throwable);
			}
			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
		try {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			executionVertexOperations.deploy(executionVertex);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}
}
