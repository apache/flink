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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.failover.adapter.DefaultFailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersion;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This failover strategy uses flip1.RestartPipelinedRegionStrategy to make task failover decisions.
 */
public class AdaptedRestartPipelinedRegionStrategyNG extends FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(AdaptedRestartPipelinedRegionStrategyNG.class);

	/** The execution graph on which this FailoverStrategy works. */
	private final ExecutionGraph executionGraph;

	/** The underlying new generation region failover strategy. */
	private RestartPipelinedRegionStrategy restartPipelinedRegionStrategy;

	/** The versioner helps to maintain execution vertex versions. */
	private final ExecutionVertexVersioner executionVertexVersioner;

	public AdaptedRestartPipelinedRegionStrategyNG(final ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executionVertexVersioner = new ExecutionVertexVersioner();
	}

	@Override
	public void onTaskFailure(final Execution taskExecution, final Throwable cause) {
		// skip the failover if global restart strategy suppresses restarts
		if (!executionGraph.getRestartStrategy().canRestart()) {
			// delegate the failure to a global fail that will check the restart strategy and not restart
			LOG.info("Fail to pass the restart strategy validation in region failover. Fallback to fail global.");
			executionGraph.failGlobal(cause);
			return;
		}

		final ExecutionVertexID vertexID = getExecutionVertexID(taskExecution.getVertex());

		final Set<ExecutionVertexID> tasksToRestart = restartPipelinedRegionStrategy.getTasksNeedingRestart(vertexID, cause);

		// restart tasks at once
		restartTasks(tasksToRestart);
	}

	private void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
		// record current versions
		final long globalModVersion = executionGraph.getGlobalModVersion();
		final Set<ExecutionVertexVersion> vertexVersions = new HashSet<>(
			executionVertexVersioner.recordVertexModifications(verticesToRestart).values());

		// check to avoid concurrent failover and local failover issues
		if (!isLocalFailoverValid(globalModVersion)) {
			return;
		}

		// cancel tasks involved in this task failure
		cancelTasks(verticesToRestart)
			.thenAccept(
				(Object ignored) -> {
					// check to avoid concurrent failover and local failover issues
					if (!isLocalFailoverValid(globalModVersion)) {
						return;
					}

					// found out vertices which are still valid to restart.
					// some vertices involved in this failover may be modified if another region
					// failover happens during the cancellation stage of this failover.
					// Will ignore the modified vertices as the other failover will deal with them.
					final Set<ExecutionVertex> unmodifiedVertices = executionVertexVersioner
						.getUnmodifiedExecutionVertices(vertexVersions)
						.stream()
						.map(id -> getExecutionVertex(id))
						.collect(Collectors.toSet());

					try {
						LOG.info("Finally restart {} tasks to recover from task failure.", unmodifiedVertices.size());

						// reset tasks to CREATED state and reload state
						resetTasks(unmodifiedVertices, globalModVersion);

						// re-schedule tasks
						rescheduleTasks(unmodifiedVertices, globalModVersion);
					} catch (GlobalModVersionMismatch e) {
						// happens when a global recovery happens concurrently to the regional recovery
						// just stop this local failover
						return;
					} catch (Exception e) {
						throw new CompletionException(e);
					}
				})
			.whenComplete(
				(Object ignored, Throwable t) -> {
					// fail globally if any error happens
					if (t != null) {
						LOG.info("Unexpected error happens in region failover. Fail globally.", t);
						failGlobal(t);
					}
				});
	}

	private CompletableFuture<?> cancelTasks(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private void resetTasks(
		final Set<ExecutionVertex> verticesToRestart,
		final long globalModVersion) throws Exception {

		final Set<CoLocationGroup> colGroups = new HashSet<>();
		final long restartTimestamp = System.currentTimeMillis();

		for (ExecutionVertex ev : verticesToRestart) {
			CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
			if (cgroup != null && !colGroups.contains(cgroup)){
				cgroup.resetConstraints();
				colGroups.add(cgroup);
			}

			ev.resetForNewExecution(restartTimestamp, globalModVersion);
		}

		// if there is checkpointed state, reload it into the executions
		if (executionGraph.getCheckpointCoordinator() != null) {
			// abort pending checkpoints to
			// i) enable new checkpoint triggering without waiting for last checkpoint expired.
			// ii) ensure the EXACTLY_ONCE semantics if needed.
			executionGraph.getCheckpointCoordinator().abortPendingCheckpoints(
				new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

			final Map<JobVertexID, ExecutionJobVertex> involvedExecutionJobVertices =
				getInvolvedExecutionJobVertices(verticesToRestart);
			executionGraph.getCheckpointCoordinator().restoreLatestCheckpointedState(
				involvedExecutionJobVertices, false, true);
		}
	}

	private void rescheduleTasks(
		final Set<ExecutionVertex> verticesToRestart,
		final long globalModVersion) throws Exception {

		final CompletableFuture<Void> newSchedulingFuture;
		switch (executionGraph.getScheduleMode()) {

			case LAZY_FROM_SOURCES:
				newSchedulingFuture = AdaptedSchedulingUtils.scheduleLazy(verticesToRestart, executionGraph);
				break;

			case EAGER:
				newSchedulingFuture = AdaptedSchedulingUtils.scheduleEager(verticesToRestart, executionGraph);
				break;

			default:
				throw new JobException("Schedule mode is invalid.");
		}

		// if no global failover is triggered in the scheduling process,
		// register a failure handling callback to the scheduling
		if (isLocalFailoverValid(globalModVersion)) {
			newSchedulingFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					if (throwable != null) {
						final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

						if (!(strippedThrowable instanceof CancellationException)) {
							// only fail if the scheduling future was not canceled
							failGlobal(strippedThrowable);
						}
					}
				});
		}
	}

	private boolean isLocalFailoverValid(final long globalModVersion) {
		// local failover is valid only if the job is still RUNNING and
		// no global failover happens since the given globalModVersion is recorded
		return executionGraph.getState() == JobStatus.RUNNING &&
			executionGraph.getGlobalModVersion() == globalModVersion;
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		return getExecutionVertex(executionVertexId).cancel();
	}

	private Map<JobVertexID, ExecutionJobVertex> getInvolvedExecutionJobVertices(
		final Set<ExecutionVertex> executionVertices) {

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>(executionVertices.size());
		for (ExecutionVertex executionVertex : executionVertices) {
			JobVertexID jobvertexId = executionVertex.getJobvertexId();
			ExecutionJobVertex jobVertex = executionVertex.getJobVertex();
			tasks.putIfAbsent(jobvertexId, jobVertex);
		}
		return tasks;
	}

	private void failGlobal(final Throwable cause) {
		// fail globally to get the executionGraph to a consistent state
		executionGraph.failGlobal(cause);
	}

	private ExecutionVertex getExecutionVertex(final ExecutionVertexID vertexID) {
		return executionGraph.getAllVertices()
			.get(vertexID.getJobVertexId())
			.getTaskVertices()[vertexID.getSubtaskIndex()];
	}

	private ExecutionVertexID getExecutionVertexID(final ExecutionVertex vertex) {
		return new ExecutionVertexID(vertex.getJobvertexId(), vertex.getParallelSubtaskIndex());
	}

	@VisibleForTesting
	public FailoverRegion getFailoverRegion(ExecutionVertex executionVertex) {
		return restartPipelinedRegionStrategy.getFailoverRegion(getExecutionVertexID(executionVertex));
	}

	@Override
	public void notifyNewVertices(final List<ExecutionJobVertex> newJobVerticesTopological) {
		// build the underlying new generation failover strategy when the executionGraph vertices are all added,
		// otherwise the failover topology will not be correctly built.
		// currently it's safe to add it here, as this method is invoked only once in production code.
		this.restartPipelinedRegionStrategy = new RestartPipelinedRegionStrategy(
			new DefaultFailoverTopology(executionGraph));
	}

	@Override
	public String getStrategyName() {
		return "New Pipelined Region Failover";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the AdaptedRestartPipelinedRegionStrategyNG.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(final ExecutionGraph executionGraph) {
			return new AdaptedRestartPipelinedRegionStrategyNG(executionGraph);
		}
	}
}
