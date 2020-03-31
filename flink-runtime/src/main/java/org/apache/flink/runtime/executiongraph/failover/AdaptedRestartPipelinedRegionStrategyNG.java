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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.SchedulingUtils;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersion;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This failover strategy uses flip1.RestartPipelinedRegionStrategy to make task failover decisions.
 */
public class AdaptedRestartPipelinedRegionStrategyNG extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(AdaptedRestartPipelinedRegionStrategyNG.class);

	/** The execution graph on which this FailoverStrategy works. */
	private final ExecutionGraph executionGraph;

	/** The versioner helps to maintain execution vertex versions. */
	private final ExecutionVertexVersioner executionVertexVersioner;

	/** The underlying new generation region failover strategy. */
	private RestartPipelinedRegionStrategy restartPipelinedRegionStrategy;

	public AdaptedRestartPipelinedRegionStrategyNG(final ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executionVertexVersioner = new ExecutionVertexVersioner();
	}

	@Override
	public void onTaskFailure(final Execution taskExecution, final Throwable cause) {
		if (!executionGraph.getRestartStrategy().canRestart()) {
			// delegate the failure to a global fail that will check the restart strategy and not restart
			LOG.info("Fail to pass the restart strategy validation in region failover. Fallback to fail global.");
			failGlobal(cause);
			return;
		}

		if (!isLocalFailoverValid(executionGraph.getGlobalModVersion())) {
			LOG.info("Skip current region failover as a global failover is ongoing.");
			return;
		}

		final ExecutionVertexID vertexID = getExecutionVertexID(taskExecution.getVertex());

		final Set<ExecutionVertexID> tasksToRestart = restartPipelinedRegionStrategy.getTasksNeedingRestart(vertexID, cause);
		restartTasks(tasksToRestart);
	}

	@VisibleForTesting
	protected void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
		final long globalModVersion = executionGraph.getGlobalModVersion();
		final Set<ExecutionVertexVersion> vertexVersions = new HashSet<>(
			executionVertexVersioner.recordVertexModifications(verticesToRestart).values());

		executionGraph.incrementRestarts();

		FutureUtils.assertNoException(
			cancelTasks(verticesToRestart)
				.thenComposeAsync(resetAndRescheduleTasks(globalModVersion, vertexVersions), executionGraph.getJobMasterMainThreadExecutor())
				.handle(failGlobalOnError()));
	}

	private Function<Object, CompletableFuture<Void>> resetAndRescheduleTasks(final long globalModVersion, final Set<ExecutionVertexVersion> vertexVersions) {
		return (ignored) -> {
			final RestartStrategy restartStrategy = executionGraph.getRestartStrategy();
			return restartStrategy.restart(
				createResetAndRescheduleTasksCallback(globalModVersion, vertexVersions),
				executionGraph.getJobMasterMainThreadExecutor()
			);
		};
	}

	private RestartCallback createResetAndRescheduleTasksCallback(final long globalModVersion, final Set<ExecutionVertexVersion> vertexVersions) {
		return () -> {
			if (!isLocalFailoverValid(globalModVersion)) {
				LOG.info("Skip current region failover as a global failover is ongoing.");
				return;
			}

			// found out vertices which are still valid to restart.
			// some vertices involved in this failover may be modified if another region
			// failover happens during the cancellation stage of this failover.
			// Will ignore the modified vertices as the other failover will deal with them.
			final Set<ExecutionVertex> unmodifiedVertices = executionVertexVersioner
				.getUnmodifiedExecutionVertices(vertexVersions)
				.stream()
				.map(this::getExecutionVertex)
				.collect(Collectors.toSet());

			try {
				LOG.info("Finally restart {} tasks to recover from task failure.", unmodifiedVertices.size());

				// reset tasks to CREATED state and reload state
				resetTasks(unmodifiedVertices, globalModVersion);

				// re-schedule tasks
				rescheduleTasks(unmodifiedVertices, globalModVersion);
			} catch (GlobalModVersionMismatch e) {
				throw new IllegalStateException(
					"Bug: ExecutionGraph was concurrently modified outside of main thread", e);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		};
	}

	private BiFunction<Object, Throwable, Object> failGlobalOnError() {
		return (Object ignored, Throwable t) -> {
			if (t != null) {
				LOG.info("Unexpected error happens in region failover. Fail globally.", t);
				failGlobal(t);
			}
			return null;
		};
	}

	@VisibleForTesting
	protected CompletableFuture<?> cancelTasks(final Set<ExecutionVertexID> vertices) {
		final List<CompletableFuture<?>> cancelFutures = vertices.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private void resetTasks(final Set<ExecutionVertex> vertices, final long globalModVersion) throws Exception {
		final Set<CoLocationGroup> colGroups = new HashSet<>();
		final long restartTimestamp = System.currentTimeMillis();

		for (ExecutionVertex ev : vertices) {
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
				getInvolvedExecutionJobVertices(vertices);
			executionGraph.getCheckpointCoordinator().restoreLatestCheckpointedState(
				involvedExecutionJobVertices, false, true);
		}
	}

	private void rescheduleTasks(final Set<ExecutionVertex> vertices, final long globalModVersion) throws Exception {

		// sort vertices topologically
		// this is to reduce the possibility that downstream tasks get launched earlier,
		// which may cause lots of partition state checks in EAGER mode
		final List<ExecutionVertex> sortedVertices = sortVerticesTopologically(vertices);

		final CompletableFuture<Void> newSchedulingFuture = SchedulingUtils.schedule(
			executionGraph.getScheduleMode(),
			sortedVertices,
			executionGraph);

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

		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		for (ExecutionVertex executionVertex : executionVertices) {
			JobVertexID jobvertexId = executionVertex.getJobvertexId();
			ExecutionJobVertex jobVertex = executionVertex.getJobVertex();
			tasks.putIfAbsent(jobvertexId, jobVertex);
		}
		return tasks;
	}

	private void failGlobal(final Throwable cause) {
		executionGraph.failGlobal(cause);
	}

	private ExecutionVertex getExecutionVertex(final ExecutionVertexID vertexID) {
		return executionGraph.getAllVertices()
			.get(vertexID.getJobVertexId())
			.getTaskVertices()[vertexID.getSubtaskIndex()];
	}

	private ExecutionVertexID getExecutionVertexID(final ExecutionVertex vertex) {
		return vertex.getID();
	}

	private List<ExecutionVertex> sortVerticesTopologically(final Set<ExecutionVertex> vertices) {
		// org execution vertex by jobVertexId
		final Map<JobVertexID, List<ExecutionVertex>> verticesMap = new HashMap<>();
		for (ExecutionVertex vertex : vertices) {
			verticesMap.computeIfAbsent(vertex.getJobvertexId(), id -> new ArrayList<>()).add(vertex);
		}

		// sort in jobVertex topological order
		final List<ExecutionVertex> sortedVertices = new ArrayList<>(vertices.size());
		for (ExecutionJobVertex jobVertex : executionGraph.getVerticesTopologically()) {
			sortedVertices.addAll(verticesMap.getOrDefault(jobVertex.getJobVertexId(), Collections.emptyList()));
		}
		return sortedVertices;
	}

	@Override
	public void notifyNewVertices(final List<ExecutionJobVertex> newJobVerticesTopological) {
		// build the underlying new generation failover strategy when the executionGraph vertices are all added,
		// otherwise the failover topology will not be correctly built.
		// currently it's safe to add it here, as this method is invoked only once in production code.
		checkState(restartPipelinedRegionStrategy == null, "notifyNewVertices() must be called only once");
		this.restartPipelinedRegionStrategy = new RestartPipelinedRegionStrategy(
			executionGraph.getFailoverTopology(),
			executionGraph.getResultPartitionAvailabilityChecker());
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
