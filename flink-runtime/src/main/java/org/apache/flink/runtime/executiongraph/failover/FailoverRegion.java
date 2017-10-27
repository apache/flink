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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * FailoverRegion manages the failover of a minimal pipeline connected sub graph.
 * It will change from CREATED to CANCELING and then to CANCELLED and at last to RUNNING,
 */
public class FailoverRegion {

	private static final AtomicReferenceFieldUpdater<FailoverRegion, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(FailoverRegion.class, JobStatus.class, "state");

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(FailoverRegion.class);

	// ------------------------------------------------------------------------

	/** a unique id for debugging */
	private final AbstractID id = new AbstractID();

	private final ExecutionGraph executionGraph;

	private final List<ExecutionVertex> connectedExecutionVertexes;

	/** The executor that executes the recovery action after all vertices are in a */
	private final Executor executor;

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.RUNNING;


	public FailoverRegion(ExecutionGraph executionGraph, Executor executor, List<ExecutionVertex> connectedExecutions) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executor = checkNotNull(executor);
		this.connectedExecutionVertexes = checkNotNull(connectedExecutions);

		LOG.debug("Created failover region {} with vertices: {}", id, connectedExecutions);
	}

	public void onExecutionFail(Execution taskExecution, Throwable cause) {
		// TODO: check if need to failover the preceding region
		if (!executionGraph.getRestartStrategy().canRestart()) {
			// delegate the failure to a global fail that will check the restart strategy and not restart
			executionGraph.failGlobal(cause);
		}
		else {
			cancel(taskExecution.getGlobalModVersion());
		}
	}

	private void allVerticesInTerminalState(long globalModVersionOfFailover) {
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.CANCELLING)) {
				if (transitionState(curStatus, JobStatus.CANCELED)) {
					reset(globalModVersionOfFailover);
					break;
				}
			}
			else {
				LOG.info("FailoverRegion {} is {} when allVerticesInTerminalState.", id, state);
				break;
			}
		}
	}

	public JobStatus getState() {
		return state;
	}

	/**
	 * get all execution vertexes contained in this region
	 */
	public List<ExecutionVertex> getAllExecutionVertexes() {
		return connectedExecutionVertexes;
	}

	// Notice the region to failover, 
	private void failover(long globalModVersionOfFailover) {
		if (!executionGraph.getRestartStrategy().canRestart()) {
			executionGraph.failGlobal(new FlinkException("RestartStrategy validate fail"));
		}
		else {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.RUNNING)) {
				cancel(globalModVersionOfFailover);
			}
			else if (curStatus.equals(JobStatus.CANCELED)) {
				reset(globalModVersionOfFailover);
			}
			else {
				LOG.info("FailoverRegion {} is {} when notified to failover.", id, state);
			}
		}
	}

	// cancel all executions in this sub graph
	private void cancel(final long globalModVersionOfFailover) {
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.RUNNING)) {
				if (transitionState(curStatus, JobStatus.CANCELLING)) {

					// we build a future that is complete once all vertices have reached a terminal state
					final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(connectedExecutionVertexes.size());

					// cancel all tasks (that still need cancelling)
					for (ExecutionVertex vertex : connectedExecutionVertexes) {
						futures.add(vertex.cancel());
					}

					final FutureUtils.ConjunctFuture<Void> allTerminal = FutureUtils.waitForAll(futures);
					allTerminal.thenAcceptAsync(
						(Void value) -> allVerticesInTerminalState(globalModVersionOfFailover),
						executor);

					break;
				}
			}
			else {
				LOG.info("FailoverRegion {} is {} when cancel.", id, state);
				break;
			}
		}
	}

	// reset all executions in this sub graph
	private void reset(long globalModVersionOfFailover) {
		try {
			// reset all connected ExecutionVertexes
			final Collection<CoLocationGroup> colGroups = new HashSet<>();
			final long restartTimestamp = System.currentTimeMillis();

			for (ExecutionVertex ev : connectedExecutionVertexes) {
				CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
				if (cgroup != null && !colGroups.contains(cgroup)){
					cgroup.resetConstraints();
					colGroups.add(cgroup);
				}

				ev.resetForNewExecution(restartTimestamp, globalModVersionOfFailover);
			}
			if (transitionState(JobStatus.CANCELED, JobStatus.CREATED)) {
				restart(globalModVersionOfFailover);
			}
			else {
				LOG.info("FailoverRegion {} switched from CANCELLING to CREATED fail, will fail this region again.", id);
				failover(globalModVersionOfFailover);
			}
		}
		catch (GlobalModVersionMismatch e) {
			// happens when a global recovery happens concurrently to the regional recovery
			// go back to a clean state
			state = JobStatus.RUNNING;
		}
		catch (Throwable e) {
			LOG.info("FailoverRegion {} reset fail, will failover again.", id);
			failover(globalModVersionOfFailover);
		}
	}

	// restart all executions in this sub graph
	private void restart(long globalModVersionOfFailover) {
		try {
			if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
				// if we have checkpointed state, reload it into the executions
				//TODO: checkpoint support restore part ExecutionVertex cp
				/**
				if (executionGraph.getCheckpointCoordinator() != null) {
					executionGraph.getCheckpointCoordinator().restoreLatestCheckpointedState(
							connectedExecutionVertexes, false, false);
				}
				*/
				//TODO, use restart strategy to schedule them.
				//restart all connected ExecutionVertexes
				for (ExecutionVertex ev : connectedExecutionVertexes) {
					try {
						ev.scheduleForExecution(
							executionGraph.getSlotProvider(),
							executionGraph.isQueuedSchedulingAllowed(),
							LocationPreferenceConstraint.ANY); // some inputs not belonging to the failover region might have failed concurrently
					}
					catch (Throwable e) {
						failover(globalModVersionOfFailover);
					}
				}
			}
			else {
				LOG.info("FailoverRegion {} switched from CREATED to RUNNING fail, will fail this region again.", id);
				failover(globalModVersionOfFailover);
			}
		} catch (Exception e) {
			LOG.info("FailoverRegion {} restart failed, failover again.", id, e);
			failover(globalModVersionOfFailover);
		}
	}

	private boolean transitionState(JobStatus current, JobStatus newState) {
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("FailoverRegion {} switched from state {} to {}.", id, current, newState);
			return true;
		}
		else {
			return false;
		}
	}

}
