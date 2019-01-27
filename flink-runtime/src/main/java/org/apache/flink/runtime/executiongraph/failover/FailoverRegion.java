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

	/** a unique id for debugging. */
	private final AbstractID id = new AbstractID();

	private final ExecutionGraph executionGraph;

	private final List<ExecutionVertex> connectedExecutionVertices;

	/** The executor that executes the recovery action after all vertices are in terminal state. */
	private final Executor executor;

	/** Current status of the job execution. */
	private volatile JobStatus state = JobStatus.RUNNING;

	/** The max number the region can fail. */
	private final int regionFailLimit;

	/** The number the region has failed and restarted. */
	private volatile int regionFailCount = 0;

	public FailoverRegion(ExecutionGraph executionGraph, Executor executor, List<ExecutionVertex> connectedExecutions,
						  int regionFailLimit) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executor = checkNotNull(executor);
		this.connectedExecutionVertices = checkNotNull(connectedExecutions);
		this.regionFailLimit = regionFailLimit;

		LOG.debug("Created failover region {} with vertices: {}", id, connectedExecutions);
	}

	public void onExecutionFail(Execution taskExecution, Throwable cause) {
		// TODO: check if need to failover the preceding region
		failover(taskExecution.getGlobalModVersion(), cause);
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
	 * get all execution vertices contained in this region.
	 */
	public List<ExecutionVertex> getAllExecutionVertices() {
		return connectedExecutionVertices;
	}

	/**
	 * Notify the region to failover.
 	 */
	private void failover(long globalModVersionOfFailover, Throwable cause) {
		LOG.info("Try to fail and restart region due to error: ", cause);

		regionFailCount++;

		if (!executionGraph.getRestartStrategy().canRestart()) {
			executionGraph.failGlobal(new FlinkException("RestartStrategy validate fail", cause));
		}
		else if (regionFailCount > regionFailLimit) {
			executionGraph.failGlobal(new FlinkException("FailoverRegion " + id + " exceeds max region restart limit", cause));
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
					final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(connectedExecutionVertices.size());

					// cancel all tasks (that still need cancelling)
					for (ExecutionVertex vertex : connectedExecutionVertices) {
						futures.add(vertex.cancel());
					}

					final FutureUtils.ConjunctFuture<Void> allTerminal = FutureUtils.waitForAll(futures);
					allTerminal.whenCompleteAsync(
						(Void ignored, Throwable throwable) -> {
							if (throwable != null) {
								failover(globalModVersionOfFailover,
									new FlinkException("Could not cancel all execution job vertices properly.", throwable));
							} else {
								allVerticesInTerminalState(globalModVersionOfFailover);
							}
						},
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
		if (transitionState(JobStatus.CANCELED, JobStatus.CREATED)) {
			// reset all connected ExecutionVertexes
			final Collection<CoLocationGroup> colGroups = new HashSet<>();

			for (ExecutionVertex ev : connectedExecutionVertices) {
				CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
				if (cgroup != null && !colGroups.contains(cgroup)){
					cgroup.resetConstraints();
					colGroups.add(cgroup);
				}
			}

			restart(globalModVersionOfFailover);
		}
		else {
			failover(globalModVersionOfFailover,
					new FlinkException("FailoverRegion " + id + " switch from CANCELLED to CREATED fail."));
		}
	}

	/**
	 * Restart the region by notify the schedule plugin.
	 */
	private void restart(long globalModVersionOfFailover) {
		try {
			if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
				// Let the scheduler event to reschedule connected ExecutionVertices
				executionGraph.resetExecutionVerticesAndNotify(globalModVersionOfFailover, connectedExecutionVertices);
			}
			else {
				failover(globalModVersionOfFailover,
						new FlinkException("FailoverRegion " + id + " witch from CREATED to RUNNING fail."));
			}
		} catch (GlobalModVersionMismatch e) {
			// happens when a global recovery happens concurrently to the regional recovery
			// should do nothing
		} catch (Exception e) {
			failover(globalModVersionOfFailover,
					new FlinkException("FailoverRegion " + id + " restart failed.", e));
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
