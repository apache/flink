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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
	static final Logger LOG = LoggerFactory.getLogger(FailoverRegion.class);

	// a unique id for debugging
	private final ResourceID id = ResourceID.generate();

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.CREATED;

	private final FailoverCoordinator failoverCoordinator;

	private final List<ExecutionVertex> connectedExecutionVertexes;

	private final List<FailoverRegion> precedingRegions;

	private final List<FailoverRegion> succeedingRegions;

	// When a region failover, it should know whether its preceding is doing failover. 
	// If precedings are not doing failover, it should reset all its executions and start them.
	// Or else, it just reset its executions, when preceding execution, it will notice its executions to start.
	private volatile boolean waitPrecedings = false;

	public FailoverRegion(FailoverCoordinator failoverCoordinator, List<ExecutionVertex> connectedExecutions) {
		this.failoverCoordinator = checkNotNull(failoverCoordinator);
		this.connectedExecutionVertexes = checkNotNull(connectedExecutions);
		this.precedingRegions = new LinkedList<>();
		this.succeedingRegions = new LinkedList<>();
		LOG.info("FailoverRegion {} contains {}", id.toString(), connectedExecutions.toString());
	}

	// notice of state changing of execution from FailoverCoordinator
	public void notifyExecutionChange(ExecutionVertex ev, ExecutionState newState) {
		LOG.debug("{} change to state {}", ev.getSimpleName(), newState);
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.CREATED) && (newState.equals(ExecutionState.RUNNING)
					|| newState.equals(ExecutionState.SCHEDULED) || newState.equals(ExecutionState.DEPLOYING))) {
				if (transitionState(curStatus, JobStatus.RUNNING)) {
					this.waitPrecedings = false;
					break;
				}
			}
			else if (curStatus.equals(JobStatus.CANCELLING) && newState.isTerminal()) {
				//TODO: maybe recording the finished number as EG do now is better
				for (ExecutionVertex cev : connectedExecutionVertexes) {
					if (!cev.getExecutionState().isTerminal()) {
						return;
					}
				}
				if (transitionState(curStatus, JobStatus.CANCELED)) {
					if (this.failoverCoordinator.shuttingDown()) {
						this.failoverCoordinator.failoverRegionCanceled(this);
					}
					else {
						reset();
					}
					break;
				}
			}
			else if (newState.equals(ExecutionState.FAILED)) {
				// TODO: judge where preceding need failover
				if (curStatus.equals(JobStatus.CREATED) || curStatus.equals(JobStatus.CANCELED)) {
					LOG.debug("Execution ({}) changed to FAILED while region is {}.", ev.getSimpleName(), curStatus);
					break;
				}
				else {
					if (!this.failoverCoordinator.getExecutionGraph().getRestartStrategy().canRestart()) {
						this.failoverCoordinator.fail();
					}
					else if (transitionState(curStatus, JobStatus.CANCELLING)) {
						noticeSucceedingFailover();
						//TODO: RestartStrategy should be change to restart at failover region level
						this.failoverCoordinator.getExecutionGraph().getRestartStrategy().restart(
								this.failoverCoordinator.getExecutionGraph());
						cancel();
						break;
					}
				}
			}
			else {
				LOG.debug("Execution ({}) changed to state {} while region is {}.", ev.getSimpleName(), newState, state);
				break;
			}
		}
	}

	public JobStatus getState() {
		return state;
	}

	/**
	 *  whether an execution vertex is contained in this sub graph
	 */
	public boolean containsExecution(ExecutionVertex ev) {
		for (ExecutionVertex vertex : connectedExecutionVertexes) {
			if (vertex.equals(ev)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * get all execution vertexes contained in this region
	 */
	public List<ExecutionVertex> getAllExecutionVertexes() {
		return connectedExecutionVertexes;
	}

	public void addPrecedingFailoverRegion(FailoverRegion region) {
		if (!this.precedingRegions.contains(region)) {
			this.precedingRegions.add(region);
		}
	}

	public void addSucceedingFailoverRegion(FailoverRegion region) {
		if (!this.succeedingRegions.contains(region)) {
			this.succeedingRegions.add(region);
		}
	}

	// Notice the region to failover, 
	public void failover(boolean waitPrecedings) {
		while (true) {
			JobStatus curStatus = this.state;
			if (curStatus.equals(JobStatus.RUNNING)) {
				if (!this.failoverCoordinator.shuttingDown()
						&& !this.failoverCoordinator.getExecutionGraph().getRestartStrategy().canRestart()) {
					this.failoverCoordinator.fail();
				}
				else if (transitionState(curStatus, JobStatus.CANCELLING)) {
					this.waitPrecedings = waitPrecedings;
					if (this.failoverCoordinator.getExecutionGraph().getRestartStrategy().canRestart()) {
						this.failoverCoordinator.getExecutionGraph().getRestartStrategy().restart(
								this.failoverCoordinator.getExecutionGraph());
					}
					cancel();
					break;
				}
			}
			else {
				if(this.waitPrecedings == false) {
					this.waitPrecedings = waitPrecedings;
				}
				LOG.info("FailoverRegion {} do failover while in {}", id.toString(), curStatus);
				if ((curStatus.equals(JobStatus.CREATED) || curStatus.equals(JobStatus.CANCELED))
						&& this.failoverCoordinator.shuttingDown()) {
					this.failoverCoordinator.failoverRegionCanceled(this);
				}
				break;
			}
		}
		// Its succeeding regions should always do failover
		noticeSucceedingFailover();
	}

	// noticed by its succeeding that it change to CREATED
	public void succeedingCreated() {
		JobStatus curState = state;
		if (state.equals(JobStatus.CREATED)) {
			if ((!waitPrecedings || precedingRegions.size() == 0) && allSucceedingCreated()) {
				restart();
			}
		}
	}

	// tell all its succeedings to failover
	public void noticeSucceedingFailover() {
		for (FailoverRegion region : succeedingRegions) {
			region.failover(true);
		}
	}

	/**
	 * judge whether all its succeedings are in CREATED 
	 */
	public boolean allSucceedingCreated() {
		boolean running = true;
		for (FailoverRegion region : succeedingRegions) {
			if (!region.getState().equals(JobStatus.CREATED)) {
				running = false;
				break;
			}
		}
		return running;
	}

	// cancel all executions in this sub graph
	private void cancel() {
		try {
			int waitingCancelExecutionNum = 0;
			for (ExecutionVertex ev : connectedExecutionVertexes) {
				ev.cancel();
				if (!ev.getCurrentExecutionAttempt().isFinished()) {
					waitingCancelExecutionNum++;
				}
			}
			LOG.debug("After {} cancel, waiting to be canceled number {}", id.toString(), waitingCancelExecutionNum);
			if (waitingCancelExecutionNum <= 0) {
				if (transitionState(JobStatus.CANCELLING, JobStatus.CANCELED)) {
					if (!this.failoverCoordinator.shuttingDown()) {
						reset();
					}
					else {
						this.failoverCoordinator.failoverRegionCanceled(this);
					}

				}
				else {
					LOG.info("FailoverRegion {} switched from CANCELLING to CANCELED fail, will fail this region again", 
							id.toString());
					failover(this.waitPrecedings);
				}
			}
		} catch (Exception e) {
			LOG.info("FailoverRegion {} cancel fail, failover again", id.toString());
			failover(this.waitPrecedings);
		}
	}

	// reset all executions in this sub graph
	private void reset() {
		try {
			//reset all connected ExecutionVertexes
			Collection<CoLocationGroup> colGroups = new HashSet<>();
			for (ExecutionVertex ev : connectedExecutionVertexes) {
				CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
				if(cgroup != null && !colGroups.contains(cgroup)){
					cgroup.resetConstraints();
					colGroups.add(cgroup);
				}
				ev.resetForNewExecution();
			}
			if (transitionState(JobStatus.CANCELED, JobStatus.CREATED)) {
				LOG.debug("FailoverRegion {} switched from CANCELLED to CREATED", id.toString());
			}
			else {
				LOG.info("FailoverRegion {} switched from CANCELLING to CREATED fail, will fail this region again", id.toString());
				failover(this.waitPrecedings);
				return;
			}
			LOG.debug("Reset all execution vertexes for region {}", id.toString());

			noticePrecedingsCreated();

			if ((!waitPrecedings || precedingRegions.size() == 0) && allSucceedingCreated()) {
				restart();
			}
		} catch (Throwable e) {
			LOG.info("FailoverRegion {} reset fail, will failover again", id.toString());
			failover(this.waitPrecedings);
		}
	}

	// notice its precedings that it is created
	private void noticePrecedingsCreated() {
		for (FailoverRegion region : this.precedingRegions) {
			region.succeedingCreated();
		}
	}

	// restart all executions in this sub graph
	private void restart() {
		try {
			if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
				LOG.debug("FailoverRegion {} switched from CREATED to RUNNING", id.toString());
				// if we have checkpointed state, reload it into the executions
				//TODO: checkpoint support restore part ExecutionVertex cp
				/**
				if (failoverCoordinator.getExecutionGraph().getCheckpointCoordinator() != null) {
					failoverCoordinator.getExecutionGraph().getCheckpointCoordinator().restoreLatestCheckpointedState(
							connectedExecutionVertexes, false, false);
				}
				*/
				//restart all connected ExecutionVertexes
				for (ExecutionVertex ev : connectedExecutionVertexes) {
					try {
						ev.scheduleForExecution(failoverCoordinator.getExecutionGraph().getSlotProvider(), 
								failoverCoordinator.getExecutionGraph().isQueuedSchedulingAllowed());
					}
					catch (Throwable e) {
						failover(this.waitPrecedings);
					}
				}
			}
			else {
				LOG.info("FailoverRegion {} switched from CREATED to RUNNING fail, will fail this region again", id.toString());
				failover(this.waitPrecedings);
			}
		} catch (Exception e) {
			LOG.info("FailoverRegion {} restart failed, failover again.", id.toString(), e);
			failover(this.waitPrecedings);
		}
	}

	private boolean transitionState(JobStatus current, JobStatus newState) {
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("FailoverRegion {} switched from state {} to {}.", id.toString(), current, newState);
			return true;
		}
		else {
			return false;
		}
	}

	//----------------------------------
	// Methods only for tests
	//----------------------------------
	@VisibleForTesting
	List<FailoverRegion> getPrecedingRegions() {
		return this.precedingRegions;
	}

	@VisibleForTesting
	List<FailoverRegion> getSucceedingRegions() {
		return this.succeedingRegions;
	}

}
