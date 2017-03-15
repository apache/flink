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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A coordinator which generate {@code FailoverRegion} for all executions when initialing, and
 * A {@code ExecutionStatusListener} and {@code JobStatusListener} 
 * that finds a {@code FailoverRegion} to do failover for each execution failure.
 */
public class FailoverCoordinator implements ExecutionStatusListener, JobStatusListener {
	private static final AtomicReferenceFieldUpdater<FailoverCoordinator, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(FailoverCoordinator.class, JobStatus.class, "state");

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(FailoverCoordinator.class);

	private final Object lock = new Object();

	private final ExecutionGraph executionGraph;

	private final RestartStrategy restartStrategy;

	private final Map<FailoverRegion, Boolean> failoverRegions = new HashMap<>(1);

	private volatile int regionInFinalStateNumber;

	private JobStatus state = JobStatus.RUNNING;

	public FailoverCoordinator(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.restartStrategy = checkNotNull(executionGraph.getRestartStrategy());
		regionInFinalStateNumber = 0;
	}

	@Override
	public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error){
		//TODO: this is for original fail() in EG, maybe not using FAILING to trigger it is better
		if (newJobStatus.equals(JobStatus.FAILING)) {
			if (restartStrategy.canRestart()) {
				for (FailoverRegion failoverRegion : failoverRegions.keySet()) {
					failoverRegion.failover(true);
				}
			}
			else {
				fail();
			}
		}
	}

	@Override
	public void executionStatusChanged(
			JobID jobID, JobVertexID vertexID,
			String taskName, int taskParallelism, int subtaskIndex,
			ExecutionAttemptID executionID, ExecutionState newExecutionState,
			long timestamp, String optionalMessage) {

		ExecutionVertex ev = executionGraph.getJobVertex(vertexID).getTaskVertices()[subtaskIndex];
		if (ev == null) {
			LOG.warn("Not find execution vertex for job vertex id {}, sub index {}", vertexID.toString(), subtaskIndex);
			return;
		}
		FailoverRegion failoverRegion = getFailoverRegion(ev);
		if (failoverRegion == null) {
			executionGraph.fail(new Exception("Can not find a failover region for the execution " + ev.getSimpleName()));
		}
		else {
			failoverRegion.notifyExecutionChange(ev, newExecutionState);
		}
	}

	public ExecutionGraph getExecutionGraph() {
		return this.executionGraph;
	}

	// when the job is canceled, need to cancel all executions and wait for them to be canceled
	public void cancel() {
		JobStatus curStatus = state;
		while (true) {
			if (curStatus.equals(JobStatus.RUNNING)) {
				if (transitionState(curStatus, JobStatus.CANCELLING)) {
					regionInFinalStateNumber = 0;
					for (FailoverRegion failoverRegion : failoverRegions.keySet()) {
						failoverRegion.failover(true);
					}
					break;
				}
			} else {
				LOG.info("Failover coordinator already in {} when cancel", curStatus);
			}
		}
	}

	// the job need to fail, cancel all executions and wait for them to be canceled
	public void fail() {
		JobStatus curStatus = state;
		while (true) {
			if (curStatus.equals(JobStatus.RUNNING)) {
				if (transitionState(curStatus, JobStatus.FAILING)) {
					regionInFinalStateNumber = 0;
					for (FailoverRegion failoverRegion : failoverRegions.keySet()) {
						failoverRegion.failover(true);
					}
					break;
				}
			} else {
				LOG.info("Failover coordinator already in {} when fail", curStatus);
			}
		}
	}

	// is the whole job is failing or canceling
	public boolean shuttingDown() {
		return state.equals(JobStatus.FAILING) || state.equals(JobStatus.CANCELLING);
	}

	// Notice by a failover region that it is in CANCELED 
	public void failoverRegionCanceled(FailoverRegion region) {
		synchronized (lock) {
			// need not check exist as failoverRegions are constructed at initializing
			if (!failoverRegions.get(region)) {
				failoverRegions.put(region, true);
				regionInFinalStateNumber++;
			}
		}
		if (regionInFinalStateNumber == failoverRegions.size()) {
			JobStatus curStatus = state;
			if (curStatus.equals(JobStatus.FAILING)) {
				if (transitionState(curStatus, JobStatus.FAILED)) {
					this.executionGraph.toFinalState(JobStatus.FAILED, null);
				}
			}
			else if (curStatus.equals(JobStatus.CANCELLING)) {
				if (transitionState(curStatus, JobStatus.CANCELED)) {
					this.executionGraph.toFinalState(JobStatus.CANCELED, null);
				}
			}
		}
	}

	// find the failover region that contains the execution vertex
	@VisibleForTesting
	FailoverRegion getFailoverRegion(ExecutionVertex ev) {
		for (FailoverRegion region : failoverRegions.keySet()) {
			if (region.containsExecution(ev)) {
				return region;
			}
		}
		return null;
	}

	// Generate all the FailoverRegion from the ExecutionGraph
	public void generateAllFailoverRegion() {
		failoverRegions.clear();
		for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
			if (getFailoverRegion(ev) != null) {
				continue;
			}
			List<ExecutionVertex> pipelinedExecutions = new LinkedList<>();
			List<ExecutionVertex> orgExecutions = new LinkedList<>();
			orgExecutions.add(ev);
			pipelinedExecutions.add(ev);
			getAllPipelinedConnectedVertexes(orgExecutions, pipelinedExecutions);
			failoverRegions.put(new FailoverRegion(this, pipelinedExecutions), false);
		}
		buildRelationOfFailoverRegions();
	}

	/**
	 * Get all connected executions of the original executions
	 *
	 * @param orgExecutions  the original execution vertexes
	 * @param connectedExecutions  the total connected executions
	 */
	private static void getAllPipelinedConnectedVertexes(List<ExecutionVertex> orgExecutions, List<ExecutionVertex> connectedExecutions) {
		List<ExecutionVertex> newAddedExecutions = new LinkedList<>();
		for (ExecutionVertex ev : orgExecutions) {
			// Add downstream ExecutionVertex
			for (IntermediateResultPartition irp : ev.getProducedPartitions().values()) {
				if (irp.getResultType().isPipelined()) {
					for (List<ExecutionEdge> consumers : irp.getConsumers()) {
						for (ExecutionEdge consumer : consumers) {
							ExecutionVertex cev = consumer.getTarget();
							if (!connectedExecutions.contains(cev)) {
								newAddedExecutions.add(cev);
							}
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
			// Add upstream ExecutionVertex
			int inputNum = ev.getNumberOfInputs();
			for (int i = 0; i < inputNum; i++) {
				for (ExecutionEdge input : ev.getInputEdges(i)) {
					if (input.getSource().getResultType().isPipelined()) {
						ExecutionVertex pev = input.getSource().getProducer();
						if (!connectedExecutions.contains(pev)) {
							newAddedExecutions.add(pev);
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(0, newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
		}
	}

	private void buildRelationOfFailoverRegions() {
		for (FailoverRegion failoverRegion : failoverRegions.keySet()) {
			for (ExecutionVertex ev : failoverRegion.getAllExecutionVertexes()) {
				for (int i = 0; i < ev.getNumberOfInputs(); i++) {
					for (ExecutionEdge edge : ev.getInputEdges(i)) {
						ExecutionVertex previous = edge.getSource().getProducer();
						if (failoverRegion.containsExecution(previous)) {
							continue;
						}
						else {
							for (FailoverRegion region : failoverRegions.keySet()) {
								if (region.containsExecution(previous)) {
									failoverRegion.addPrecedingFailoverRegion(region);
									region.addSucceedingFailoverRegion(failoverRegion);
								}
							}
						}
					}
				}
			}
		}
	}

	private boolean transitionState(JobStatus current, JobStatus newState) {
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("FailoverCoordinator switched from state {} to {}.", current, newState);
			return true;
		}
		else {
			return false;
		}
	}
}
