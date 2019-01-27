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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStatusListener;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.failover.ExecutionGraphOperationLog;
import org.apache.flink.runtime.jobmaster.failover.FailoverOperationLog;
import org.apache.flink.runtime.jobmaster.failover.InputSplitsOperationLog;
import org.apache.flink.runtime.jobmaster.failover.OperationLog;
import org.apache.flink.runtime.jobmaster.failover.OperationLogManager;
import org.apache.flink.runtime.jobmaster.failover.Replayable;
import org.apache.flink.runtime.jobmaster.failover.ResultDescriptor;
import org.apache.flink.runtime.jobmaster.failover.ResultPartitionOperationLog;
import org.apache.flink.runtime.schedule.ExecutionVertexStatus;
import org.apache.flink.runtime.schedule.GraphManagerPlugin;
import org.apache.flink.runtime.schedule.ResultPartitionStatus;
import org.apache.flink.runtime.schedule.SchedulingConfig;
import org.apache.flink.runtime.schedule.VertexScheduler;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The manager for the graph. It will manager the expansion of graph and the scheduling of the vertices.
 */
public class GraphManager implements Replayable, ExecutionStatusListener {

	static final Logger LOG = LoggerFactory.getLogger(GraphManager.class);

	/** The customizable plugin that handles vertex events and schedule vertices. */
	private final GraphManagerPlugin graphManagerPlugin;

	private final OperationLogManager operationLogManager;

	private final ExecutionGraph executionGraph;

	private final List<Collection<ExecutionVertexID>> executionVerticesToBeScheduled;

	private volatile boolean isReconciling;

	public GraphManager(
			GraphManagerPlugin graphManagerPlugin,
			JobMasterGateway jobMasterGateway,
			OperationLogManager operationLogManager,
			ExecutionGraph executionGraph) {
		this.graphManagerPlugin = checkNotNull(graphManagerPlugin);
		this.operationLogManager = checkNotNull(operationLogManager);
		this.executionGraph = checkNotNull(executionGraph);

		this.executionVerticesToBeScheduled = new LinkedList<>();
	}

	public void open(JobGraph jobGraph, SchedulingConfig config) {
		graphManagerPlugin.open(new ExecutionGraphVertexScheduler(), jobGraph, config);
	}

	/**
	 * Called when the graph manager should be stopped.
	 */
	public void dispose() {
		graphManagerPlugin.close();
	}

	/**
	 * Called when the graph starts a global failover.
	 */
	public void reset() {
		graphManagerPlugin.reset();
		operationLogManager.stop();
		operationLogManager.clear();
		operationLogManager.start();
	}

	/**
	 * Whether is replaying log.
	 */
	public boolean isReplaying() {
		return operationLogManager.isReplaying();
	}

	/**
	 * Whether is reconciling with task executors.
	 */
	public boolean isReconciling() {
		return isReconciling;
	}

	public void enterReconcile() {
		isReconciling = true;
	}

	public void leaveReconcile() {
		synchronized (executionVerticesToBeScheduled) {
			for (Collection<ExecutionVertexID> executionVertexIDS : executionVerticesToBeScheduled) {
				executionGraph.scheduleVertices(executionVertexIDS);
			}
			isReconciling = false;
		}
	}

	public boolean allowLazyDeployment() {
		return graphManagerPlugin.allowLazyDeployment();
	}

	public void startScheduling() {
		LOG.info("Start scheduling execution graph with graph manager plugin: {}",
			graphManagerPlugin.getClass().getName());
		graphManagerPlugin.onSchedulingStarted();
	}

	public void notifyExecutionVertexFailover(List<ExecutionVertexID> failoverExecutionVertices) {
		if (!operationLogManager.isReplaying()) {
			operationLogManager.writeOpLog(new FailoverOperationLog(failoverExecutionVertices));
		}
		graphManagerPlugin.onExecutionVertexFailover(
				new ExecutionVertexFailoverEvent(failoverExecutionVertices));
	}

	public void notifyResultPartitionConsumable(
			ExecutionVertexID executionVertexID,
			IntermediateDataSetID resultID,
			int partitionNumber,
			TaskManagerLocation location) {
		if (!operationLogManager.isReplaying() &&
				executionGraph.getAllIntermediateResults().get(resultID).getResultType().isPipelined()) {
			operationLogManager.writeOpLog(new ResultPartitionOperationLog(
					executionVertexID, resultID, location));
		}
		graphManagerPlugin.onResultPartitionConsumable(
				new ResultPartitionConsumableEvent(resultID, partitionNumber));
	}

	public void notifyInputSplitsCreated(
			JobVertexID jobVertexID,
			Map<OperatorID, InputSplit[]> inputSplitsMap) {
		if (!operationLogManager.isReplaying()) {
			operationLogManager.writeOpLog(new InputSplitsOperationLog(
					jobVertexID, inputSplitsMap));
		}
	}

	public boolean reconcileExecutionVertex(
			JobVertexID vertexId,
			int subtask,
			ExecutionState state,
			ExecutionAttemptID executionId,
			int attemptNumber,
			long startTimestamp,
			ResultPartitionID[] partitionIds,
			boolean[] partitionsConsumable,
			Map<OperatorID, List<InputSplit>> assignedInputSplits,
			LogicalSlot slot) {
		ExecutionJobVertex ejv = executionGraph.getJobVertex(vertexId);
		if (ejv == null || ejv.getParallelism() <= subtask) {
			LOG.info("Can not find the execution vertex {}_{}", vertexId, subtask);
			return false;
		}

		try {
			ExecutionVertex ev = ejv.getTaskVertices()[subtask];
			if (ev.reconcileExecution(
					state, executionId, attemptNumber, startTimestamp, partitionIds, partitionsConsumable, assignedInputSplits, slot)) {

				return true;
			} else {
				return false;
			}
		}
		catch (Throwable t) {
			LOG.info("Fail to reconcile vertex {}_{}.", vertexId, subtask, t);
			return false;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Implements of Replayable, for recovery after jm failover.
	// --------------------------------------------------------------------------------------------

	@Override
	public void replayOpLog(OperationLog opLog) {
		checkArgument(isReconciling && JobStatus.CREATED.equals(executionGraph.getState()),
				"Job is in " + executionGraph.getState() + " while replaying log.");

		if (opLog instanceof ExecutionGraphOperationLog) {
			ExecutionGraphOperationLog egOperationLog = (ExecutionGraphOperationLog) opLog;
			JobVertexID jobVertexID = egOperationLog.getExecutionVertexID().getJobVertexID();
			int subTaskIndex = egOperationLog.getExecutionVertexID().getSubTaskIndex();
			executionGraph.getJobVertex(jobVertexID).getTaskVertices()[subTaskIndex].recoverStatus(
					egOperationLog.getExecutionState(),
					egOperationLog.getConsumedInputs(),
					egOperationLog.getResultDescriptor());

		} else if (opLog instanceof ResultPartitionOperationLog) {
			ResultPartitionOperationLog rpOperationLog = (ResultPartitionOperationLog) opLog;

			JobVertexID jobVertexID = rpOperationLog.getExecutionVertexID().getJobVertexID();
			int subTaskIndex = rpOperationLog.getExecutionVertexID().getSubTaskIndex();
			executionGraph.getJobVertex(jobVertexID).getTaskVertices()[subTaskIndex].recoverResultPartitionStatus(
					rpOperationLog.getResultID(),
					rpOperationLog.getLocation());
		} else if (opLog instanceof FailoverOperationLog) {
			FailoverOperationLog failoverOperationLog = (FailoverOperationLog) opLog;

			List<ExecutionVertexID> ids = failoverOperationLog.getExecutionVertexIDs();
			List<ExecutionVertex> evs = new ArrayList<>(ids.size());
			for (ExecutionVertexID id : ids) {
				evs.add(executionGraph.getJobVertex(id.getJobVertexID()).getTaskVertices()[id.getSubTaskIndex()]);
			}
			try {
				for (Collection<ExecutionVertexID> toBeScheduledVertices : executionVerticesToBeScheduled) {
					toBeScheduledVertices.removeAll(evs);
				}
				executionGraph.resetExecutionVerticesAndNotify(executionGraph.getGlobalModVersion(), evs);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Fail to reset execution vertex", e);
			}
		} else if (opLog instanceof InputSplitsOperationLog) {
			InputSplitsOperationLog splitsOperationLog = (InputSplitsOperationLog) opLog;
			try {
				executionGraph.getAllVertices().get(splitsOperationLog.getJobVertexID())
						.setUpInputSplits(splitsOperationLog.getInputSplitsMap());
			} catch (Exception e) {
				throw new FlinkRuntimeException("Fail to set up input splits of vertex", e);
			}
		} else {
			throw new FlinkRuntimeException("Unsupported operation log " + opLog);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Implements of ExecutionStatusListener.
	// --------------------------------------------------------------------------------------------

	@Override
	public void executionStatusChanged(
			JobID jobID,
			JobVertexID vertexID,
			String taskName,
			int totalNumberOfSubTasks,
			int subtaskIndex,
			ExecutionAttemptID executionID,
			ExecutionState newExecutionState,
			long timestamp,
			String optionalMessage) {

		ExecutionVertexID executionVertexID = new ExecutionVertexID(vertexID, subtaskIndex);
		Map<OperatorID, List<InputSplit>> inputSplits = null;
		ResultDescriptor resultDescriptor = null;
		Map<String, Accumulator<?, ?>> userAccumulators = null;
		IOMetrics metrics = null;

		switch (newExecutionState) {
			case FINISHED:
				ExecutionVertex ev = executionGraph.getJobVertex(vertexID).getTaskVertices()[subtaskIndex];
				inputSplits = ev.getAssignedInputSplits();
				ResultPartitionID[] resultPartitionIds = new ResultPartitionID[ev.getProducedPartitions().size()];
				int i = 0;
				for (IntermediateResultPartitionID irp : ev.getProducedPartitions().keySet()) {
					resultPartitionIds[i++] = new ResultPartitionID(irp, ev.getCurrentExecutionAttempt().getAttemptId());
				}
				resultDescriptor = new ResultDescriptor(
						ev.getCurrentAssignedResourceLocation(),
						resultPartitionIds);
				userAccumulators = ev.getCurrentExecutionAttempt().getUserAccumulators();
				metrics = ev.getCurrentExecutionAttempt().getIOMetrics();

			// For DEPLOYING, RUNNING, just record the state.
			case RUNNING:
			case DEPLOYING:
				if (!operationLogManager.isReplaying()) {
					operationLogManager.writeOpLog(new ExecutionGraphOperationLog(
							executionVertexID,
							newExecutionState,
							inputSplits,
							resultDescriptor,
							userAccumulators,
							metrics));
				}
				graphManagerPlugin.onExecutionVertexStateChanged(new ExecutionVertexStateChangedEvent(
						executionVertexID,
						newExecutionState));
				break;
			default:
				break;
		}
	}

	/**
	 * This scheduler leverages ExecutionGraph to schedule vertices.
	 */
	public class ExecutionGraphVertexScheduler implements VertexScheduler {

		@Override
		public void scheduleExecutionVertices(Collection<ExecutionVertexID> verticesToSchedule) {
			synchronized (executionVerticesToBeScheduled) {
				if (isReconciling) {
					executionVerticesToBeScheduled.add(verticesToSchedule);
					return;
				}
			}
			executionGraph.scheduleVertices(verticesToSchedule);
		}

		@Override
		public ExecutionVertexStatus getExecutionVertexStatus(ExecutionVertexID executionVertexID) {
			checkNotNull(executionVertexID);

			ExecutionJobVertex vertex = executionGraph.getJobVertex(executionVertexID.getJobVertexID());
			if (vertex == null) {
				throw new IllegalArgumentException("Cannot find any vertex with id " + executionVertexID.getJobVertexID());
			}

			return vertex.getTaskVertices()[executionVertexID.getSubTaskIndex()].getCurrentStatus();
		}

		@Override
		public ResultPartitionStatus getResultPartitionStatus(IntermediateDataSetID resultID, int partitionNumber) {
			checkNotNull(resultID);
			IntermediateResult result = executionGraph.getAllIntermediateResults().get(resultID);
			if (result == null) {
				throw new IllegalArgumentException("Cannot find any result with id " + resultID);
			}

			return new ResultPartitionStatus(
				resultID,
				partitionNumber,
				result.getPartitions()[partitionNumber].isConsumable());
		}

		@Override
		public double getResultConsumablePartitionRatio(IntermediateDataSetID resultID) {
			checkNotNull(resultID);
			IntermediateResult result = executionGraph.getAllIntermediateResults().get(resultID);
			if (result == null) {
				throw new IllegalArgumentException("Cannot find any result with id " + resultID);
			}

			return result.getResultConsumablePartitionRatio();
		}
	}
}
