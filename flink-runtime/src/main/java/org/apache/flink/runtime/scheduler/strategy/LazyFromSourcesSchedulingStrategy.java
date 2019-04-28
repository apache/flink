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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LazyFromSourcesSchedulingStrategy implements SchedulingStrategy {

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(true);

	private final JobGraph jobGraph;

	public LazyFromSourcesSchedulingStrategy(
		SchedulerOperations schedulerOperations,
		SchedulingTopology schedulingTopology,
		JobGraph jobGraph) {
		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.jobGraph = checkNotNull(jobGraph);
	}

	@Override
	public void startScheduling() {
		List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = new LinkedList<>();
		for (SchedulingVertex schedulingVertex : schedulingTopology.getVertices()) {
			if (jobGraph.findVertexByID(schedulingVertex.getJobVertexId()).isInputVertex()) {
				// schedule vertices without consumed result partition
				executionVertexDeploymentOptions.add(
					new ExecutionVertexDeploymentOption(schedulingVertex.getId(), deploymentOption));
			}
		}
		if (!executionVertexDeploymentOptions.isEmpty()) {
			schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
		}
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesNeedingRestart) {
		List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = new ArrayList<>(verticesNeedingRestart.size());
		for (ExecutionVertexID executionVertexID : verticesNeedingRestart) {
			executionVertexDeploymentOptions.add(
				new ExecutionVertexDeploymentOption(executionVertexID, deploymentOption));
		}
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
	}

	@Override
	public void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState) {
		Optional<SchedulingVertex> schedulingVertex = schedulingTopology.getVertex(executionVertexId);
		if (!schedulingVertex.isPresent()) {
			throw new IllegalStateException("can not find scheduling vertex for " + executionVertexId);
		}

		if (ExecutionState.RUNNING.equals(executionState)) {
			List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = new ArrayList<>();
			for (SchedulingResultPartition srp : schedulingVertex.get().getProducedResultPartitions()) {
				if (srp.getPartitionType().isPipelined()) {
					for (SchedulingVertex sv : srp.getConsumers()) {
						executionVertexDeploymentOptions.add(
							new ExecutionVertexDeploymentOption(sv.getId(), deploymentOption));
					}
				}
			}
			schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
		} else if (ExecutionState.FINISHED.equals(executionState)) {
			List<SchedulingResultPartition> finishedPartitions = null;
			for (SchedulingResultPartition srp : schedulingVertex.get().getProducedResultPartitions()) {
				if (srp.getPartitionType().isBlocking()) {
					Optional<SchedulingIntermediateDataSet> intermediateDataSet = schedulingTopology.getIntermediateDataSet(srp.getResultId());
					if (!intermediateDataSet.isPresent()) {
						throw new IllegalStateException("can not find scheduling intermediate dataset for " + srp.getResultId());
					}
					final int refCnt = intermediateDataSet.get().decrementNumberOfRunningProducersAndGetRemaining();
					if (refCnt < 0) {
						throw new IllegalStateException("Decremented number of unfinished producers below 0. "
							+ "This is most likely a bug in the execution state/intermediate result "
							+ "partition management.");
					} else if (refCnt == 0) {
						if (finishedPartitions == null) {
							finishedPartitions = new LinkedList<>();
						}
						finishedPartitions.add(srp);
					}
				}
			}

			if (finishedPartitions != null) {
				List<SchedulingVertex> toScheduleConsumers = new LinkedList<>();
				for (SchedulingResultPartition finishedPartition : finishedPartitions) {
					toScheduleConsumers.addAll(finishedPartition.getConsumers());
				}
				scheduleBlockingConsumers(toScheduleConsumers);
			}
		}
	}

	@Override
	public void onPartitionConsumable(ExecutionVertexID executionVertexId, ResultPartitionID resultPartitionId) {
		// No need to react to this notification.
	}

	private void scheduleBlockingConsumers(List<SchedulingVertex> vertices) {
		List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = new LinkedList<>();
		for (SchedulingVertex schedulingVertex : vertices) {
			final ExecutionState state = schedulingVertex.getState();

			if (CREATED.equals(state)) {
				final InputDependencyConstraint inputDependency = jobGraph.findVertexByID(schedulingVertex
					.getJobVertexId()).getInputDependencyConstraint();

				boolean shouldSchedule = true;
				if (!InputDependencyConstraint.ANY.equals(inputDependency)) {
					for (SchedulingResultPartition srp : schedulingVertex.getConsumedResultPartitions()) {
						Optional<SchedulingIntermediateDataSet> sid = schedulingTopology.getIntermediateDataSet(srp.getResultId());
						if (!sid.isPresent()) {
							throw new IllegalArgumentException("can not find schedule intermediate data set for " + srp);
						}

						if (!sid.get().areAllPartitionsFinished()) {
							shouldSchedule = false;
							break;
						}
					}
				}

				if (shouldSchedule) {
					executionVertexDeploymentOptions.add(
						new ExecutionVertexDeploymentOption(schedulingVertex.getId(), deploymentOption));
				}
			}
		}

		if (!executionVertexDeploymentOptions.isEmpty()) {
			schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
		}
	}

	public static class Factory implements SchedulingStrategyFactory {
		@Override
		public SchedulingStrategy getInstance(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology,
			JobGraph jobGraph) {
			return new LazyFromSourcesSchedulingStrategy(schedulerOperations, schedulingTopology, jobGraph);
		}
	}
}
