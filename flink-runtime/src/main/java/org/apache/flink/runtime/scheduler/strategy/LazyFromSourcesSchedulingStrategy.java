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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for batch job which schedule vertices when input data are ready.
 */
public class LazyFromSourcesSchedulingStrategy implements SchedulingStrategy {

	private static final Predicate<SchedulingExecutionVertex> IS_IN_CREATED_EXECUTION_STATE = schedulingExecutionVertex -> CREATED == schedulingExecutionVertex.getState();

	private final SchedulerOperations schedulerOperations;

	private final SchedulingTopology schedulingTopology;

	private final Map<ExecutionVertexID, DeploymentOption> deploymentOptions;

	private final InputDependencyConstraintChecker inputConstraintChecker;

	public LazyFromSourcesSchedulingStrategy(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology) {

		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.deploymentOptions = new HashMap<>();
		this.inputConstraintChecker = new InputDependencyConstraintChecker();
	}

	@Override
	public void startScheduling() {
		final DeploymentOption updateOption = new DeploymentOption(true);
		final DeploymentOption nonUpdateOption = new DeploymentOption(false);

		for (SchedulingExecutionVertex schedulingVertex : schedulingTopology.getVertices()) {
			DeploymentOption option = nonUpdateOption;
			for (SchedulingResultPartition srp : schedulingVertex.getProducedResultPartitions()) {
				if (srp.getPartitionType().isPipelined()) {
					option = updateOption;
				}
				inputConstraintChecker.addSchedulingResultPartition(srp);
			}
			deploymentOptions.put(schedulingVertex.getId(), option);
		}

		allocateSlotsAndDeployExecutionVertexIds(getAllVerticesFromTopology());
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		// increase counter of the dataset first
		verticesToRestart
			.stream()
			.map(schedulingTopology::getVertexOrThrow)
			.flatMap(vertex -> vertex.getProducedResultPartitions().stream())
			.forEach(inputConstraintChecker::resetSchedulingResultPartition);

		allocateSlotsAndDeployExecutionVertexIds(verticesToRestart);
	}

	@Override
	public void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState) {
		if (!FINISHED.equals(executionState)) {
			return;
		}

		final Set<SchedulingExecutionVertex> verticesToSchedule = schedulingTopology.getVertexOrThrow(executionVertexId)
			.getProducedResultPartitions()
			.stream()
			.flatMap(partition -> inputConstraintChecker.markSchedulingResultPartitionFinished(partition).stream())
			.flatMap(partition -> partition.getConsumers().stream())
			.collect(Collectors.toSet());

		allocateSlotsAndDeployExecutionVertices(verticesToSchedule);
	}

	@Override
	public void onPartitionConsumable(ExecutionVertexID executionVertexId, ResultPartitionID resultPartitionId) {
		final SchedulingResultPartition resultPartition = schedulingTopology
			.getResultPartitionOrThrow(resultPartitionId.getPartitionId());

		if (!resultPartition.getPartitionType().isPipelined()) {
			return;
		}

		final SchedulingExecutionVertex producerVertex = schedulingTopology.getVertexOrThrow(executionVertexId);
		if (!producerVertex.getProducedResultPartitions().contains(resultPartition)) {
			throw new IllegalStateException("partition " + resultPartitionId
					+ " is not the produced partition of " + executionVertexId);
		}

		allocateSlotsAndDeployExecutionVertices(resultPartition.getConsumers());
	}

	private void allocateSlotsAndDeployExecutionVertexIds(Set<ExecutionVertexID> verticesToSchedule) {
		allocateSlotsAndDeployExecutionVertices(
			verticesToSchedule
				.stream()
				.map(schedulingTopology::getVertexOrThrow)
				.collect(Collectors.toList()));
	}

	private void allocateSlotsAndDeployExecutionVertices(final Collection<SchedulingExecutionVertex> schedulingExecutionVertices) {
		schedulerOperations.allocateSlotsAndDeploy(
			schedulingExecutionVertices
				.stream()
				.filter(isInputConstraintSatisfied().and(IS_IN_CREATED_EXECUTION_STATE))
				.map(SchedulingExecutionVertex::getId)
				.map(executionVertexID -> new ExecutionVertexDeploymentOption(
					executionVertexID,
					deploymentOptions.get(executionVertexID)))
				.collect(Collectors.toSet()));
	}

	private Predicate<SchedulingExecutionVertex> isInputConstraintSatisfied() {
		return inputConstraintChecker::check;
	}

	private Set<ExecutionVertexID> getAllVerticesFromTopology() {
		return StreamSupport
			.stream(schedulingTopology.getVertices().spliterator(), false)
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
	}

	/**
	 * The factory for creating {@link LazyFromSourcesSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {
		@Override
		public SchedulingStrategy createInstance(
				SchedulerOperations schedulerOperations,
				SchedulingTopology schedulingTopology,
				JobGraph jobGraph) {
			return new LazyFromSourcesSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
