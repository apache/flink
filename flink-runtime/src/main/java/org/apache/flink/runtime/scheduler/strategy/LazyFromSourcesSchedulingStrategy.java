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
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
			for (SchedulingResultPartition srp : schedulingVertex.getProducedResults()) {
				if (srp.getResultType().isPipelined()) {
					option = updateOption;
				}
				inputConstraintChecker.addSchedulingResultPartition(srp);
			}
			deploymentOptions.put(schedulingVertex.getId(), option);
		}

		allocateSlotsAndDeployExecutionVertices(schedulingTopology.getVertices());
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		// increase counter of the dataset first
		verticesToRestart
			.stream()
			.map(schedulingTopology::getVertex)
			.flatMap(vertex -> IterableUtils.toStream(vertex.getProducedResults()))
			.forEach(inputConstraintChecker::resetSchedulingResultPartition);

		allocateSlotsAndDeployExecutionVertices(
			SchedulingStrategyUtils.getVerticesFromIds(schedulingTopology, verticesToRestart));
	}

	@Override
	public void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState) {
		if (!FINISHED.equals(executionState)) {
			return;
		}

		final Set<SchedulingExecutionVertex> verticesToSchedule = IterableUtils
			.toStream(schedulingTopology.getVertex(executionVertexId).getProducedResults())
			.filter(partition -> partition.getResultType().isBlocking())
			.flatMap(partition -> inputConstraintChecker.markSchedulingResultPartitionFinished(partition).stream())
			.flatMap(partition -> IterableUtils.toStream(partition.getConsumers()))
			.collect(Collectors.toSet());

		allocateSlotsAndDeployExecutionVertices(verticesToSchedule);
	}

	@Override
	public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {
		final SchedulingResultPartition resultPartition = schedulingTopology
			.getResultPartition(resultPartitionId);

		if (!resultPartition.getResultType().isPipelined()) {
			return;
		}

		allocateSlotsAndDeployExecutionVertices(resultPartition.getConsumers());
	}

	private void allocateSlotsAndDeployExecutionVertices(
			final Iterable<? extends SchedulingExecutionVertex> vertices) {

		final Set<ExecutionVertexID> verticesToDeploy = IterableUtils.toStream(vertices)
			.filter(IS_IN_CREATED_EXECUTION_STATE.and(isInputConstraintSatisfied()))
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());

		final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
				schedulingTopology,
				verticesToDeploy,
				deploymentOptions::get);

		for (ExecutionVertexDeploymentOption deploymentOption : vertexDeploymentOptions) {
			schedulerOperations.allocateSlotsAndDeploy(Collections.singletonList(deploymentOption));
		}
	}

	private Predicate<SchedulingExecutionVertex> isInputConstraintSatisfied() {
		return inputConstraintChecker::check;
	}

	/**
	 * The factory for creating {@link LazyFromSourcesSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {
		@Override
		public SchedulingStrategy createInstance(
				SchedulerOperations schedulerOperations,
				SchedulingTopology schedulingTopology) {
			return new LazyFromSourcesSchedulingStrategy(schedulerOperations, schedulingTopology);
		}
	}
}
