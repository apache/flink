/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utils to build a {@link TaskInputsOutputsDescriptor} from an {@link ExecutionJobVertex}.
 * The result descriptor would suit for an instance with max possible input channels and result subpartitions.
 */
final class ExecutionJobVertexTaskInputsOutputsDescriptorBuilder {

	static TaskInputsOutputsDescriptor buildTaskInputsOutputsDescriptor(
			final ExecutionJobVertex executionJobVertex,
			final Map<JobVertexID, ExecutionJobVertex> vertices) {

		final Map<IntermediateDataSetID, Integer> maxPossibleNumbersOfInputChannelsPerInstance =
			getMaxPossibleNumbersOfInputChannelsPerInstance(executionJobVertex);
		final Map<IntermediateDataSetID, Integer> maxPossibleNumbersOfResultSubpartitionsPerInstance =
			getMaxPossibleNumbersOfResultSubpartitionsPerInstance(executionJobVertex, vertices);

		return TaskInputsOutputsDescriptor.from(
			maxPossibleNumbersOfInputChannelsPerInstance,
			maxPossibleNumbersOfResultSubpartitionsPerInstance);
	}

	private static Map<IntermediateDataSetID, Integer> getMaxPossibleNumbersOfInputChannelsPerInstance(
			final ExecutionJobVertex executionJobVertex) {

		Map<IntermediateDataSetID, Integer> maxPossibleNumbers = new HashMap<>();
		final List<JobEdge> inputEdges = executionJobVertex.getJobVertex().getInputs();
		for (int i = 0; i < inputEdges.size(); i++) {
			final JobEdge inputEdge =  inputEdges.get(i);
			final IntermediateResult consumedResult = executionJobVertex.getInputs().get(i);

			// the inputs order should match in JobGraph and ExecutionGraph
			checkState(consumedResult.getId().equals(inputEdge.getSourceId()));

			final int maxPossibleNumber = getMaxPossibleNumberOfEdgesToTarget(
				executionJobVertex.getParallelism(),
				consumedResult.getNumberOfAssignedPartitions(),
				inputEdge.getDistributionPattern());
			maxPossibleNumbers.put(consumedResult.getId(), maxPossibleNumber);
		}

		return maxPossibleNumbers;
	}

	private static Map<IntermediateDataSetID, Integer> getMaxPossibleNumbersOfResultSubpartitionsPerInstance(
			final ExecutionJobVertex executionJobVertex,
			final Map<JobVertexID, ExecutionJobVertex> vertices) {

		Map<IntermediateDataSetID, Integer> maxPossibleNumbers = new HashMap<>();
		final List<IntermediateDataSet> producedDataSets = executionJobVertex.getJobVertex().getProducedDataSets();
		for (int i = 0; i < producedDataSets.size(); i++) {
			final IntermediateDataSet producedDataSet =  producedDataSets.get(i);

			checkState(
				producedDataSet.getConsumers().size() == 1,
				new IllegalStateException("Currently in execution, a result should have exactly one consumer job vertex."));

			final JobEdge outputEdge = producedDataSet.getConsumers().get(0);
			final ExecutionJobVertex consumerJobVertex = vertices.get(outputEdge.getTarget().getID());

			final int maxPossibleNumber = getMaxPossibleNumberOfEdgesToTarget(
				executionJobVertex.getParallelism(),
				consumerJobVertex.getParallelism(),
				outputEdge.getDistributionPattern());
			maxPossibleNumbers.put(producedDataSet.getId(), maxPossibleNumber);
		}

		return maxPossibleNumbers;
	}

	private static int getMaxPossibleNumberOfEdgesToTarget(
			final int thisParallelism,
			final int targetParallelism,
			final DistributionPattern distributionPattern) {

		switch (distributionPattern) {
			case ALL_TO_ALL:
				return targetParallelism;
			case POINTWISE:
				return (int) Math.ceil((double) targetParallelism / thisParallelism);
			default:
				throw new IllegalStateException("Unrecognized distribution pattern " + distributionPattern);
		}
	}
}
