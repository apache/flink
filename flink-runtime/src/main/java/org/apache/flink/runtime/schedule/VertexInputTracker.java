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

package org.apache.flink.runtime.schedule;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tracks the input readiness of vertices.
 */
public class VertexInputTracker {

	private static final Logger LOG = LoggerFactory.getLogger(VertexInputTracker.class);

	private VertexScheduler scheduler;

	private VertexInputTrackerConfig config;

	private Map<ExecutionVertexID, Map<IntermediateDataSetID, VertexInput>> vertexInputsMap = new HashMap<>();

	public VertexInputTracker(JobGraph jobGraph, VertexScheduler scheduler, SchedulingConfig schedulingConfig) {
		checkNotNull(jobGraph);
		checkNotNull(schedulingConfig);
		this.scheduler = checkNotNull(scheduler);

		try {
			Configuration configuration = schedulingConfig.getConfiguration();
			config = InstantiationUtil.readObjectFromConfig(
				configuration,
				VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG,
				schedulingConfig.getUserClassLoader());

			if (config == null) {
				config = new VertexInputTracker.VertexInputTrackerConfig(
					InputDependencyConstraint.valueOf(
						configuration.getValue(VertexInputTrackerOptions.INPUT_DEPENDENCY_CONSTRAINT)),
					configuration.getDouble(VertexInputTrackerOptions.INPUT_CONSUMABLE_THRESHOLD_PIPELINED),
					configuration.getDouble(VertexInputTrackerOptions.INPUT_CONSUMABLE_THRESHOLD_BLOCKING)
				);
			}

			LOG.info("VertexInputTracker config: {}", config);
		} catch (Exception e) {
			throw new IllegalStateException("Could not load or create InputTracker config.", e);
		}

		// build vertex inputs map
		for (IntermediateDataSetID resultID : jobGraph.getResultIDs()) {
			JobVertex producerJobVertex = jobGraph.findVertexByID(jobGraph.getResultProducerID(resultID));
			for (int i = 0; i < producerJobVertex.getParallelism(); i++) {
				for (JobEdge edge : jobGraph.getResult(resultID).getConsumers()) {
					for (ExecutionVertexID vertexID : edge.getConsumerExecutionVertices(i)) {
						if (!vertexInputsMap.containsKey(vertexID)) {
							vertexInputsMap.put(vertexID, new HashMap<>());
						}
						if (!vertexInputsMap.get(vertexID).containsKey(resultID)) {
							double consumableThreshold = config.getInputConsumableThreshold(
								vertexID.getJobVertexID(),
								resultID,
								jobGraph.getResult(resultID).getResultType().isPipelined());

							if (edge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
								vertexInputsMap.get(vertexID).put(resultID,
									new AllToAllVertexInput(resultID, consumableThreshold));
							} else {
								vertexInputsMap.get(vertexID).put(resultID,
									new VertexInput(resultID, consumableThreshold));
							}
						}
						vertexInputsMap.get(vertexID).get(resultID).addPartition(i);
					}
				}
			}
		}
	}

	public boolean areInputsReady(ExecutionVertexID vertexID) {
		// It is source vertex if no input info
		if (!vertexInputsMap.containsKey(vertexID)) {
			return true;
		}

		if (config.getInputDependencyConstraint(vertexID.getJobVertexID()) == InputDependencyConstraint.ALL) {
			for (VertexInput input : vertexInputsMap.get(vertexID).values()) {
				if (!input.isConsumable(scheduler)) {
					return false;
				}
			}
			return true;
		} else {
			for (VertexInput input : vertexInputsMap.get(vertexID).values()) {
				if (input.isConsumable(scheduler)) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * This indicates when the inputs of a vertex are considered ready overall.
	 */
	public enum InputDependencyConstraint {

		/** Ready if any input is consumable. */
		ANY,

		/** Ready if all the inputs are consumable. */
		ALL
	}

	/**
	 * Config options for VertexInputTracker.
	 */
	public static class VertexInputTrackerOptions {

		public static final String VERTEX_INPUT_TRACKER_CONFIG = "vertex-input-tracker.config";

		public static final ConfigOption<String> INPUT_DEPENDENCY_CONSTRAINT =
			key(VERTEX_INPUT_TRACKER_CONFIG + ".input-dependency-constraint")
				.defaultValue(InputDependencyConstraint.ANY.toString());

		public static final ConfigOption<Double> INPUT_CONSUMABLE_THRESHOLD_PIPELINED =
			key(VERTEX_INPUT_TRACKER_CONFIG + ".input-consumable-threshold.pipelined")
				.defaultValue(Double.MIN_VALUE);

		public static final ConfigOption<Double> INPUT_CONSUMABLE_THRESHOLD_BLOCKING =
			key(VERTEX_INPUT_TRACKER_CONFIG + ".input-consumable-threshold.blocking")
				.defaultValue(1.0);
	}

	/**
	 * Configs for the VertexInputTracker.
	 */
	public static class VertexInputTrackerConfig implements Serializable {

		private InputDependencyConstraint globalInputDependencyConstraint;

		private double globalPipelinedInputConsumableThreshold;

		private double globalBlockingInputConsumableThreshold;

		private Map<JobVertexID, InputDependencyConstraint> vertexInputDependencyConstraintMap = new HashMap<>();

		private Map<JobVertexID, Map<IntermediateDataSetID, Double>> inputConsumableThresholdMap = new HashMap<>();

		public VertexInputTrackerConfig(
			InputDependencyConstraint inputDependencyConstraint,
			double pipelinedInputConsumableThreshold,
			double blockingInputConsumableThreshold) {

			this.globalInputDependencyConstraint = inputDependencyConstraint;
			this.globalPipelinedInputConsumableThreshold = pipelinedInputConsumableThreshold;
			this.globalBlockingInputConsumableThreshold = blockingInputConsumableThreshold;
		}

		public void setInputDependencyConstraint(
			JobVertexID vertexID,
			InputDependencyConstraint inputDependencyConstraint) {

			vertexInputDependencyConstraintMap.put(vertexID, inputDependencyConstraint);
		}

		public InputDependencyConstraint getInputDependencyConstraint(JobVertexID vertexID) {
			if (vertexInputDependencyConstraintMap.containsKey(vertexID)) {
				return vertexInputDependencyConstraintMap.get(vertexID);
			}
			return globalInputDependencyConstraint;
		}

		public void setInputConsumableThreshold(
			JobVertexID vertexID,
			IntermediateDataSetID resultID,
			double threshold) {

			if (!inputConsumableThresholdMap.containsKey(vertexID)) {
				inputConsumableThresholdMap.put(vertexID, new HashMap<>());
			}
			inputConsumableThresholdMap.get(vertexID).put(resultID, threshold);
		}

		public double getInputConsumableThreshold(
			JobVertexID vertexID,
			IntermediateDataSetID resultID,
			boolean isPipelined) {

			if (inputConsumableThresholdMap.containsKey(vertexID) &&
				inputConsumableThresholdMap.get(vertexID).containsKey(resultID)) {
				return inputConsumableThresholdMap.get(vertexID).get(resultID);
			}

			if (isPipelined) {
				return globalPipelinedInputConsumableThreshold;
			} else {
				return globalBlockingInputConsumableThreshold;
			}
		}

		@Override
		public String toString() {
			return "VertexInputTrackerConfig(" +
				"globalInputDependencyConstraint: " + globalInputDependencyConstraint + ", " +
				"globalPipelinedInputConsumableThreshold: " + globalPipelinedInputConsumableThreshold + ", " +
				"globalBlockingInputConsumableThreshold: " + globalBlockingInputConsumableThreshold + ", " +
				"vertexInputDependencyConstraintMap: " + vertexInputDependencyConstraintMap + ", " +
				"vertexInputConsumableThresholdMap: " + inputConsumableThresholdMap + ", " +
				")";
		}
	}

	/**
	 * Input of a vertex. It consists of multiple partitions from one result.
	 */
	private static class VertexInput {

		protected double consumableThreshold;

		protected IntermediateDataSetID resultID;

		private Collection<Integer> partitions = new ArrayList<>();

		public VertexInput(IntermediateDataSetID resultID, double consumableThreshold) {
			this.resultID = resultID;
			this.consumableThreshold = consumableThreshold;
		}

		public void addPartition(int partitionNumber) {
			partitions.add(partitionNumber);
		}

		public boolean isConsumable(VertexScheduler scheduler) {
			int consumableCount = 0;
			for (int partitionNumber : partitions) {
				if (scheduler.getResultPartitionStatus(resultID, partitionNumber).isConsumable()) {
					consumableCount++;
				}
			}
			return 1.0 * consumableCount / partitions.size() >= consumableThreshold;
		}
	}

	/**
	 * Vertex input for ALL-to-ALL job edges.
	 * It helps to reduce the load to compute whether the input is consumable significantly.
	 */
	private static class AllToAllVertexInput extends VertexInput {

		public AllToAllVertexInput(IntermediateDataSetID resultID, double consumableThreshold) {
			super(resultID, consumableThreshold);
		}

		@Override
		public void addPartition(int partitionNumber) {
			// No need to maintain the partitions. Reduce memory cost as well.
		}

		@Override
		public boolean isConsumable(VertexScheduler scheduler) {
			return scheduler.getResultConsumablePartitionRatio(resultID) >= consumableThreshold;
		}
	}
}
