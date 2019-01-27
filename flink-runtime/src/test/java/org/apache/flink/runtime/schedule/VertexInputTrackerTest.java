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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link VertexInputTracker}.
 */
public class VertexInputTrackerTest {

	/**
	 * Tests vertex input tracker.
	 */
	@Test
	public void testVertexInputTracker() throws Exception {
		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});

		Configuration configuration = new Configuration();
		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ANY,
				Double.MIN_VALUE,
				1
			);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, configuration,
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		VertexScheduler scheduler = mock(VertexScheduler.class);
		Map<IntermediateDataSetID, Map<Integer, Boolean>> resultConsumableMap = new HashMap<>();
		IntermediateDataSetID resultID1 = v1.getProducedDataSets().get(0).getId();
		IntermediateDataSetID resultID2 = v2.getProducedDataSets().get(0).getId();
		resultConsumableMap.put(resultID1, new HashMap<>());
		resultConsumableMap.put(resultID2, new HashMap<>());
		for (int i = 0; i < parallelism; i++) {
			resultConsumableMap.get(resultID1).put(i, false);
			resultConsumableMap.get(resultID2).put(i, false);
		}
		when(scheduler.getResultPartitionStatus(any(IntermediateDataSetID.class), anyInt())).thenAnswer(
			invocation -> {
				IntermediateDataSetID resultID = invocation.getArgumentAt(0, IntermediateDataSetID.class);
				int partitionNumber = invocation.getArgumentAt(1, Integer.class);

				boolean isConsumable = false;
				if (resultConsumableMap.get(resultID) != null) {
					isConsumable = resultConsumableMap.get(resultID).get(partitionNumber);
				}

				return new ResultPartitionStatus(resultID, partitionNumber, isConsumable);
			}
		);
		when(scheduler.getResultConsumablePartitionRatio(any(IntermediateDataSetID.class))).thenAnswer(
			invocation -> {
				IntermediateDataSetID resultID = invocation.getArgumentAt(0, IntermediateDataSetID.class);

				int consumableCount = 0;
				for (boolean consumable : resultConsumableMap.get(resultID).values()) {
					if (consumable) {
						consumableCount++;
					}
				}

				return 1.0 * consumableCount / resultConsumableMap.get(resultID).size();
			}
		);

		VertexInputTracker tracker = new VertexInputTracker(jobGraph, scheduler,
			new SchedulingConfig(configuration, VertexInputTrackerTest.class.getClassLoader()));

		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v1.getID(), i)));
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v2.getID(), i)));
			assertFalse(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}

		// any pipelined result partition consumable
		resultConsumableMap.get(resultID1).put(0, true);
		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		resultConsumableMap.get(resultID1).put(0, false);

		// any blocking result partition consumable
		resultConsumableMap.get(resultID2).put(0, true);
		for (int i = 0; i < parallelism; i++) {
			assertFalse(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		resultConsumableMap.get(resultID2).put(0, false);

		// all blocking result partition consumable
		for (int i = 0; i < parallelism; i++) {
			resultConsumableMap.get(resultID2).put(i, true);
		}
		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		for (int i = 0; i < parallelism; i++) {
			resultConsumableMap.get(resultID2).put(i, false);
		}
	}

	/**
	 * Tests vertex input tracker.
	 * Using vertex level percentile threshold and input constraints.
	 */
	@Test
	public void testVertexInputTrackerVertexLevel() throws Exception {
		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});

		Configuration configuration = new Configuration();
		double pipelinedConsumableThreshold = 0.4;
		double blockingConsumableThreshold = 0.7;
		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ANY,
				Double.MIN_VALUE,
				1
			);
		inputTrackerConfig.setInputDependencyConstraint(
			v3.getID(),
			VertexInputTracker.InputDependencyConstraint.ANY
		);
		inputTrackerConfig.setInputConsumableThreshold(
			v3.getID(),
			v1.getProducedDataSets().get(0).getId(),
			pipelinedConsumableThreshold
		);
		inputTrackerConfig.setInputConsumableThreshold(
			v3.getID(),
			v2.getProducedDataSets().get(0).getId(),
			blockingConsumableThreshold
		);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, configuration,
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		VertexScheduler scheduler = mock(VertexScheduler.class);
		Map<IntermediateDataSetID, Map<Integer, Boolean>> resultConsumableMap = new HashMap<>();
		IntermediateDataSetID resultID1 = v1.getProducedDataSets().get(0).getId();
		IntermediateDataSetID resultID2 = v2.getProducedDataSets().get(0).getId();
		resultConsumableMap.put(resultID1, new HashMap<>());
		resultConsumableMap.put(resultID2, new HashMap<>());
		for (int i = 0; i < parallelism; i++) {
			resultConsumableMap.get(resultID1).put(i, false);
			resultConsumableMap.get(resultID2).put(i, false);
		}
		when(scheduler.getResultPartitionStatus(any(IntermediateDataSetID.class), anyInt())).thenAnswer(
			invocation -> {
				IntermediateDataSetID resultID = invocation.getArgumentAt(0, IntermediateDataSetID.class);
				int partitionNumber = invocation.getArgumentAt(1, Integer.class);

				boolean isConsumable = false;
				if (resultConsumableMap.get(resultID) != null) {
					isConsumable = resultConsumableMap.get(resultID).get(partitionNumber);
				}

				return new ResultPartitionStatus(resultID, partitionNumber, isConsumable);
			}
		);
		when(scheduler.getResultConsumablePartitionRatio(any(IntermediateDataSetID.class))).thenAnswer(
			invocation -> {
				IntermediateDataSetID resultID = invocation.getArgumentAt(0, IntermediateDataSetID.class);

				int consumableCount = 0;
				for (boolean consumable : resultConsumableMap.get(resultID).values()) {
					if (consumable) {
						consumableCount++;
					}
				}

				return 1.0 * consumableCount / resultConsumableMap.get(resultID).size();
			}
		);

		VertexInputTracker tracker = new VertexInputTracker(jobGraph, scheduler,
			new SchedulingConfig(configuration, VertexInputTrackerTest.class.getClassLoader()));

		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v1.getID(), i)));
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v2.getID(), i)));
			assertFalse(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}

		// Set the blocking result consumable rate smaller than threshold
		for (int i = 0; i < parallelism * blockingConsumableThreshold - 1; i++) {
			resultConsumableMap.get(resultID2).put(i, true);
		}
		for (int i = 0; i < parallelism; i++) {
			assertFalse(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		for (int i = 0; i < parallelism * blockingConsumableThreshold; i++) {
			resultConsumableMap.get(resultID2).put(i, false);
		}

		// Set the blocking result consumable rate larger than threshold
		for (int i = 0; i < parallelism * blockingConsumableThreshold; i++) {
			resultConsumableMap.get(resultID2).put(i, true);
		}
		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		for (int i = 0; i < parallelism * blockingConsumableThreshold; i++) {
			resultConsumableMap.get(resultID2).put(i, false);
		}

		// Set the pipelined result consumable rate smaller than threshold
		for (int i = 0; i < parallelism * pipelinedConsumableThreshold - 1; i++) {
			resultConsumableMap.get(resultID1).put(i, true);
		}
		for (int i = 0; i < parallelism; i++) {
			assertFalse(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		for (int i = 0; i < parallelism * blockingConsumableThreshold; i++) {
			resultConsumableMap.get(resultID1).put(i, false);
		}

		// Set the pipelined result consumable rate larger than threshold
		for (int i = 0; i < parallelism * pipelinedConsumableThreshold; i++) {
			resultConsumableMap.get(resultID1).put(i, true);
		}
		for (int i = 0; i < parallelism; i++) {
			assertTrue(tracker.areInputsReady(new ExecutionVertexID(v3.getID(), i)));
		}
		for (int i = 0; i < parallelism * blockingConsumableThreshold; i++) {
			resultConsumableMap.get(resultID1).put(i, false);
		}
	}
}
