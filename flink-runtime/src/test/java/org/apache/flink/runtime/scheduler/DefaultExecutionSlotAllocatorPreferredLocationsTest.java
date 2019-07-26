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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for the calculation of preferred locations based on inputs in {@link DefaultExecutionSlotAllocator}.
 */
public class DefaultExecutionSlotAllocatorPreferredLocationsTest extends TestLogger {

	/**
	 * Tests that the input edge will be ignored if it has too many different locations.
	 */
	@Test
	public void testIgnoreEdgeOfTooManyLocations() throws Exception {
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);
		final List<ExecutionVertexID> producerIds = new ArrayList<>(ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER + 1);

		TestingInputsLocationsRetriever.Builder locationRetrieverBuilder = new TestingInputsLocationsRetriever.Builder();
		JobVertexID jobVertexID = new JobVertexID();
		for (int i = 0; i < ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER + 1; i++) {
			final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexID, i);
			locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
			producerIds.add(producerId);
		}

		final TestingInputsLocationsRetriever inputsLocationsRetriever = locationRetrieverBuilder.build();

		for (int i = 0; i < ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER + 1; i++) {
			inputsLocationsRetriever.markScheduled(producerIds.get(i));
		}

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(consumerId, inputsLocationsRetriever);

		assertThat(preferredLocations.get(), hasSize(0));
	}

	/**
	 * Tests that will choose the locations on the edge which has less different number.
	 */
	@Test
	public void testChooseLocationsOnEdgeWithLessDifferentNumber() throws Exception {
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		TestingInputsLocationsRetriever.Builder locationRetrieverBuilder = new TestingInputsLocationsRetriever.Builder();
		final JobVertexID jobVertexID1 = new JobVertexID();
		final JobVertexID jobVertexID2 = new JobVertexID();
		int parallelism1 = 3;
		int parallelism2 = 5;
		List<ExecutionVertexID> producers1 = new ArrayList<>(parallelism1);
		List<ExecutionVertexID> producers2 = new ArrayList<>(parallelism2);

		for (int i = 0; i < parallelism1; i++) {
			final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexID1, i);
			producers1.add(producerId);
			locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
		}

		for (int i = 0; i < parallelism2; i++) {
			final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexID2, i);
			producers2.add(producerId);
			locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
		}

		final TestingInputsLocationsRetriever inputsLocationsRetriever = locationRetrieverBuilder.build();

		List<TaskManagerLocation> expectedLocations = new ArrayList<>(parallelism1);
		for (int i = 0; i < parallelism1; i++) {
			inputsLocationsRetriever.assignTaskManagerLocation(producers1.get(i));
			expectedLocations.add(inputsLocationsRetriever.getTaskManagerLocation(producers1.get(i)).get().getNow(null));
		}

		for (int i = 0; i < parallelism2; i++) {
			inputsLocationsRetriever.assignTaskManagerLocation(producers2.get(i));
		}

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(consumerId, inputsLocationsRetriever);

		assertThat(preferredLocations.get(), containsInAnyOrder(expectedLocations.toArray()));
	}

}
