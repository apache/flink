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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link EagerSchedulingStrategy}.
 */
public class EagerSchedulingStrategyTest extends TestLogger {

	/**
	 * Tests that when start scheduling eager scheduling strategy will start all vertices in scheduling topology.
	 */
	@Test
	public void testStartScheduling() {
		TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();
		JobVertexID jobVertexID = new JobVertexID();
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 4));

		TestingSchedulerOperations testingSchedulerOperations = new TestingSchedulerOperations();
		EagerSchedulingStrategy schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperations,
				testingSchedulingTopology);

		schedulingStrategy.startScheduling();

		assertEquals(1, testingSchedulerOperations.getScheduledVertices().size());

		Collection<ExecutionVertexDeploymentOption> scheduledVertices = testingSchedulerOperations.getScheduledVertices().get(0);
		assertEquals(5, scheduledVertices.size());
		for (SchedulingExecutionVertex schedulingExecutionVertex : testingSchedulingTopology.getVertices()) {
			assertThat(getExecutionVertexIdsFromDeployOptions(scheduledVertices), hasItem(schedulingExecutionVertex.getId()));
		}
	}

	/**
	 * Tests that eager scheduling strategy will restart all vertices needing restarted at same time.
	 */
	@Test
	public void testRestartTasks() {
		TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();
		JobVertexID jobVertexID = new JobVertexID();
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 4));

		TestingSchedulerOperations testingSchedulerOperations = new TestingSchedulerOperations();
		EagerSchedulingStrategy schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperations,
				testingSchedulingTopology);

		Set<ExecutionVertexID> toBeRestartedVertices1 = new HashSet<>();
		toBeRestartedVertices1.add(new ExecutionVertexID(jobVertexID, 0));
		toBeRestartedVertices1.add(new ExecutionVertexID(jobVertexID, 4));
		schedulingStrategy.restartTasks(toBeRestartedVertices1);

		assertEquals(1, testingSchedulerOperations.getScheduledVertices().size());
		Collection<ExecutionVertexDeploymentOption> scheduledVertices1 = testingSchedulerOperations.getScheduledVertices().get(0);
		assertEquals(2, scheduledVertices1.size());
		assertThat(getExecutionVertexIdsFromDeployOptions(scheduledVertices1), containsInAnyOrder(toBeRestartedVertices1.toArray()));

		Set<ExecutionVertexID> toBeRestartedVertices2 = new HashSet<>();
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 1));
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 2));
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 3));
		schedulingStrategy.restartTasks(toBeRestartedVertices2);

		assertEquals(2, testingSchedulerOperations.getScheduledVertices().size());
		Collection<ExecutionVertexDeploymentOption> scheduledVertices2 = testingSchedulerOperations.getScheduledVertices().get(1);
		assertEquals(3, scheduledVertices2.size());
		assertThat(getExecutionVertexIdsFromDeployOptions(scheduledVertices2), containsInAnyOrder(toBeRestartedVertices2.toArray()));
	}

	private Collection<ExecutionVertexID> getExecutionVertexIdsFromDeployOptions(
			Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		return StreamSupport
				.stream(deploymentOptions.spliterator(), false)
				.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
				.collect(Collectors.toList());
	}
}
