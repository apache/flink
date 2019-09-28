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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.runtime.scheduler.strategy.StrategyTestUtil.getExecutionVertexIdsFromDeployOptions;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link EagerSchedulingStrategy}.
 */
public class EagerSchedulingStrategyTest extends TestLogger {

	private TestingSchedulerOperations testingSchedulerOperations;

	private TestingSchedulingTopology testingSchedulingTopology;

	private EagerSchedulingStrategy schedulingStrategy;

	@Before
	public void setUp() {
		testingSchedulerOperations = new TestingSchedulerOperations();
		testingSchedulingTopology = new TestingSchedulingTopology();
		schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperations,
				testingSchedulingTopology);
	}

	/**
	 * Tests that when start scheduling eager scheduling strategy will start all vertices in scheduling topology.
	 */
	@Test
	public void testStartScheduling() {
		JobVertexID jobVertexID = new JobVertexID();
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 4));

		schedulingStrategy.startScheduling();

		assertThat(testingSchedulerOperations.getScheduledVertices(), hasSize(1));

		Collection<ExecutionVertexDeploymentOption> scheduledVertices = testingSchedulerOperations.getScheduledVertices().get(0);
		Collection<ExecutionVertexID> scheduledVertexIDs = getExecutionVertexIdsFromDeployOptions(scheduledVertices);
		assertThat(scheduledVertexIDs, hasSize(5));
		for (SchedulingExecutionVertex schedulingExecutionVertex : testingSchedulingTopology.getVertices()) {
			assertThat(scheduledVertexIDs, hasItem(schedulingExecutionVertex.getId()));
		}
	}

	/**
	 * Tests that eager scheduling strategy will restart all vertices needing restarted at same time.
	 */
	@Test
	public void testRestartTasks() {
		JobVertexID jobVertexID = new JobVertexID();
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingExecutionVertex(new TestingSchedulingExecutionVertex(jobVertexID, 4));

		Set<ExecutionVertexID> verticesToRestart1 = new HashSet<>(Arrays.asList(
				new ExecutionVertexID(jobVertexID, 0),
				new ExecutionVertexID(jobVertexID, 4)));
		schedulingStrategy.restartTasks(verticesToRestart1);

		Set<ExecutionVertexID> verticesToRestart2 = new HashSet<>(Arrays.asList(
				new ExecutionVertexID(jobVertexID, 1),
				new ExecutionVertexID(jobVertexID, 2),
				new ExecutionVertexID(jobVertexID, 3)));
		schedulingStrategy.restartTasks(verticesToRestart2);

		assertThat(testingSchedulerOperations.getScheduledVertices(), hasSize(2));

		Collection<ExecutionVertexDeploymentOption> scheduledVertices1 = testingSchedulerOperations.getScheduledVertices().get(0);
		assertThat(getExecutionVertexIdsFromDeployOptions(scheduledVertices1), containsInAnyOrder(verticesToRestart1.toArray()));

		Collection<ExecutionVertexDeploymentOption> scheduledVertices2 = testingSchedulerOperations.getScheduledVertices().get(1);
		assertThat(getExecutionVertexIdsFromDeployOptions(scheduledVertices2), containsInAnyOrder(verticesToRestart2.toArray()));
	}
}
