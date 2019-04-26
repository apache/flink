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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

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
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 4));

		TestingSchedulerOperation testingSchedulerOperation = new TestingSchedulerOperation();
		EagerSchedulingStrategy schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperation,
				testingSchedulingTopology,
				null);

		schedulingStrategy.startScheduling();

		assertEquals(5, testingSchedulerOperation.getScheduledVertices().size());
	}

	/**
	 * Tests that eager scheduling strategy will restart all vertices needing restarted at same time.
	 */
	@Test
	public void testRestartTasks() {
		TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();
		JobVertexID jobVertexID = new JobVertexID();
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 0));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 1));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 2));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingVertex(new TestingSchedulingVertex(jobVertexID, 4));

		TestingSchedulerOperation testingSchedulerOperation = new TestingSchedulerOperation();
		EagerSchedulingStrategy schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperation,
				testingSchedulingTopology,
				null);

		Set<ExecutionVertexID> toBeRestartedVertices1 = new HashSet<>(2);
		toBeRestartedVertices1.add(new ExecutionVertexID(jobVertexID, 0));
		toBeRestartedVertices1.add(new ExecutionVertexID(jobVertexID, 4));
		schedulingStrategy.restartTasks(toBeRestartedVertices1);

		assertEquals(2, testingSchedulerOperation.getScheduledVertices().size());

		Set<ExecutionVertexID> toBeRestartedVertices2 = new HashSet<>(2);
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 1));
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 2));
		toBeRestartedVertices2.add(new ExecutionVertexID(jobVertexID, 3));
		schedulingStrategy.restartTasks(toBeRestartedVertices2);

		assertEquals(5, testingSchedulerOperation.getScheduledVertices().size());
	}
}
