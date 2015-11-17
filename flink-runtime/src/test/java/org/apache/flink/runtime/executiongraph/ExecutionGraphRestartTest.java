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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExecutionGraphRestartTest {

	private final static int NUM_TASKS = 31;

	@Test
	public void testNotRestartManually() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutionContext(),
				new JobID(),
				"test job",
				new Configuration(),
				AkkaUtils.getDefaultTimeout());
		eg.setNumberOfRetriesLeft(0);
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		assertEquals(JobStatus.FAILED, eg.getState());

		eg.restart();

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	@Test
	public void testRestartAutomatically() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutionContext(),
				new JobID(),
				"Test job",
				new Configuration(),
				AkkaUtils.getDefaultTimeout());
		eg.setNumberOfRetriesLeft(1);
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, eg.getState());

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		// Wait for async restart
		Deadline deadline = timeout.fromNow();
		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.RUNNING) {
			Thread.sleep(100);
		}

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for deploying after async restart
		deadline = timeout.fromNow();
		boolean success = false;

		while (deadline.hasTimeLeft() && !success) {
			success = true;

			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				if (vertex.getCurrentExecutionAttempt().getAssignedResource() == null) {
					success = false;
					Thread.sleep(100);
					break;
				}
			}
		}

		if (deadline.hasTimeLeft()) {
			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				vertex.getCurrentExecutionAttempt().markFinished();
			}

			assertEquals(JobStatus.FINISHED, eg.getState());
		}
		else {
			fail("Failed to wait until all execution attempts left the state DEPLOYING.");
		}
	}
}
