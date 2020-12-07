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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestBase;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.util.FlinkException;

import org.junit.Test;

import java.util.function.Predicate;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Additional {@link ExecutionGraph} restart tests {@link ExecutionGraphRestartTest} which
 * require the usage of a {@link SlotProvider}.
 */
public class ExecutionGraphCoLocationRestartTest extends SchedulerTestBase {

	private static final int NUM_TASKS = 31;

	@Override
	protected ComponentMainThreadExecutor getComponentMainThreadExecutor() {
		return ComponentMainThreadExecutorServiceAdapter.forMainThread();
	}

	@Test
	public void testConstraintsAfterRestart() throws Exception {

		final long timeout = 5000L;

		//setting up
		testingSlotProvider.addTaskManager(NUM_TASKS);

		JobVertex groupVertex = ExecutionGraphTestUtils.createNoOpVertex(NUM_TASKS);
		JobVertex groupVertex2 = ExecutionGraphTestUtils.createNoOpVertex(NUM_TASKS);
		groupVertex2.connectNewDataSetAsInput(groupVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		groupVertex.setSlotSharingGroup(sharingGroup);
		groupVertex2.setSlotSharingGroup(sharingGroup);
		groupVertex.setStrictlyCoLocatedWith(groupVertex2);

		//initiate and schedule job
		final JobGraph jobGraph = new JobGraph(groupVertex, groupVertex2);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		final ExecutionGraph eg = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jobGraph)
			.setSlotProvider(testingSlotProvider)
			.setRestartStrategy(new TestRestartStrategy(1, false))
			.build();

		// enable the queued scheduling for the slot pool
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();

		Predicate<AccessExecution> isDeploying = ExecutionGraphTestUtils.isInExecutionState(ExecutionState.DEPLOYING);
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
			eg,
			isDeploying,
			timeout);

		assertEquals(JobStatus.RUNNING, eg.getState());

		//sanity checks
		validateConstraints(eg);

		eg.getAllExecutionVertices().iterator().next().fail(new FlinkException("Test exception"));

		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().completeCancelling();
		}

		// wait until we have restarted
		ExecutionGraphTestUtils.waitUntilJobStatus(eg, JobStatus.RUNNING, timeout);

		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
			eg,
			isDeploying,
			timeout);

		//checking execution vertex properties
		validateConstraints(eg);

		ExecutionGraphTestUtils.finishAllVertices(eg);

		assertThat(eg.getState(), is(FINISHED));
	}

	private void validateConstraints(ExecutionGraph eg) {

		ExecutionJobVertex[] tasks = eg.getAllVertices().values().toArray(new ExecutionJobVertex[2]);

		for (int i = 0; i < NUM_TASKS; i++) {
			CoLocationConstraint constr1 = tasks[0].getTaskVertices()[i].getLocationConstraint();
			CoLocationConstraint constr2 = tasks[1].getTaskVertices()[i].getLocationConstraint();
			assertThat(constr1.isAssigned(), is(true));
			assertThat(constr1.getLocation(), equalTo(constr2.getLocation()));
		}
	}
}
