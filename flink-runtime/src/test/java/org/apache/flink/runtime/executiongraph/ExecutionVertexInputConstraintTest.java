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

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.isInExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitForAllExecutionsPredicate;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionVertexState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the inputs constraint for {@link ExecutionVertex}.
 */
public class ExecutionVertexInputConstraintTest extends TestLogger {

	private ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	@Test
	public void testInputConsumable() throws Exception {
		List<JobVertex> vertices = createOrderedVertices();
		ExecutionGraph eg = createExecutionGraph(vertices, InputDependencyConstraint.ALL);
		ExecutionVertex ev11 = eg.getJobVertex(vertices.get(0).getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[0];
		ExecutionVertex ev22 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[1];
		ExecutionVertex ev31 = eg.getJobVertex(vertices.get(2).getID()).getTaskVertices()[0];
		ExecutionVertex ev32 = eg.getJobVertex(vertices.get(2).getID()).getTaskVertices()[1];

		eg.start(mainThreadExecutor);

		eg.scheduleForExecution();

		// Inputs not consumable on init
		assertFalse(ev31.isInputConsumable(0));
		assertFalse(ev31.isInputConsumable(1));

		// One pipelined input consumable on data produced
		IntermediateResultPartition partition11 = ev11.getProducedPartitions().values().iterator().next();
		ev11.scheduleOrUpdateConsumers(new ResultPartitionID(partition11.getPartitionId(),
			ev11.getCurrentExecutionAttempt().getAttemptId()));
		assertTrue(ev31.isInputConsumable(0));
		// Input0 of ev32 is not consumable. It consumes the same PIPELINED result with ev31 but not the same partition
		assertFalse(ev32.isInputConsumable(0));

		// The blocking input not consumable if only one partition is FINISHED
		ev21.getCurrentExecutionAttempt().markFinished();
		assertFalse(ev31.isInputConsumable(1));

		// The blocking input consumable if all partitions are FINISHED
		ev22.getCurrentExecutionAttempt().markFinished();
		assertTrue(ev31.isInputConsumable(1));

		// Inputs not consumable after failover
		ev11.fail(new Exception());

		waitUntilJobRestarted(eg);
		assertFalse(ev31.isInputConsumable(0));
		assertFalse(ev31.isInputConsumable(1));
	}

	@Test
	public void testInputConstraintANY() throws Exception {
		List<JobVertex> vertices = createOrderedVertices();
		ExecutionGraph eg = createExecutionGraph(vertices, InputDependencyConstraint.ANY);
		ExecutionVertex ev11 = eg.getJobVertex(vertices.get(0).getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[0];
		ExecutionVertex ev22 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[1];
		ExecutionVertex ev31 = eg.getJobVertex(vertices.get(2).getID()).getTaskVertices()[0];

		eg.start(mainThreadExecutor);
		eg.scheduleForExecution();


		// Inputs constraint not satisfied on init
		assertFalse(ev31.checkInputDependencyConstraints());

		// Input1 consumable satisfies the constraint
		IntermediateResultPartition partition11 = ev11.getProducedPartitions().values().iterator().next();
		ev11.scheduleOrUpdateConsumers(new ResultPartitionID(partition11.getPartitionId(),
			ev11.getCurrentExecutionAttempt().getAttemptId()));
		assertTrue(ev31.checkInputDependencyConstraints());

		// Inputs constraint not satisfied after failover
		ev11.fail(new Exception());


		waitUntilJobRestarted(eg);


		assertFalse(ev31.checkInputDependencyConstraints());

		// Input2 consumable satisfies the constraint
		waitUntilExecutionVertexState(ev21, ExecutionState.DEPLOYING, 2000L);
		waitUntilExecutionVertexState(ev22, ExecutionState.DEPLOYING, 2000L);
		ev21.getCurrentExecutionAttempt().markFinished();
		ev22.getCurrentExecutionAttempt().markFinished();
		assertTrue(ev31.checkInputDependencyConstraints());

	}

	@Test
	public void testInputConstraintALL() throws Exception {
		List<JobVertex> vertices = createOrderedVertices();
		ExecutionGraph eg = createExecutionGraph(vertices, InputDependencyConstraint.ALL);
		ExecutionVertex ev11 = eg.getJobVertex(vertices.get(0).getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[0];
		ExecutionVertex ev22 = eg.getJobVertex(vertices.get(1).getID()).getTaskVertices()[1];
		ExecutionVertex ev31 = eg.getJobVertex(vertices.get(2).getID()).getTaskVertices()[0];

		eg.start(mainThreadExecutor);
		eg.scheduleForExecution();


		// Inputs constraint not satisfied on init
		assertFalse(ev31.checkInputDependencyConstraints());

		// Input1 consumable does not satisfy the constraint
		IntermediateResultPartition partition11 = ev11.getProducedPartitions().values().iterator().next();
		ev11.scheduleOrUpdateConsumers(new ResultPartitionID(partition11.getPartitionId(),
			ev11.getCurrentExecutionAttempt().getAttemptId()));
		assertFalse(ev31.checkInputDependencyConstraints());

		// Input2 consumable satisfies the constraint
		ev21.getCurrentExecutionAttempt().markFinished();
		ev22.getCurrentExecutionAttempt().markFinished();
		assertTrue(ev31.checkInputDependencyConstraints());

		// Inputs constraint not satisfied after failover
		ev11.fail(new Exception());


		waitUntilJobRestarted(eg);

		assertFalse(ev31.checkInputDependencyConstraints());
	}

	private static List<JobVertex> createOrderedVertices() {
		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		return Arrays.asList(v1, v2, v3);
	}

	private static ExecutionGraph createExecutionGraph(
			List<JobVertex> orderedVertices,
			InputDependencyConstraint inputDependencyConstraint) throws Exception {

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";
		final SlotProvider slotProvider = new SimpleSlotProvider(jobId, 20);

		for (JobVertex vertex : orderedVertices) {
			vertex.setInputDependencyConstraint(inputDependencyConstraint);
		}

		ExecutionGraph eg = new ExecutionGraph(
			new DummyJobInformation(
				jobId,
				jobName),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			TestRestartStrategy.directExecuting(),
			new RestartAllStrategy.Factory(),
			slotProvider);
		eg.attachJobGraph(orderedVertices);

		return eg;
	}

	private void waitUntilJobRestarted(ExecutionGraph eg) throws Exception {
		waitForAllExecutionsPredicate(eg,
			isInExecutionState(ExecutionState.CANCELING)
				.or(isInExecutionState(ExecutionState.CANCELED))
				.or(isInExecutionState(ExecutionState.FAILED))
				.or(isInExecutionState(ExecutionState.FINISHED)),
			2000L);

		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			if (ev.getCurrentExecutionAttempt().getState() == ExecutionState.CANCELING) {
				ev.getCurrentExecutionAttempt().completeCancelling();
			}
		}

		waitUntilJobStatus(eg, JobStatus.RUNNING, 2000L);
	}
}
