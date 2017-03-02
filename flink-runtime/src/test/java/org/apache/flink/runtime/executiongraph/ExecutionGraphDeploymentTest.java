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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

public class ExecutionGraphDeploymentTest {

	@Test
	public void testBuildDeploymentDescriptor() {
		try {
			final JobID jobId = new JobID();

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();
			final JobVertexID jid3 = new JobVertexID();
			final JobVertexID jid4 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);
			JobVertex v3 = new JobVertex("v3", jid3);
			JobVertex v4 = new JobVertex("v4", jid4);

			v1.setParallelism(10);
			v2.setParallelism(10);
			v3.setParallelism(10);
			v4.setParallelism(10);

			v1.setInvokableClass(BatchTask.class);
			v2.setInvokableClass(BatchTask.class);
			v3.setInvokableClass(BatchTask.class);
			v4.setInvokableClass(BatchTask.class);

			v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL);
			v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL);
			v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL);

			ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				jobId, 
				"some job", 
				new Configuration(),
				new SerializedValue<>(new ExecutionConfig()),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy(),
				new Scheduler(TestingUtils.defaultExecutionContext()));

			List<JobVertex> ordered = Arrays.asList(v1, v2, v3, v4);

			eg.attachJobGraph(ordered);

			ExecutionJobVertex ejv = eg.getAllVertices().get(jid2);
			ExecutionVertex vertex = ejv.getTaskVertices()[3];

			ExecutionGraphTestUtils.SimpleActorGateway instanceGateway = new ExecutionGraphTestUtils.SimpleActorGateway(TestingUtils.directExecutionContext());

			final Instance instance = getInstance(new ActorTaskManagerGateway(instanceGateway));

			final SimpleSlot slot = instance.allocateSimpleSlot(jobId);

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			TaskDeploymentDescriptor descr = instanceGateway.lastTDD;
			assertNotNull(descr);

			JobInformation jobInformation = descr.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
			TaskInformation taskInformation = descr.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());

			assertEquals(jobId, jobInformation.getJobId());
			assertEquals(jid2, taskInformation.getJobVertexId());
			assertEquals(3, descr.getSubtaskIndex());
			assertEquals(10, taskInformation.getNumberOfSubtasks());
			assertEquals(BatchTask.class.getName(), taskInformation.getInvokableClassName());
			assertEquals("v2", taskInformation.getTaskName());

			Collection<ResultPartitionDeploymentDescriptor> producedPartitions = descr.getProducedPartitions();
			Collection<InputGateDeploymentDescriptor> consumedPartitions = descr.getInputGates();

			assertEquals(2, producedPartitions.size());
			assertEquals(1, consumedPartitions.size());

			Iterator<ResultPartitionDeploymentDescriptor> iteratorProducedPartitions = producedPartitions.iterator();
			Iterator<InputGateDeploymentDescriptor> iteratorConsumedPartitions = consumedPartitions.iterator();

			assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
			assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
			assertEquals(10, iteratorConsumedPartitions.next().getInputChannelDeploymentDescriptors().length);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFinishing() {
		try {
			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7650, v2, 2350);

			for (Execution e : executions.values()) {
				e.markFinished();
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFailing() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7, v2, 6);

			for (Execution e : executions.values()) {
				e.markFailed(null);
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFailedExternally() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 7, v2, 6);

			for (Execution e : executions.values()) {
				e.fail(null);
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsCanceled() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new JobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 19, v2, 37);

			for (Execution e : executions.values()) {
				e.cancel();
				e.cancelingComplete();
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRegistrationOfExecutionsFailingFinalize() {
		try {

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			JobVertex v1 = new FailingFinalizeJobVertex("v1", jid1);
			JobVertex v2 = new JobVertex("v2", jid2);

			Map<ExecutionAttemptID, Execution> executions = setupExecution(v1, 6, v2, 4);

			List<Execution> execList = new ArrayList<Execution>();
			execList.addAll(executions.values());
			// sort executions by job vertex. Failing job vertex first
			Collections.sort(execList, new Comparator<Execution>() {
				@Override
				public int compare(Execution o1, Execution o2) {
					return o1.getVertex().getSimpleName().compareTo(o2.getVertex().getSimpleName());
				}
			});

			int cnt = 0;
			for (Execution e : execList) {
				cnt++;
				e.markFinished();
				if (cnt <= 6) {
					// the last execution of the first job vertex triggers the failing finalize hook
					assertEquals(ExecutionState.FINISHED, e.getState());
				}
				else {
					// all following executions should be canceled
					assertEquals(ExecutionState.CANCELED, e.getState());
				}
			}

			assertEquals(0, executions.size());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	/**
	 * Tests that a blocking batch job fails if there are not enough resources left to schedule the
	 * succeeding tasks. This test case is related to [FLINK-4296] where finished producing tasks
	 * swallow the fail exception when scheduling a consumer task.
	 */
	public void testNoResourceAvailableFailure() throws Exception {
		final JobID jobId = new JobID();
		JobVertex v1 = new JobVertex("source");
		JobVertex v2 = new JobVertex("sink");

		int dop1 = 1;
		int dop2 = 1;

		v1.setParallelism(dop1);
		v2.setParallelism(dop2);

		v1.setInvokableClass(BatchTask.class);
		v2.setInvokableClass(BatchTask.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

		Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());
		for (int i = 0; i < dop1; i++) {
			scheduler.newInstanceAvailable(
				ExecutionGraphTestUtils.getInstance(
					new ActorTaskManagerGateway(
						new ExecutionGraphTestUtils.SimpleActorGateway(
							TestingUtils.directExecutionContext()))));
		}

		// execution graph that executes actions synchronously
		ExecutionGraph eg = new ExecutionGraph(
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			jobId,
			"failing test job",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			scheduler);

		eg.setQueuedSchedulingAllowed(false);

		List<JobVertex> ordered = Arrays.asList(v1, v2);
		eg.attachJobGraph(ordered);

		assertEquals(dop1, scheduler.getNumberOfAvailableSlots());

		// schedule, this triggers mock deployment
		eg.scheduleForExecution();

		ExecutionAttemptID attemptID = eg.getJobVertex(v1.getID()).getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId();
		eg.updateState(new TaskExecutionState(jobId, attemptID, ExecutionState.RUNNING));
		eg.updateState(new TaskExecutionState(jobId, attemptID, ExecutionState.FINISHED, null));

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	private Map<ExecutionAttemptID, Execution> setupExecution(JobVertex v1, int dop1, JobVertex v2, int dop2) throws Exception {
		final JobID jobId = new JobID();

		v1.setParallelism(dop1);
		v2.setParallelism(dop2);

		v1.setInvokableClass(BatchTask.class);
		v2.setInvokableClass(BatchTask.class);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		for (int i = 0; i < dop1 + dop2; i++) {
			scheduler.newInstanceAvailable(
				ExecutionGraphTestUtils.getInstance(
					new ActorTaskManagerGateway(
						new ExecutionGraphTestUtils.SimpleActorGateway(
							TestingUtils.directExecutionContext()))));
		}

		// execution graph that executes actions synchronously
		ExecutionGraph eg = new ExecutionGraph(
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			jobId, 
			"some job", 
			new Configuration(), 
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			scheduler);
		
		eg.setQueuedSchedulingAllowed(false);

		List<JobVertex> ordered = Arrays.asList(v1, v2);
		eg.attachJobGraph(ordered);

		assertEquals(dop1 + dop2, scheduler.getNumberOfAvailableSlots());

		// schedule, this triggers mock deployment
		eg.scheduleForExecution();

		Map<ExecutionAttemptID, Execution> executions = eg.getRegisteredExecutions();
		assertEquals(dop1 + dop2, executions.size());

		return executions;
	}

	@SuppressWarnings("serial")
	public static class FailingFinalizeJobVertex extends JobVertex {

		public FailingFinalizeJobVertex(String name) {
			super(name);
		}

		public FailingFinalizeJobVertex(String name, JobVertexID id) {
			super(name, id);
		}

		@Override
		public void finalizeOnMaster(ClassLoader cl) throws Exception {
			throw new Exception();
		}
	}
}
