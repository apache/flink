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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionGraphDeploymentTest {

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testBuildDeploymentDescriptor() {
		try {
			TestingUtils.setCallingThreadDispatcher(system);
			final JobID jobId = new JobID();

			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();
			final JobVertexID jid3 = new JobVertexID();
			final JobVertexID jid4 = new JobVertexID();

			AbstractJobVertex v1 = new AbstractJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);
			AbstractJobVertex v3 = new AbstractJobVertex("v3", jid3);
			AbstractJobVertex v4 = new AbstractJobVertex("v4", jid4);

			v1.setParallelism(10);
			v2.setParallelism(10);
			v3.setParallelism(10);
			v4.setParallelism(10);

			v1.setInvokableClass(RegularPactTask.class);
			v2.setInvokableClass(RegularPactTask.class);
			v3.setInvokableClass(RegularPactTask.class);
			v4.setInvokableClass(RegularPactTask.class);

			v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL);
			v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL);
			v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL);

			ExecutionGraph eg = new ExecutionGraph(jobId, "some job", new Configuration(),
					AkkaUtils.getDefaultTimeout());

			List<AbstractJobVertex> ordered = Arrays.asList(v1, v2, v3, v4);

			eg.attachJobGraph(ordered);

			ExecutionJobVertex ejv = eg.getAllVertices().get(jid2);
			ExecutionVertex vertex = ejv.getTaskVertices()[3];

			// create synchronous task manager
			final TestActorRef<? extends Actor> simpleTaskManager = TestActorRef.create(system,
					Props.create(ExecutionGraphTestUtils
							.SimpleAcknowledgingTaskManager.class));

			ExecutionGraphTestUtils.SimpleAcknowledgingTaskManager tm = (ExecutionGraphTestUtils
					.SimpleAcknowledgingTaskManager) simpleTaskManager.underlyingActor();

			final Instance instance = getInstance(simpleTaskManager);

			final SimpleSlot slot = instance.allocateSimpleSlot(jobId);

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			TaskDeploymentDescriptor descr = tm.lastTDD;
			assertNotNull(descr);

			assertEquals(jobId, descr.getJobID());
			assertEquals(jid2, descr.getVertexID());
			assertEquals(3, descr.getIndexInSubtaskGroup());
			assertEquals(10, descr.getNumberOfSubtasks());
			assertEquals(RegularPactTask.class.getName(), descr.getInvokableClassName());
			assertEquals("v2", descr.getTaskName());

			List<ResultPartitionDeploymentDescriptor> producedPartitions = descr.getProducedPartitions();
			List<InputGateDeploymentDescriptor> consumedPartitions = descr.getInputGates();

			assertEquals(2, producedPartitions.size());
			assertEquals(1, consumedPartitions.size());

			assertEquals(10, producedPartitions.get(0).getNumberOfSubpartitions());
			assertEquals(10, producedPartitions.get(1).getNumberOfSubpartitions());
			assertEquals(10, consumedPartitions.get(0).getInputChannelDeploymentDescriptors().length);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			TestingUtils.setGlobalExecutionContext();
		}
	}

	@Test
	public void testRegistrationOfExecutionsFinishing() {
		try {
			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();

			AbstractJobVertex v1 = new AbstractJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);

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

			AbstractJobVertex v1 = new AbstractJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);

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

			AbstractJobVertex v1 = new AbstractJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);

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

			AbstractJobVertex v1 = new AbstractJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);

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

			AbstractJobVertex v1 = new FailingFinalizeJobVertex("v1", jid1);
			AbstractJobVertex v2 = new AbstractJobVertex("v2", jid2);

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

	private Map<ExecutionAttemptID, Execution> setupExecution(AbstractJobVertex v1, int dop1, AbstractJobVertex v2, int dop2) throws Exception {
		final JobID jobId = new JobID();

		v1.setParallelism(dop1);
		v2.setParallelism(dop2);

		v1.setInvokableClass(RegularPactTask.class);
		v2.setInvokableClass(RegularPactTask.class);

		// execution graph that executes actions synchronously
		ExecutionGraph eg = new ExecutionGraph(jobId, "some job", new Configuration(),
				AkkaUtils.getDefaultTimeout());
		eg.setQueuedSchedulingAllowed(false);

		List<AbstractJobVertex> ordered = Arrays.asList(v1, v2);
		eg.attachJobGraph(ordered);

		// create a mock taskmanager that accepts deployment calls
		ActorRef tm = system.actorOf(Props.create(ExecutionGraphTestUtils.SimpleAcknowledgingTaskManager.class));

		Scheduler scheduler = new Scheduler();
		for (int i = 0; i < dop1 + dop2; i++) {
			scheduler.newInstanceAvailable(ExecutionGraphTestUtils.getInstance(tm));
		}
		assertEquals(dop1 + dop2, scheduler.getNumberOfAvailableSlots());

		// schedule, this triggers mock deployment
		eg.scheduleForExecution(scheduler);

		Map<ExecutionAttemptID, Execution> executions = eg.getRegisteredExecutions();
		assertEquals(dop1 + dop2, executions.size());

		return executions;
	}

	@SuppressWarnings("serial")
	public static class FailingFinalizeJobVertex extends AbstractJobVertex {

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