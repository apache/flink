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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.net.InetAddress;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.messages.TaskMessages.FailIntermediateResultPartitions;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.messages.TaskMessages.TaskOperationResult;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.concurrent.ExecutionContext;

public class ExecutionGraphTestUtils {

	// --------------------------------------------------------------------------------------------
	//  state modifications
	// --------------------------------------------------------------------------------------------
	
	public static void setVertexState(ExecutionVertex vertex, ExecutionState state) {
		try {
			Execution exec = vertex.getCurrentExecutionAttempt();
			
			Field f = Execution.class.getDeclaredField("state");
			f.setAccessible(true);
			f.set(exec, state);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the state failed", e);
		}
	}
	
	public static void setVertexResource(ExecutionVertex vertex, SimpleSlot slot) {
		try {
			Execution exec = vertex.getCurrentExecutionAttempt();
			
			Field f = Execution.class.getDeclaredField("assignedResource");
			f.setAccessible(true);
			f.set(exec, slot);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the slot failed", e);
		}
	}
	
	public static void setGraphStatus(ExecutionGraph graph, JobStatus status) {
		try {
			Field f = ExecutionGraph.class.getDeclaredField("state");
			f.setAccessible(true);
			f.set(graph, status);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the status failed", e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  utility mocking methods
	// --------------------------------------------------------------------------------------------

	public static Instance getInstance(final ActorGateway gateway) throws Exception {
		return getInstance(gateway, 1);
	}

	public static Instance getInstance(final ActorGateway gateway, final int numberOfSlots) throws Exception {
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10001);

		return new Instance(gateway, connection, new InstanceID(), hardwareDescription, numberOfSlots);
	}

	@SuppressWarnings("serial")
	public static class SimpleActorGateway extends BaseTestingActorGateway {
		
		public TaskDeploymentDescriptor lastTDD;

		public SimpleActorGateway(ExecutionContext executionContext){
			super(executionContext);
		}

		@Override
		public Object handleMessage(Object message) {
			Object result = null;
			if(message instanceof SubmitTask) {
				SubmitTask submitTask = (SubmitTask) message;
				lastTDD = submitTask.tasks();

				result = Messages.getAcknowledge();
			} else if(message instanceof CancelTask) {
				CancelTask cancelTask = (CancelTask) message;

				result = new TaskOperationResult(cancelTask.attemptID(), true);
			} else if(message instanceof FailIntermediateResultPartitions) {
				result = new Object();
			}

			return result;
		}
	}

	@SuppressWarnings("serial")
	public static class SimpleFailingActorGateway extends BaseTestingActorGateway {

		public SimpleFailingActorGateway(ExecutionContext executionContext) {
			super(executionContext);
		}

		@Override
		public Object handleMessage(Object message) throws Exception {
			if(message instanceof SubmitTask) {
				throw new Exception(ERROR_MESSAGE);
			} else if (message instanceof CancelTask) {
				CancelTask cancelTask = (CancelTask) message;

				return new TaskOperationResult(cancelTask.attemptID(), true);
			} else {
				return null;
			}
		}
	}

	public static final String ERROR_MESSAGE = "test_failure_error_message";

	public static ExecutionJobVertex getExecutionVertex(JobVertexID id, ExecutionContext executionContext) throws JobException {
		JobVertex ajv = new JobVertex("TestVertex", id);
		ajv.setInvokableClass(mock(AbstractInvokable.class).getClass());

		ExecutionGraph graph = new ExecutionGraph(
				executionContext,
				new JobID(),
				"test job",
				new Configuration(),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy());

		ExecutionJobVertex ejv = spy(new ExecutionJobVertex(graph, ajv, 1,
				AkkaUtils.getDefaultTimeout()));

		Answer<Void> noop = new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				return null;
			}
		};

		doAnswer(noop).when(ejv).vertexCancelled(Matchers.anyInt());
		doAnswer(noop).when(ejv).vertexFailed(Matchers.anyInt(), Matchers.any(Throwable.class));
		doAnswer(noop).when(ejv).vertexFinished(Matchers.anyInt());

		return ejv;
	}
	
	public static ExecutionJobVertex getExecutionVertex(JobVertexID id) throws JobException {
		return getExecutionVertex(id, TestingUtils.defaultExecutionContext());
	}
}
