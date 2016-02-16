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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.messages.TaskMessages.TaskOperationResult;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import scala.concurrent.ExecutionContext;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExecutionVertex.class)
public class ExecutionVertexStopTest extends TestLogger {

	private static ActorSystem system;

	private static boolean receivedStopSignal;

	@AfterClass
	public static void teardown(){
		if(system != null) {
			JavaTestKit.shutdownActorSystem(system);
			system = null;
		}
	}

	@Test
	public void testStop() throws Exception {
		final JobVertexID jid = new JobVertexID();
		final ExecutionJobVertex ejv = getExecutionVertex(jid);

		Execution executionMock = mock(Execution.class);
		whenNew(Execution.class).withAnyArguments().thenReturn(executionMock);

		final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());

		vertex.stop();

		verify(executionMock).stop();
	}

	@Test
	public void testStopRpc() throws Exception {
		final JobVertexID jid = new JobVertexID();
		final ExecutionJobVertex ejv = getExecutionVertex(jid);

		final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
				AkkaUtils.getDefaultTimeout());
		final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

		setVertexState(vertex, ExecutionState.SCHEDULED);
		assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());

		final ActorGateway gateway = new StopSequenceInstanceGateway(
				TestingUtils.defaultExecutionContext(), new TaskOperationResult(execId, true));

		Instance instance = getInstance(gateway);
		SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

		vertex.deployToSlot(slot);

		receivedStopSignal = false;
		vertex.stop();
		assertTrue(receivedStopSignal);
	}

	public static class StopSequenceInstanceGateway extends BaseTestingActorGateway {
		private static final long serialVersionUID = 7611571264006653627L;

		private final TaskOperationResult result;

		public StopSequenceInstanceGateway(ExecutionContext executionContext, TaskOperationResult result) {
			super(executionContext);
			this.result = result;
		}

		@Override
		public Object handleMessage(Object message) throws Exception {
			Object result = null;
			if (message instanceof TaskMessages.SubmitTask) {
				result = Messages.getAcknowledge();
			} else if (message instanceof TaskMessages.StopTask) {
				result = this.result;
				receivedStopSignal = true;
			}

			return result;
		}
	}

}
