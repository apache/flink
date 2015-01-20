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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.*;

import static org.junit.Assert.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.TaskManagerMessages.TaskOperationResult;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutionVertexDeploymentTest {
	private static ActorSystem system;

	@BeforeClass
	public static void setup(){
		system = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown(){
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testDeployCall() {
		try {
			final JobVertexID jid = new JobVertexID();

			TestingUtils.setCallingThreadDispatcher(system);
			ActorRef tm = TestActorRef.create(system, Props.create(SimpleAcknowledgingTaskManager
					.class));
			
			// mock taskmanager to simply accept the call
			Instance instance = getInstance(tm);

			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
	
	@Test
	public void testDeployWithSynchronousAnswer() {
		try {
			TestingUtils.setCallingThreadDispatcher(system);

			final JobVertexID jid = new JobVertexID();

			final TestActorRef simpleTaskManager = TestActorRef.create(system,
					Props.create(SimpleAcknowledgingTaskManager.class));
			
			final Instance instance = getInstance(simpleTaskManager);

			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
	
	@Test
	public void testDeployWithAsynchronousAnswer() {
		try {
			final JobVertexID jid = new JobVertexID();

			final TestActorRef simpleTaskManager = TestActorRef.create(system,
					Props.create(SimpleAcknowledgingTaskManager.class));
			
			final Instance instance = getInstance(simpleTaskManager);

			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() != ExecutionState.RUNNING) {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployFailedSynchronous() {
		try {
			TestingUtils.setCallingThreadDispatcher(system);

			final JobVertexID jid = new JobVertexID();

			final TestActorRef simpleTaskManager = TestActorRef.create(system,
					Props.create(SimpleFailingTaskManager.class));
			
			final Instance instance = getInstance(simpleTaskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
	
	@Test
	public void testDeployFailedAsynchronously() {
		try {
			final JobVertexID jid = new JobVertexID();

			final TestActorRef simpleTaskManager = TestActorRef.create(system,
					Props.create(SimpleFailingTaskManager.class));
			
			final Instance instance = getInstance(simpleTaskManager);

			final AllocatedSlot slot = instance.allocateSlot(new JobID());

			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() == ExecutionState.FAILED && vertex.getFailureCause() != null) {
					break;
				} else {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFailExternallyDuringDeploy() {
		try {
			final JobVertexID jid = new JobVertexID();

			final ActionQueue queue = new ActionQueue();
			final TestingUtils.QueuedActionExecutionContext ec = new TestingUtils
					.QueuedActionExecutionContext(queue);

			TestingUtils.setExecutionContext(ec);

			final TestActorRef simpleTaskManager = TestActorRef.create(system,
					Props.create(SimpleAcknowledgingTaskManager.class));

			final Instance instance = getInstance(simpleTaskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());

			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			Exception testError = new Exception("test error");
			vertex.fail(testError);

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertEquals(testError, vertex.getFailureCause());

			queue.triggerNextAction();

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
	
	@Test
	public void testFailCallOvertakesDeploymentAnswer() {
		
		try {
			ActionQueue queue = new ActionQueue();
			TestingUtils.QueuedActionExecutionContext context = new TestingUtils
					.QueuedActionExecutionContext(queue);

			TestingUtils.setExecutionContext(context);

			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());
			final ExecutionAttemptID eid = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TestActorRef simpleTaskManager = TestActorRef.create(system, Props.create(new
					ExecutionVertexCancelTest.CancelSequenceTaskManagerCreator(new
					TaskOperationResult(eid, false), new TaskOperationResult(eid, true))));

			final Instance instance = getInstance(simpleTaskManager);

			final AllocatedSlot slot = instance.allocateSlot(new JobID());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			Exception testError = new Exception("test error");
			vertex.fail(testError);
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());

			// cancel call overtakes deploy call
			Runnable deploy = queue.popNextAction();
			Runnable cancel1 = queue.popNextAction();

			cancel1.run();
			// execute onComplete callback
			queue.triggerNextAction();

			deploy.run();

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());

			// should have sent another cancel call
			queue.triggerNextAction();
			// execute onComplete callback
			queue.triggerNextAction();

			assertEquals(testError, vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);

			assertTrue(queue.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
}
