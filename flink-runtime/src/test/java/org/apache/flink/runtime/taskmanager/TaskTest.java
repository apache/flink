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

package org.apache.flink.runtime.taskmanager;

import akka.actor.ActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.MockNetworkEnvironment;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskTest {

	@Test
	public void testTaskStates() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			
			final RuntimeEnvironment env = mock(RuntimeEnvironment.class);
			
			Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();
			task.setEnvironment(env);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			// cancel
			task.cancelExecution();
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			
			// cannot go into running or finished state
			
			assertFalse(task.startExecution());
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			
			assertFalse(task.markAsFinished());
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			
			task.markFailed(new Exception("test"));
			assertTrue(ExecutionState.CANCELED == task.getExecutionState());

			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTaskStartFinish() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			
			final Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();
			
			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
			
			Thread operation = new Thread() {
				@Override
				public void run() {
					try {
						assertTrue(task.markAsFinished());
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			
			final RuntimeEnvironment env = mock(RuntimeEnvironment.class);
			when(env.getExecutingThread()).thenReturn(operation);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			// start the execution
			task.setEnvironment(env);
			task.startExecution();
			
			// wait for the execution to be finished
			operation.join();
			
			if (error.get() != null) {
				ExceptionUtils.rethrow(error.get());
			}
			
			assertEquals(ExecutionState.FINISHED, task.getExecutionState());

			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTaskFailesInRunning() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			
			final Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();

			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
			
			Thread operation = new Thread() {
				@Override
				public void run() {
					try {
						task.markFailed(new Exception("test exception message"));
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			
			final RuntimeEnvironment env = mock(RuntimeEnvironment.class);
			when(env.getExecutingThread()).thenReturn(operation);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			// start the execution
			task.setEnvironment(env);
			task.startExecution();
			
			// wait for the execution to be finished
			operation.join();
			
			if (error.get() != null) {
				ExceptionUtils.rethrow(error.get());
			}
			
			// make sure the final state is correct and the task manager knows the changes
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTaskCanceledInRunning() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();

			final Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();
			
			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
			
			// latches to create a deterministic order of events
			final OneShotLatch toRunning = new OneShotLatch();
			final OneShotLatch afterCanceling = new OneShotLatch();
			
			Thread operation = new Thread() {
				@Override
				public void run() {
					try {
						toRunning.trigger();
						afterCanceling.await();
						assertFalse(task.markAsFinished());
						task.cancelingDone();
					}
					catch (Throwable t) {
						error.set(t);
					}
				}
			};
			
			final RuntimeEnvironment env = mock(RuntimeEnvironment.class);
			when(env.getExecutingThread()).thenReturn(operation);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			// start the execution
			task.setEnvironment(env);
			task.startExecution();
			
			toRunning.await();
			task.cancelExecution();
			afterCanceling.trigger();
			
			// wait for the execution to be finished
			operation.join();
			
			if (error.get() != null) {
				ExceptionUtils.rethrow(error.get());
			}
			
			// make sure the final state is correct and the task manager knows the changes
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTaskWithEnvironment() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			
			TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jid, vid, eid, "TestTask", 2, 7,
					new Configuration(), new Configuration(), TestInvokableCorrect.class.getName(),
					Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
					Collections.<InputGateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();
			
			RuntimeEnvironment env = new RuntimeEnvironment(mock(ActorRef.class), task, tdd, getClass().getClassLoader(),
					mock(MemoryManager.class), mock(IOManager.class), mock(InputSplitProvider.class),
					new BroadcastVariableManager(), MockNetworkEnvironment.getMock());

			task.setEnvironment(env);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			task.startExecution();
			task.getEnvironment().getExecutingThread().join();
			
			assertEquals(ExecutionState.FINISHED, task.getExecutionState());

			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTaskWithEnvironmentAndException() {
		try {
			final JobID jid = new JobID();
			final JobVertexID vid = new JobVertexID();
			final ExecutionAttemptID eid = new ExecutionAttemptID();
			
			TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(jid, vid, eid, "TestTask", 2, 7,
					new Configuration(), new Configuration(), TestInvokableWithException.class.getName(),
					Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
					Collections.<InputGateDeploymentDescriptor>emptyList(),
					new ArrayList<BlobKey>(), 0);
			
			Task task = spy(new Task(jid, vid, 2, 7, eid, "TestTask", ActorRef.noSender()));
			doNothing().when(task).unregisterTask();
			
			RuntimeEnvironment env = new RuntimeEnvironment(mock(ActorRef.class), task, tdd, getClass().getClassLoader(),
					mock(MemoryManager.class), mock(IOManager.class), mock(InputSplitProvider.class),
					new BroadcastVariableManager(), MockNetworkEnvironment.getMock());

			task.setEnvironment(env);
			
			assertEquals(ExecutionState.DEPLOYING, task.getExecutionState());
			
			task.startExecution();
			task.getEnvironment().getExecutingThread().join();
			
			assertEquals(ExecutionState.FAILED, task.getExecutionState());

			verify(task).unregisterTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class TestInvokableCorrect extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() {}
	}
	
	public static final class TestInvokableWithException extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {
			throw new Exception("test exception");
		}
	}
}
