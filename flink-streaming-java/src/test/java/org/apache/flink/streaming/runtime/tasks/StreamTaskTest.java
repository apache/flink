/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import akka.actor.ActorRef;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamTaskTest {

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() {
		try {
			StreamConfig cfg = new StreamConfig(new Configuration());
			cfg.setStreamOperator(new SlowlyDeserializingOperator());
			
			Task task = createTask(SourceStreamTask.class, cfg);
			task.startTaskThread();
			
			// wait until the task thread reached state RUNNING 
			while (task.getExecutionState() == ExecutionState.CREATED ||
					task.getExecutionState() == ExecutionState.DEPLOYING)
			{
				Thread.sleep(5);
			}
			
			// make sure the task is really running
			if (task.getExecutionState() != ExecutionState.RUNNING) {
				fail("Task entered state " + task.getExecutionState() + " with error "
						+ ExceptionUtils.stringifyException(task.getFailureCause()));
			}
			
			// send a cancel. because the operator takes a long time to deserialize, this should
			// hit the task before the operator is deserialized
			task.cancelExecution();
			
			// the task should reach state canceled eventually
			assertTrue(task.getExecutionState() == ExecutionState.CANCELING ||
					task.getExecutionState() == ExecutionState.CANCELED);
			
			task.getExecutingThread().join(60000);
			
			assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private Task createTask(Class<? extends AbstractInvokable> invokable, StreamConfig taskConfig) {
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
		
		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getPartitionManager()).thenReturn(partitionManager);
		when(network.getPartitionConsumableNotifier()).thenReturn(consumableNotifier);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), new JobVertexID(), new ExecutionAttemptID(),
				"Test Task", 0, 1, 0,
				new Configuration(),
				taskConfig.getConfiguration(),
				invokable.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList(),
				0);

		return new Task(
				tdd,
				mock(MemoryManager.class),
				mock(IOManager.class),
				network,
				mock(BroadcastVariableManager.class),
				new DummyGateway(),
				new DummyGateway(),
				new FiniteDuration(60, TimeUnit.SECONDS),
				libCache,
				mock(FileCache.class),
				new TaskManagerRuntimeInfo("localhost", new Configuration()));
	}
	
	// ------------------------------------------------------------------------
	//  Test operators
	// ------------------------------------------------------------------------
	
	public static class SlowlyDeserializingOperator extends StreamSource<Long, SourceFunction<Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean canceled = false;
		
		public SlowlyDeserializingOperator() {
			super(new MockSourceFunction());
		}

		@Override
		public void run(Object lockingObject, Output<StreamRecord<Long>> collector) throws Exception {
			while (!canceled) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) {}
			}
		}

		@Override
		public void cancel() {
			canceled = true;
		}

		// slow deserialization
		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			
			long delay = 500;
			long deadline = System.currentTimeMillis() + delay;
			do {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException ignored) {}
			} while ((delay = deadline - System.currentTimeMillis()) > 0);
		}
	}
	
	private static class MockSourceFunction implements SourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Long> ctx) {}

		@Override
		public void cancel() {}
	}

	// ------------------------------------------------------------------------
	//  Test JobManager/TaskManager gateways
	// ------------------------------------------------------------------------
	
	private static class DummyGateway implements ActorGateway {
		private static final long serialVersionUID = 1L;

		@Override
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			return null;
		}

		@Override
		public void tell(Object message) {}

		@Override
		public void tell(Object message, ActorGateway sender) {}

		@Override
		public void forward(Object message, ActorGateway sender) {}

		@Override
		public Future<Object> retry(Object message, int numberRetries, FiniteDuration timeout, ExecutionContext executionContext) {
			return null;
		}

		@Override
		public String path() {
			return null;
		}

		@Override
		public ActorRef actor() {
			return null;
		}

		@Override
		public UUID leaderSessionID() {
			return null;
		}
	}
}
