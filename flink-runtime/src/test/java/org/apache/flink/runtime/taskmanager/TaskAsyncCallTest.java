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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.SerializedValue;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskAsyncCallTest {

	private static final int NUM_CALLS = 1000;
	
	private static OneShotLatch awaitLatch;
	private static OneShotLatch triggerLatch;

	@Before
	public void createQueuesAndActors() {
		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
	}


	// ------------------------------------------------------------------------
	//  Tests 
	// ------------------------------------------------------------------------
	
	@Test
	public void testCheckpointCallsInOrder() {
		try {
			Task task = createTask();
			task.startTaskThread();
			
			awaitLatch.await();
			
			for (int i = 1; i <= NUM_CALLS; i++) {
				task.triggerCheckpointBarrier(i, 156865867234L);
			}
			
			triggerLatch.await();
			
			assertFalse(task.isCanceledOrFailed());

			ExecutionState currentState = task.getExecutionState();
			if (currentState != ExecutionState.RUNNING && currentState != ExecutionState.FINISHED) {
				fail("Task should be RUNNING or FINISHED, but is " + currentState);
			}
			
			task.cancelExecution();
			task.getExecutingThread().join();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMixedAsyncCallsInOrder() {
		try {
			Task task = createTask();
			task.startTaskThread();

			awaitLatch.await();

			for (int i = 1; i <= NUM_CALLS; i++) {
				task.triggerCheckpointBarrier(i, 156865867234L);
				task.notifyCheckpointComplete(i);
			}

			triggerLatch.await();

			assertFalse(task.isCanceledOrFailed());
			ExecutionState currentState = task.getExecutionState();
			if (currentState != ExecutionState.RUNNING && currentState != ExecutionState.FINISHED) {
				fail("Task should be RUNNING or FINISHED, but is " + currentState);
			}

			task.cancelExecution();
			task.getExecutingThread().join();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static Task createTask() throws Exception {
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());
		
		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionStateChecker partitionStateChecker = mock(PartitionStateChecker.class);
		Executor executor = mock(Executor.class);
		NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
		when(networkEnvironment.getResultPartitionManager()).thenReturn(partitionManager);
		when(networkEnvironment.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(networkEnvironment.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), "Job Name", new JobVertexID(), new ExecutionAttemptID(),
				new SerializedValue<>(new ExecutionConfig()),
				"Test Task", 1, 0, 1, 0,
				new Configuration(), new Configuration(),
				CheckpointsInOrderInvokable.class.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList(),
				0);

		return new Task(tdd,
			mock(MemoryManager.class),
			mock(IOManager.class),
			networkEnvironment,
			mock(BroadcastVariableManager.class),
			mock(TaskManagerConnection.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			libCache,
			mock(FileCache.class),
			new TaskManagerRuntimeInfo("localhost", new Configuration(), System.getProperty("java.io.tmpdir")),
			mock(TaskMetricGroup.class),
			consumableNotifier,
			partitionStateChecker,
			executor);
	}

	public static class CheckpointsInOrderInvokable extends AbstractInvokable implements StatefulTask {

		private volatile long lastCheckpointId = 0;
		
		private volatile Exception error;
		
		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();
			
			// wait forever (until canceled)
			synchronized (this) {
				while (error == null && lastCheckpointId < NUM_CALLS) {
					wait();
				}
			}
			
			triggerLatch.trigger();
			if (error != null) {
				throw error;
			}
		}

		@Override
		public void setInitialState(TaskStateHandles taskStateHandles) throws Exception {

		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData) {
			lastCheckpointId++;
			if (checkpointMetaData.getCheckpointId() == lastCheckpointId) {
				if (lastCheckpointId == NUM_CALLS) {
					triggerLatch.trigger();
				}
			}
			else if (this.error == null) {
				this.error = new Exception("calls out of order");
				synchronized (this) {
					notifyAll();
				}
			}
			return true;
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData) throws Exception {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			if (checkpointId != lastCheckpointId && this.error == null) {
				this.error = new Exception("calls out of order");
				synchronized (this) {
					notifyAll();
				}
			}
		}
	}
}
