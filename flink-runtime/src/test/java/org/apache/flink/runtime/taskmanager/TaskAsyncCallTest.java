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
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineTaskNotCheckpointingException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forFullCheckpoint());
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
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forFullCheckpoint());
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

	@Test
	public void testExceptionAfterIllegalCheckpointCalls() {
		try {
			long checkpointId = 1314L;
			ArrayCheckpointResponder checkpointResponder = new ArrayCheckpointResponder();
			Task task = createTask(UnsupportedCheckpointsInvokable.class.getName(), checkpointResponder);
			task.startTaskThread();

			awaitLatch.await();

			task.triggerCheckpointBarrier(checkpointId, 156865867234L, CheckpointOptions.forFullCheckpoint());

			triggerLatch.await();

			assertFalse(task.isCanceledOrFailed());

			ArrayList<ArrayCheckpointResponder.DeclineCheckpointInformation> informationList = checkpointResponder.getDeclineCheckpointInformationList();
			assertEquals(1, informationList.size());

			ArrayCheckpointResponder.DeclineCheckpointInformation information = informationList.get(0);
			String errorMessage = "Task '" + task.getTaskInfo().getTaskNameWithSubtasks() + "'does not support checkpointing";

			assertEquals(task.getJobID(), information.getJobID());
			assertEquals(task.getExecutionId(), information.getExecutionAttemptID());
			assertEquals(checkpointId, information.getCheckpointId());
			assertThat(information.getCause(), instanceOf(CheckpointDeclineTaskNotCheckpointingException.class));
			assertEquals(errorMessage, information.getCause().getMessage());

			task.cancelExecution();
			task.getExecutingThread().join();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static Task createTask() throws Exception {
		return createTask(CheckpointsInOrderInvokable.class.getName(), mock(CheckpointResponder.class));
	}

	private static Task createTask(String invokableClassName, CheckpointResponder checkpointResponder) throws Exception {
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());
		
		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);
		NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
		when(networkEnvironment.getResultPartitionManager()).thenReturn(partitionManager);
		when(networkEnvironment.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(networkEnvironment.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));

		JobInformation jobInformation = new JobInformation(
			new JobID(),
			"Job Name",
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList());

		TaskInformation taskInformation = new TaskInformation(
			new JobVertexID(),
			"Test Task",
			1,
			1,
			invokableClassName,
			new Configuration());

		return new Task(
			jobInformation,
			taskInformation,
			new ExecutionAttemptID(),
			new AllocationID(),
			0,
			0,
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			0,
			new TaskStateHandles(),
			mock(MemoryManager.class),
			mock(IOManager.class),
			networkEnvironment,
			mock(BroadcastVariableManager.class),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			checkpointResponder,
			libCache,
			mock(FileCache.class),
			new TestingTaskManagerRuntimeInfo(),
			mock(TaskMetricGroup.class),
			consumableNotifier,
			partitionProducerStateChecker,
			executor);
	}

	public static class CheckpointsInOrderInvokable extends AbstractInvokable {

		private volatile long lastCheckpointId = 0;
		
		private volatile Exception error;

		public CheckpointsInOrderInvokable(Environment environment, TaskStateHandles taskStateHandles) {
			super(environment, taskStateHandles);
		}

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
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
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
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
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

	public static class UnsupportedCheckpointsInvokable extends AbstractInvokable {

		private volatile long lastCheckpointId = 0;

		private volatile Exception error;

		public UnsupportedCheckpointsInvokable(Environment environment, TaskStateHandles taskStateHandles) {
			super(environment, taskStateHandles);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			// wait forever (until canceled)
			synchronized (this) {
				while (error == null && lastCheckpointId < 1) {
					wait();
				}
			}

			if (error != null) {
				throw error;
			}
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
			lastCheckpointId++;
			throw new UnsupportedOperationException(String.format("triggerCheckpoint not supported by %s", this.getClass().getName()));
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
			throw new UnsupportedOperationException(String.format("triggerCheckpointOnBarrier not supported by %s", this.getClass().getName()));
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
			throw new UnsupportedOperationException(String.format("abortCheckpointOnBarrier not supported by %s", this.getClass().getName()));
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			throw new UnsupportedOperationException(String.format("notifyCheckpointComplete not supported by %s", this.getClass().getName()));
		}
	}

	public static class ArrayCheckpointResponder implements CheckpointResponder {
		class DeclineCheckpointInformation {
			private JobID jobID;
			private ExecutionAttemptID executionAttemptID;
			private long checkpointId;
			private Throwable cause;

			DeclineCheckpointInformation(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, Throwable cause) {
				this.jobID = jobID;
				this.executionAttemptID = executionAttemptID;
				this.checkpointId = checkpointId;
				this.cause = cause;
			}

			public JobID getJobID() {
				return this.jobID;
			}

			public ExecutionAttemptID getExecutionAttemptID() {
				return this.executionAttemptID;
			}

			public long getCheckpointId() {
				return this.checkpointId;
			}

			public Throwable getCause() {
				return this.cause;
			}
		}

		private ArrayList<DeclineCheckpointInformation> declineCheckpointInformationList;

		public ArrayCheckpointResponder(){
			this.declineCheckpointInformationList = new ArrayList<>(1);
		}

		public ArrayList<DeclineCheckpointInformation> getDeclineCheckpointInformationList() {
			return this.declineCheckpointInformationList;
		}

		@Override
		public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, SubtaskState subtaskState) {

		}

		@Override
		public void declineCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, Throwable cause) {
			declineCheckpointInformationList.add(new DeclineCheckpointInformation(jobID, executionAttemptID, checkpointId, cause));
			triggerLatch.trigger();
		}
	}
}
