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

import akka.dispatch.Futures;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionStateListener;
import org.apache.flink.runtime.taskmanager.TaskManagerConnection;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamTaskTest {

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() throws Exception {
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new SlowlyDeserializingOperator());
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(SourceStreamTask.class, cfg, new Configuration());

		TestingExecutionStateListener testingExecutionStateListener = new TestingExecutionStateListener();

		task.registerExecutionListener(testingExecutionStateListener);
		task.startTaskThread();

		Future<ExecutionState> running = testingExecutionStateListener.notifyWhenExecutionState(ExecutionState.RUNNING);

		// wait until the task thread reached state RUNNING
		ExecutionState executionState = Await.result(running, deadline.timeLeft());

		// make sure the task is really running
		if (executionState != ExecutionState.RUNNING) {
			fail("Task entered state " + task.getExecutionState() + " with error "
					+ ExceptionUtils.stringifyException(task.getFailureCause()));
		}

		// send a cancel. because the operator takes a long time to deserialize, this should
		// hit the task before the operator is deserialized
		task.cancelExecution();

		Future<ExecutionState> canceling = testingExecutionStateListener.notifyWhenExecutionState(ExecutionState.CANCELING);

		executionState = Await.result(canceling, deadline.timeLeft());

		// the task should reach state canceled eventually
		assertTrue(executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED);

		task.getExecutingThread().join(deadline.timeLeft().toMillis());

		assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testStateBackendLoading() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(ConfigConstants.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(SourceStreamTask.class, cfg, taskManagerConfig);

		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}


	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class TestingExecutionStateListener implements TaskExecutionStateListener {

		private ExecutionState executionState = null;

		private final PriorityQueue<Tuple2<ExecutionState, Promise<ExecutionState>>> priorityQueue = new PriorityQueue<>(
			1,
			new Comparator<Tuple2<ExecutionState, Promise<ExecutionState>>>() {
				@Override
				public int compare(Tuple2<ExecutionState, Promise<ExecutionState>> o1, Tuple2<ExecutionState, Promise<ExecutionState>> o2) {
					return o1.f0.ordinal() - o2.f0.ordinal();
				}
			});

		public Future<ExecutionState> notifyWhenExecutionState(ExecutionState executionState) {
			synchronized (priorityQueue) {
				if (this.executionState != null && this.executionState.ordinal() >= executionState.ordinal()) {
					return Futures.successful(executionState);
				} else {
					Promise<ExecutionState> promise = new Promise.DefaultPromise<ExecutionState>();

					priorityQueue.offer(Tuple2.of(executionState, promise));

					return promise.future();
				}
			}
		}

		@Override
		public void notifyTaskExecutionStateChanged(TaskExecutionState taskExecutionState) {
			synchronized (priorityQueue) {
				this.executionState = taskExecutionState.getExecutionState();

				while (!priorityQueue.isEmpty() && priorityQueue.peek().f0.ordinal() <= executionState.ordinal()) {
					Promise<ExecutionState> promise = priorityQueue.poll().f1;

					promise.success(executionState);
				}
			}
		}
	}

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			StreamConfig taskConfig,
			Configuration taskManagerConfig) throws Exception {

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
		
		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionStateChecker partitionStateChecker = mock(PartitionStateChecker.class);
		Executor executor = mock(Executor.class);

		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(partitionManager);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), "Job Name", new JobVertexID(), new ExecutionAttemptID(),
				new SerializedValue<>(new ExecutionConfig()),
				"Test Task", 1, 0, 1, 0,
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
			mock(TaskManagerConnection.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			libCache,
			mock(FileCache.class),
			new TaskManagerRuntimeInfo("localhost", taskManagerConfig, System.getProperty("java.io.tmpdir")),
			new UnregisteredTaskMetricsGroup(),
			consumableNotifier,
			partitionStateChecker,
			executor);
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

	public static final class MockStateBackend implements StateBackendFactory<AbstractStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public AbstractStateBackend createFromConfig(Configuration config) throws Exception {
			return mock(AbstractStateBackend.class);
		}
	}
}
