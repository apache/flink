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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.TestingUncaughtExceptionHandler;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.writer.AvailabilityTestResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.MockResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionTest;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskLocalStateStoreImpl;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestTaskLocalStateStore;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpCheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TestTaskBuilder;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.MockIndexedInputGate;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.streaming.util.TestSequentialReadingStreamOperator;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.apache.flink.streaming.util.StreamTaskUtil.waitTaskIsRunning;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamTask}.
 */
@SuppressWarnings("deprecation")
public class StreamTaskTest extends TestLogger {

	private static OneShotLatch syncLatch;

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(30);

	@Test
	public void testCleanUpExceptionSuppressing() throws Exception {
		OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
			OneInputStreamTask::new,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(new FailingTwiceOperator());
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>("Doesn't matter", 0));

		try {
			testHarness.waitForTaskCompletion();
		}
		catch (Exception ex) {
			if (!ExceptionUtils.findThrowable(ex, ExpectedTestException.class).isPresent()) {
				throw ex;
			}
		}
	}

	private static class FailingTwiceOperator extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			throw new ExpectedTestException();
		}

		@Override
		public void dispose() throws Exception {
			fail("This exception should be suppressed");
		}
	}

	/**
	 * This test checks the async exceptions handling wraps the message and cause as an AsynchronousException
	 * and propagates this to the environment.
	 */
	@Test
	public void streamTaskAsyncExceptionHandler_handleException_forwardsMessageProperly() {
		MockEnvironment mockEnvironment = MockEnvironment.builder().build();
		RuntimeException expectedException = new RuntimeException("RUNTIME EXCEPTION");

		final StreamTask.StreamTaskAsyncExceptionHandler asyncExceptionHandler = new StreamTask.StreamTaskAsyncExceptionHandler(mockEnvironment);

		mockEnvironment.setExpectedExternalFailureCause(AsynchronousException.class);
		final String expectedErrorMessage = "EXPECTED_ERROR MESSAGE";

		asyncExceptionHandler.handleAsyncException(expectedErrorMessage, expectedException);

		// expect an AsynchronousException containing the supplied error details
		Optional<? extends Throwable> actualExternalFailureCause = mockEnvironment.getActualExternalFailureCause();
		final Throwable actualException = actualExternalFailureCause
			.orElseThrow(() -> new AssertionError("Expected exceptional completion"));

		assertThat(actualException, instanceOf(AsynchronousException.class));
		assertThat(actualException.getMessage(), is("EXPECTED_ERROR MESSAGE"));
		assertThat(actualException.getCause(), is(expectedException));
	}

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() throws Exception {
		final StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		cfg.setStreamOperator(new SlowlyDeserializingOperator());
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		final TaskManagerActions taskManagerActions = spy(new NoOpTaskManagerActions());
		try (NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
			final Task task =  new TestTaskBuilder(shuffleEnvironment)
				.setInvokable(SourceStreamTask.class)
				.setTaskConfig(cfg.getConfiguration())
				.setTaskManagerActions(taskManagerActions)
				.build();

			final TaskExecutionState state = new TaskExecutionState(
				task.getJobID(), task.getExecutionId(), ExecutionState.RUNNING);

			task.startTaskThread();

			verify(taskManagerActions, timeout(2000L)).updateTaskExecutionState(eq(state));

			// send a cancel. because the operator takes a long time to deserialize, this should
			// hit the task before the operator is deserialized
			task.cancelExecution();

			task.getExecutingThread().join();

			assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		}
	}

	@Test
	public void testStateBackendLoadingAndClosing() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CheckpointingOptions.STATE_BACKEND, TestMemoryStateBackendFactory.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateKeySerializer(mock(TypeSerializer.class));
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		TestStreamSource<Long, MockSourceFunction> streamSource = new TestStreamSource<>(new MockSourceFunction());
		cfg.setStreamOperator(streamSource);
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		try (ShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
			Task task = createTask(StateBackendTestSource.class, shuffleEnvironment, cfg, taskManagerConfig);

			StateBackendTestSource.fail = false;
			task.startTaskThread();

			// wait for clean termination
			task.getExecutingThread().join();

			// ensure that the state backends and stream iterables are closed ...
			verify(TestStreamSource.operatorStateBackend).close();
			verify(TestStreamSource.keyedStateBackend).close();
			verify(TestStreamSource.rawOperatorStateInputs).close();
			verify(TestStreamSource.rawKeyedStateInputs).close();
			// ... and disposed
			verify(TestStreamSource.operatorStateBackend).dispose();
			verify(TestStreamSource.keyedStateBackend).dispose();

			assertEquals(ExecutionState.FINISHED, task.getExecutionState());
		}
	}

	@Test
	public void testStateBackendClosingOnFailure() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CheckpointingOptions.STATE_BACKEND, TestMemoryStateBackendFactory.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateKeySerializer(mock(TypeSerializer.class));
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		TestStreamSource<Long, MockSourceFunction> streamSource = new TestStreamSource<>(new MockSourceFunction());
		cfg.setStreamOperator(streamSource);
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		try (NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
			Task task = createTask(StateBackendTestSource.class, shuffleEnvironment, cfg, taskManagerConfig);

			StateBackendTestSource.fail = true;
			task.startTaskThread();

			// wait for clean termination
			task.getExecutingThread().join();

			// ensure that the state backends and stream iterables are closed ...
			verify(TestStreamSource.operatorStateBackend).close();
			verify(TestStreamSource.keyedStateBackend).close();
			verify(TestStreamSource.rawOperatorStateInputs).close();
			verify(TestStreamSource.rawKeyedStateInputs).close();
			// ... and disposed
			verify(TestStreamSource.operatorStateBackend).dispose();
			verify(TestStreamSource.keyedStateBackend).dispose();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
		}
	}

	@Test
	public void testCanceleablesCanceledOnCancelTaskError() throws Exception {
		syncLatch = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		try (NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {

			Task task = createTask(CancelFailingTask.class, shuffleEnvironment, cfg, new Configuration());

			// start the task and wait until it runs
			// execution state RUNNING is not enough, we need to wait until the stream task's run() method
			// is entered
			task.startTaskThread();
			syncLatch.await();

			// cancel the execution - this should lead to smooth shutdown
			task.cancelExecution();
			task.getExecutingThread().join();

			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		}
	}

	/**
	 * A task that locks for ever, fail in {@link #cancelTask()}. It can be only shut down cleanly
	 * if {@link StreamTask#getCancelables()} are closed properly.
	 */
	public static class CancelFailingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		public CancelFailingTask(Environment env) throws Exception {
			super(env);
		}

		@Override
		protected void init() {}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
			final OneShotLatch latch = new OneShotLatch();
			final Object lock = new Object();

			LockHolder holder = new LockHolder(lock, latch);
			holder.start();
			try {
				// cancellation should try and cancel this
				getCancelables().registerCloseable(holder);

				// wait till the lock holder has the lock
				latch.await();

				// we are at the point where cancelling can happen
				syncLatch.trigger();

				// try to acquire the lock - this is not possible as long as the lock holder
				// thread lives
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (lock) {
					// nothing
				}
			}
			finally {
				holder.close();
			}
			controller.allActionsCompleted();
		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() throws Exception {
			throw new Exception("test exception");
		}

		/**
		 * A thread that holds a lock as long as it lives.
		 */
		private static final class LockHolder extends Thread implements Closeable {

			private final OneShotLatch trigger;
			private final Object lock;
			private volatile boolean canceled;

			private LockHolder(Object lock, OneShotLatch trigger) {
				this.lock = lock;
				this.trigger = trigger;
			}

			@Override
			public void run() {
				synchronized (lock) {
					while (!canceled) {
						// signal that we grabbed the lock
						trigger.trigger();

						// basically freeze this thread
						try {
							//noinspection SleepWhileHoldingLock
							Thread.sleep(1000000000);
						} catch (InterruptedException ignored) {}
					}
				}
			}

			public void cancel() {
				canceled = true;
			}

			@Override
			public void close() {
				canceled = true;
				interrupt();
			}
		}
	}

	/**
	 *  CancelTaskException can be thrown in a down stream task, for example if an upstream task
	 *  was cancelled first and those two tasks were connected via
	 *  {@link org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel}.
	 *  {@link StreamTask} should be able to correctly handle such situation.
	 */
	@Test
	public void testCancelTaskExceptionHandling() throws Exception {
		StreamConfig cfg = new StreamConfig(new Configuration());

		try (NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
			Task task = createTask(CancelThrowingTask.class, shuffleEnvironment, cfg, new Configuration());

			task.startTaskThread();
			task.getExecutingThread().join();

			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		}
	}

	/**
	 * A task that throws {@link CancelTaskException}.
	 */
	public static class CancelThrowingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		public CancelThrowingTask(Environment env) throws Exception {
			super(env);
		}

		@Override
		protected void init() {}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) {
			throw new CancelTaskException();
		}
	}

	@Test
	public void testDecliningCheckpointStreamOperator() throws Exception {
		DeclineDummyEnvironment declineDummyEnvironment = new DeclineDummyEnvironment();

		// mock the returned snapshots
		OperatorSnapshotFutures operatorSnapshotResult1 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult2 = mock(OperatorSnapshotFutures.class);

		final Exception testException = new ExpectedTestException();

		RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
			declineDummyEnvironment,
			operatorChain(
				streamOperatorWithSnapshotException(testException),
				streamOperatorWithSnapshot(operatorSnapshotResult1),
				streamOperatorWithSnapshot(operatorSnapshotResult2)
				)));
		MockStreamTask streamTask = task.streamTask;

		waitTaskIsRunning(streamTask, task.invocationFuture);

		streamTask.triggerCheckpointAsync(
			new CheckpointMetaData(42L, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		try {
			task.waitForTaskCompletion(false);
		}
		catch (Exception ex) {
			if (!ExceptionUtils.findThrowable(ex, ExpectedTestException.class).isPresent()) {
				throw ex;
			}
		}

		verify(operatorSnapshotResult1).cancel();
		verify(operatorSnapshotResult2).cancel();
	}

	/**
	 * Tests that uncaught exceptions in the async part of a checkpoint operation are forwarded
	 * to the uncaught exception handler. See <a href="https://issues.apache.org/jira/browse/FLINK-12889">FLINK-12889</a>.
	 */
	@Test
	public void testUncaughtExceptionInAsynchronousCheckpointingOperation() throws Exception {
		final RuntimeException failingCause = new RuntimeException("Test exception");
		FailingDummyEnvironment failingDummyEnvironment = new FailingDummyEnvironment(failingCause);

		// mock the returned snapshots
		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			ExceptionallyDoneFuture.of(failingCause),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()));

		final TestingUncaughtExceptionHandler uncaughtExceptionHandler = new TestingUncaughtExceptionHandler();

		RunningTask<MockStreamTask> task = runTask(() -> new MockStreamTask(
			failingDummyEnvironment,
			operatorChain(streamOperatorWithSnapshot(operatorSnapshotResult)),
			uncaughtExceptionHandler));
		MockStreamTask streamTask = task.streamTask;

		waitTaskIsRunning(streamTask, task.invocationFuture);

		streamTask.triggerCheckpointAsync(
			new CheckpointMetaData(42L, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		final Throwable uncaughtException = uncaughtExceptionHandler.waitForUncaughtException();
		assertThat(uncaughtException, is(failingCause));

		streamTask.finishInput();
		task.waitForTaskCompletion(false);
	}

	/**
	 * Tests that in case of a failing AsyncCheckpointRunnable all operator snapshot results are
	 * cancelled and all non partitioned state handles are discarded.
	 */
	@Test
	public void testFailingAsyncCheckpointRunnable() throws Exception {

		// mock the new state operator snapshots
		OperatorSnapshotFutures operatorSnapshotResult1 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult2 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult3 = mock(OperatorSnapshotFutures.class);

		RunnableFuture<SnapshotResult<OperatorStateHandle>> failingFuture = mock(RunnableFuture.class);
		when(failingFuture.get()).thenThrow(new ExecutionException(new Exception("Test exception")));

		when(operatorSnapshotResult3.getOperatorStateRawFuture()).thenReturn(failingFuture);

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder().build()) {
			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(
					streamOperatorWithSnapshot(operatorSnapshotResult1),
					streamOperatorWithSnapshot(operatorSnapshotResult2),
					streamOperatorWithSnapshot(operatorSnapshotResult3))));

			MockStreamTask streamTask = task.streamTask;

			waitTaskIsRunning(streamTask, task.invocationFuture);

			mockEnvironment.setExpectedExternalFailureCause(Throwable.class);
			streamTask.triggerCheckpointAsync(
				new CheckpointMetaData(42L, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false)
				.get();

			// wait for the completion of the async task
			ExecutorService executor = streamTask.getAsyncOperationsThreadPool();
			executor.shutdown();
			if (!executor.awaitTermination(10000L, TimeUnit.MILLISECONDS)) {
				fail("Executor did not shut down within the given timeout. This indicates that the " +
					"checkpointing did not resume.");
			}

			assertTrue(mockEnvironment.getActualExternalFailureCause().isPresent());

			verify(operatorSnapshotResult1).cancel();
			verify(operatorSnapshotResult2).cancel();
			verify(operatorSnapshotResult3).cancel();

			streamTask.finishInput();
			task.waitForTaskCompletion(false);
		}
	}

	/**
	 * FLINK-5667
	 *
	 * <p>Tests that a concurrent cancel operation does not discard the state handles of an
	 * acknowledged checkpoint. The situation can only happen if the cancel call is executed
	 * after Environment.acknowledgeCheckpoint() and before the
	 * CloseableRegistry.unregisterClosable() call.
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseAfterAcknowledge() throws Exception {

		final OneShotLatch acknowledgeCheckpointLatch = new OneShotLatch();
		final OneShotLatch completeAcknowledge = new OneShotLatch();

		CheckpointResponder checkpointResponder = mock(CheckpointResponder.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) {
				acknowledgeCheckpointLatch.trigger();

				// block here so that we can issue the concurrent cancel call
				while (true) {
					try {
						// wait until we successfully await (no pun intended)
						completeAcknowledge.await();

						// when await() returns normally, we break out of the loop
						break;
					} catch (InterruptedException e) {
						// survive interruptions that arise from thread pool shutdown
						// production code cannot actually throw InterruptedException from
						// checkpoint acknowledgement
					}
				}

				return null;
			}
		}).when(checkpointResponder).acknowledgeCheckpoint(
			any(JobID.class),
			any(ExecutionAttemptID.class),
			anyLong(),
			any(CheckpointMetrics.class),
			any(TaskStateSnapshot.class));

		TaskStateManager taskStateManager = new TaskStateManagerImpl(
			new JobID(1L, 2L),
			new ExecutionAttemptID(1L, 2L),
			mock(TaskLocalStateStoreImpl.class),
			null,
			checkpointResponder);

		KeyedStateHandle managedKeyedStateHandle = mock(KeyedStateHandle.class);
		KeyedStateHandle rawKeyedStateHandle = mock(KeyedStateHandle.class);
		OperatorStateHandle managedOperatorStateHandle = mock(OperatorStreamStateHandle.class);
		OperatorStateHandle rawOperatorStateHandle = mock(OperatorStreamStateHandle.class);

		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			DoneFuture.of(SnapshotResult.of(managedKeyedStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawKeyedStateHandle)),
			DoneFuture.of(SnapshotResult.of(managedOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()));

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder()
			.setTaskName("mock-task")
			.setTaskStateManager(taskStateManager)
			.build()) {

			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(streamOperatorWithSnapshot(operatorSnapshotResult))));

			MockStreamTask streamTask = task.streamTask;
			waitTaskIsRunning(streamTask, task.invocationFuture);

			final long checkpointId = 42L;
			streamTask.triggerCheckpointAsync(
				new CheckpointMetaData(checkpointId, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

			acknowledgeCheckpointLatch.await();

			ArgumentCaptor<TaskStateSnapshot> subtaskStateCaptor = ArgumentCaptor.forClass(TaskStateSnapshot.class);

			// check that the checkpoint has been completed
			verify(checkpointResponder).acknowledgeCheckpoint(
				any(JobID.class),
				any(ExecutionAttemptID.class),
				eq(checkpointId),
				any(CheckpointMetrics.class),
				subtaskStateCaptor.capture());

			TaskStateSnapshot subtaskStates = subtaskStateCaptor.getValue();
			OperatorSubtaskState subtaskState = subtaskStates.getSubtaskStateMappings().iterator().next().getValue();

			// check that the subtask state contains the expected state handles
			assertEquals(singleton(managedKeyedStateHandle), subtaskState.getManagedKeyedState());
			assertEquals(singleton(rawKeyedStateHandle), subtaskState.getRawKeyedState());
			assertEquals(singleton(managedOperatorStateHandle), subtaskState.getManagedOperatorState());
			assertEquals(singleton(rawOperatorStateHandle), subtaskState.getRawOperatorState());

			// check that the state handles have not been discarded
			verify(managedKeyedStateHandle, never()).discardState();
			verify(rawKeyedStateHandle, never()).discardState();
			verify(managedOperatorStateHandle, never()).discardState();
			verify(rawOperatorStateHandle, never()).discardState();

			streamTask.cancel();

			completeAcknowledge.trigger();

			// canceling the stream task after it has acknowledged the checkpoint should not discard
			// the state handles
			verify(managedKeyedStateHandle, never()).discardState();
			verify(rawKeyedStateHandle, never()).discardState();
			verify(managedOperatorStateHandle, never()).discardState();
			verify(rawOperatorStateHandle, never()).discardState();

			task.waitForTaskCompletion(true);
		}
	}

	/**
	 * FLINK-5667
	 *
	 * <p>Tests that a concurrent cancel operation discards the state handles of a not yet
	 * acknowledged checkpoint and prevents sending an acknowledge message to the
	 * CheckpointCoordinator. The situation can only happen if the cancel call is executed
	 * before Environment.acknowledgeCheckpoint().
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseBeforeAcknowledge() throws Exception {

		final TestingKeyedStateHandle managedKeyedStateHandle = new TestingKeyedStateHandle();
		final TestingKeyedStateHandle rawKeyedStateHandle = new TestingKeyedStateHandle();
		final TestingOperatorStateHandle managedOperatorStateHandle = new TestingOperatorStateHandle();
		final TestingOperatorStateHandle rawOperatorStateHandle = new TestingOperatorStateHandle();

		final BlockingRunnableFuture<SnapshotResult<KeyedStateHandle>> rawKeyedStateHandleFuture = new BlockingRunnableFuture<>(2, SnapshotResult.of(rawKeyedStateHandle));
		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			DoneFuture.of(SnapshotResult.of(managedKeyedStateHandle)),
			rawKeyedStateHandleFuture,
			DoneFuture.of(SnapshotResult.of(managedOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()));

		final OneInputStreamOperator<String, String> streamOperator = streamOperatorWithSnapshot(operatorSnapshotResult);

		final AcknowledgeDummyEnvironment mockEnvironment = new AcknowledgeDummyEnvironment();

		RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
			mockEnvironment,
			operatorChain(streamOperator)));

		waitTaskIsRunning(task.streamTask, task.invocationFuture);

		final long checkpointId = 42L;
		task.streamTask.triggerCheckpointAsync(
			new CheckpointMetaData(checkpointId, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		rawKeyedStateHandleFuture.awaitRun();

		task.streamTask.cancel();

		final FutureUtils.ConjunctFuture<Void> discardFuture = FutureUtils.waitForAll(
			Arrays.asList(
				managedKeyedStateHandle.getDiscardFuture(),
				rawKeyedStateHandle.getDiscardFuture(),
				managedOperatorStateHandle.getDiscardFuture(),
				rawOperatorStateHandle.getDiscardFuture()));

		// make sure that all state handles have been discarded
		discardFuture.get();

		try {
			mockEnvironment.getAcknowledgeCheckpointFuture().get(10L, TimeUnit.MILLISECONDS);
			fail("The checkpoint should not get acknowledged.");
		} catch (TimeoutException expected) {
			// future should not be completed
		}

		task.waitForTaskCompletion(true);
	}

	/**
	 * FLINK-5985
	 *
	 * <p>This test ensures that empty snapshots (no op/keyed stated whatsoever) will be reported as stateless tasks. This
	 * happens by translating an empty {@link SubtaskState} into reporting 'null' to #acknowledgeCheckpoint.
	 */
	@Test
	public void testEmptySubtaskStateLeadsToStatelessAcknowledgment() throws Exception {

		// latch blocks until the async checkpoint thread acknowledges
		final OneShotLatch checkpointCompletedLatch = new OneShotLatch();
		final List<SubtaskState> checkpointResult = new ArrayList<>(1);

		CheckpointResponder checkpointResponder = mock(CheckpointResponder.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				SubtaskState subtaskState = invocation.getArgument(4);
				checkpointResult.add(subtaskState);
				checkpointCompletedLatch.trigger();
				return null;
			}
		}).when(checkpointResponder).acknowledgeCheckpoint(
			any(JobID.class),
			any(ExecutionAttemptID.class),
			anyLong(),
			any(CheckpointMetrics.class),
			nullable(TaskStateSnapshot.class));

		TaskStateManager taskStateManager = new TaskStateManagerImpl(
			new JobID(1L, 2L),
			new ExecutionAttemptID(1L, 2L),
			mock(TaskLocalStateStoreImpl.class),
			null,
			checkpointResponder);

		// mock the operator with empty snapshot result (all state handles are null)
		OneInputStreamOperator<String, String> statelessOperator = streamOperatorWithSnapshot(new OperatorSnapshotFutures());

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder()
			.setTaskStateManager(taskStateManager)
			.build()) {

			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(statelessOperator)));

			waitTaskIsRunning(task.streamTask, task.invocationFuture);

			task.streamTask.triggerCheckpointAsync(
				new CheckpointMetaData(42L, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

			checkpointCompletedLatch.await(30, TimeUnit.SECONDS);

			// ensure that 'null' was acknowledged as subtask state
			Assert.assertNull(checkpointResult.get(0));

			task.streamTask.cancel();
			task.waitForTaskCompletion(true);
		}
	}

	/**
	 * Tests that the StreamTask first closes all of its operators before setting its
	 * state to not running (isRunning == false)
	 *
	 * <p>See FLINK-7430.
	 */
	@Test
	public void testOperatorClosingBeforeStopRunning() throws Throwable {
		BlockingCloseStreamOperator.resetLatches();
		Configuration taskConfiguration = new Configuration();
		StreamConfig streamConfig = new StreamConfig(taskConfiguration);
		streamConfig.setStreamOperator(new BlockingCloseStreamOperator());
		streamConfig.setOperatorID(new OperatorID());

		try (MockEnvironment mockEnvironment =
				new MockEnvironmentBuilder()
					.setTaskName("Test Task")
					.setManagedMemorySize(32L * 1024L)
					.setInputSplitProvider(new MockInputSplitProvider())
					.setBufferSize(1)
					.setTaskConfiguration(taskConfiguration)
					.build()) {

			RunningTask<StreamTask<Void, BlockingCloseStreamOperator>> task = runTask(() -> new NoOpStreamTask<>(mockEnvironment));

			BlockingCloseStreamOperator.inClose.await();

			// check that the StreamTask is not yet in isRunning == false
			assertTrue(task.streamTask.isRunning());

			// let the operator finish its close operation
			BlockingCloseStreamOperator.finishClose.trigger();

			task.waitForTaskCompletion(false);

			// now the StreamTask should no longer be running
			assertFalse(task.streamTask.isRunning());
		}
	}

	/**
	 * Tests that {@link StreamTask#notifyCheckpointCompleteAsync(long)} is not relayed to closed operators.
	 *
	 * <p>See FLINK-16383.
	 */
	@Test
	public void testNotifyCheckpointOnClosedOperator() throws Throwable {
		ClosingOperator operator = new ClosingOperator();
		MultipleInputStreamTaskTestHarnessBuilder<Integer> builder =
			new MultipleInputStreamTaskTestHarnessBuilder<>(OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
				.addInput(BasicTypeInfo.INT_TYPE_INFO);
		StreamTaskMailboxTestHarness<Integer> harness = builder
			.setupOutputForSingletonOperatorChain(operator)
			.build();
		// keeps the mailbox from suspending
		harness.setAutoProcess(false);
		harness.processElement(new StreamRecord<>(1));

		harness.streamTask.notifyCheckpointCompleteAsync(1);
		harness.streamTask.runMailboxStep();
		assertEquals(1, operator.notified.get());
		assertEquals(false, operator.closed.get());

		// close operators directly, so that task is still fully running
		harness.streamTask.operatorChain.closeOperators(harness.streamTask.getActionExecutor());
		harness.streamTask.notifyCheckpointCompleteAsync(2);
		harness.streamTask.runMailboxStep();
		assertEquals(1, operator.notified.get());
		assertEquals(true, operator.closed.get());
	}

	/**
	 * Tests that checkpoints are declined if operators are (partially) closed.
	 *
	 * <p>See FLINK-16383.
	 */
	@Test
	public void testCheckpointDeclinedOnClosedOperator() throws Throwable {
		ClosingOperator operator = new ClosingOperator();
		MultipleInputStreamTaskTestHarnessBuilder<Integer> builder =
			new MultipleInputStreamTaskTestHarnessBuilder<>(OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
					.addInput(BasicTypeInfo.INT_TYPE_INFO);
		StreamTaskMailboxTestHarness<Integer> harness = builder
			.setupOutputForSingletonOperatorChain(operator)
			.build();
		// keeps the mailbox from suspending
		harness.setAutoProcess(false);
		harness.processElement(new StreamRecord<>(1));

		harness.streamTask.operatorChain.closeOperators(harness.streamTask.getActionExecutor());
		assertEquals(true, operator.closed.get());

		harness.streamTask.triggerCheckpointOnBarrier(new CheckpointMetaData(1, 0), CheckpointOptions.forCheckpointWithDefaultLocation(), new CheckpointMetrics());
		assertEquals(1, harness.getCheckpointResponder().getDeclineReports().size());
	}

	@Test
	public void testExecuteMailboxActionsAfterLeavingInputProcessorMailboxLoop() throws Exception {
		OneShotLatch latch = new OneShotLatch();
		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder().build()) {
			RunningTask<StreamTask<?, ?>> task = runTask(() -> new StreamTask<Object, StreamOperator<Object>>(mockEnvironment) {
				@Override
				protected void init() throws Exception {
				}

				@Override
				protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
					mailboxProcessor.getMailboxExecutor(0).execute(latch::trigger, "trigger");
					controller.allActionsCompleted();
				}
			});
			latch.await();
			task.waitForTaskCompletion(false);
		}
	}

	@Test
	public void testBeforeInvokeWithoutChannelStates() throws Exception {
		int numWriters = 2;
		int numGates = 2;
		RecoveryResultPartition[] partitions = new RecoveryResultPartition[numWriters];
		for (int i = 0; i < numWriters; i++) {
			partitions[i] = new RecoveryResultPartition();
		}
		RecoveryInputGate[] gates = new RecoveryInputGate[numGates];
		for (int i = 0; i < numGates; i++) {
			gates[i] = new RecoveryInputGate(partitions);
		}

		MockEnvironment mockEnvironment = new MockEnvironmentBuilder().build();
		mockEnvironment.addOutputs(Arrays.asList(partitions));
		mockEnvironment.addInputs(Arrays.asList(gates));
		StreamTask task = new MockStreamTaskBuilder(mockEnvironment).build();
		try {
			verifyResults(gates, partitions, false, false);

			task.beforeInvoke();

			verifyResults(gates, partitions, false, true);
		} finally {
			task.cleanUpInvoke();
		}
	}

	@Test
	public void testBeforeInvokeWithChannelStates() throws Exception {
		int numWriters = 2;
		int numGates = 2;
		RecoveryResultPartition[] partitions = new RecoveryResultPartition[numWriters];
		for (int i = 0; i < numWriters; i++) {
			partitions[i] = new RecoveryResultPartition();
		}
		RecoveryInputGate[] gates = new RecoveryInputGate[numGates];
		for (int i = 0; i < numGates; i++) {
			gates[i] = new RecoveryInputGate(partitions);
		}

		ChannelStateReader reader = new ResultPartitionTest.FiniteChannelStateReader(1, new int[] {0});
		TaskStateManager taskStateManager = new TaskStateManagerImpl(
			new JobID(),
			new ExecutionAttemptID(),
			new TestTaskLocalStateStore(),
			null,
			NoOpCheckpointResponder.INSTANCE,
			reader);
		MockEnvironment mockEnvironment = new MockEnvironmentBuilder().setTaskStateManager(taskStateManager).build();
		mockEnvironment.addOutputs(Arrays.asList(partitions));
		mockEnvironment.addInputs(Arrays.asList(gates));
		StreamTask task = new MockStreamTaskBuilder(mockEnvironment).build();
		try {
			verifyResults(gates, partitions, false, false);

			task.beforeInvoke();

			verifyResults(gates, partitions, true, false);

			// execute the partition request mail inserted after input recovery completes
			task.mailboxProcessor.drain();

			for (RecoveryInputGate inputGate : gates) {
				assertTrue(inputGate.isPartitionRequested());
			}
		} finally {
			task.cleanUpInvoke();
		}
	}

	private void verifyResults(RecoveryInputGate[] gates, RecoveryResultPartition[] partitions, boolean recoveryExpected, boolean requestExpected) {
		for (RecoveryResultPartition resultPartition : partitions) {
			assertEquals(recoveryExpected, resultPartition.isStateRecovered());
		}
		for (RecoveryInputGate inputGate : gates) {
			assertEquals(recoveryExpected, inputGate.isStateRecovered());
			assertEquals(requestExpected, inputGate.isPartitionRequested());
		}
	}

	/**
	 * Tests that some StreamTask methods are called only in the main task's thread.
	 * Currently, the main task's thread is the thread that creates the task.
	 */
	@Test
	public void testThreadInvariants() throws Throwable {
		Configuration taskConfiguration = new Configuration();
		StreamConfig streamConfig = new StreamConfig(taskConfiguration);
		streamConfig.setStreamOperator(new StreamMap<>(value -> value));
		streamConfig.setOperatorID(new OperatorID());
		try (MockEnvironment mockEnvironment =
				new MockEnvironmentBuilder()
					.setTaskConfiguration(taskConfiguration)
					.build()) {

			ClassLoader taskClassLoader = new TestUserCodeClassLoader();

			RunningTask<ThreadInspectingTask> runningTask = runTask(() -> {
				Thread.currentThread().setContextClassLoader(taskClassLoader);
				return new ThreadInspectingTask(mockEnvironment);
			});
			runningTask.invocationFuture.get();

			assertThat(runningTask.streamTask.getTaskClassLoader(), is(sameInstance(taskClassLoader)));
		}
	}

	/**
	 * This test ensures that {@link RecordWriter} is correctly closed even if we fail to construct
	 * {@link OperatorChain}, for example because of user class deserialization error.
	 */
	@Test
	public void testRecordWriterClosedOnStreamOperatorFactoryDeserializationError() throws Exception {
		Configuration taskConfiguration = new Configuration();
		StreamConfig streamConfig = new StreamConfig(taskConfiguration);
		streamConfig.setStreamOperatorFactory(new UnusedOperatorFactory());

		// Make sure that there is some output edge in the config so that some RecordWriter is created
		StreamConfigChainer cfg = new StreamConfigChainer(new OperatorID(42, 42), streamConfig, this);
		cfg.chain(
			new OperatorID(44, 44),
			new UnusedOperatorFactory(),
			StringSerializer.INSTANCE,
			StringSerializer.INSTANCE,
			false);
		cfg.finish();

		// Overwrite the serialized bytes to some garbage to induce deserialization exception
		taskConfiguration.setBytes(StreamConfig.SERIALIZEDUDF, new byte[42]);

		try (MockEnvironment mockEnvironment =
				new MockEnvironmentBuilder()
					.setTaskConfiguration(taskConfiguration)
					.build()) {

			mockEnvironment.addOutput(new ArrayList<>());
			StreamTask<String, TestSequentialReadingStreamOperator> streamTask =
				new NoOpStreamTask<>(mockEnvironment);

			try {
				streamTask.invoke();
				fail("Should have failed with an exception!");
			} catch (Exception ex) {
				if (!ExceptionUtils.findThrowable(ex, StreamCorruptedException.class).isPresent()) {
					throw ex;
				}
			}
		}

		assertTrue(
			RecordWriter.DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " thread is still running",
			Thread.getAllStackTraces().keySet().stream()
				.noneMatch(thread -> thread.getName().startsWith(RecordWriter.DEFAULT_OUTPUT_FLUSH_THREAD_NAME)));
	}

	@Test
	public void testProcessWithAvailableOutput() throws Exception {
		try (final MockEnvironment environment = setupEnvironment(new boolean[] {true, true})) {
			final int numberOfProcessCalls = 10;
			final AvailabilityTestInputProcessor inputProcessor = new AvailabilityTestInputProcessor(numberOfProcessCalls);
			final StreamTask task = new MockStreamTaskBuilder(environment)
				.setStreamInputProcessor(inputProcessor)
				.build();

			task.invoke();
			assertEquals(numberOfProcessCalls, inputProcessor.currentNumProcessCalls);
		}
	}

	@Test
	public void testProcessWithUnAvailableOutput() throws Exception {
		try (final MockEnvironment environment = setupEnvironment(new boolean[] {true, false})) {
			final int numberOfProcessCalls = 10;
			final AvailabilityTestInputProcessor inputProcessor = new AvailabilityTestInputProcessor(numberOfProcessCalls);
			final StreamTask task = new MockStreamTaskBuilder(environment)
				.setStreamInputProcessor(inputProcessor)
				.build();
			final MailboxExecutor executor = task.mailboxProcessor.getMainMailboxExecutor();

			final RunnableWithException completeFutureTask = () -> {
				assertEquals(1, inputProcessor.currentNumProcessCalls);
				assertTrue(task.mailboxProcessor.isDefaultActionUnavailable());
				environment.getWriter(1).getAvailableFuture().complete(null);
			};

			executor.submit(() -> {
				executor.submit(completeFutureTask, "This task will complete the future to resume process input action."); },
				"This task will submit another task to execute after processing input once.");

			task.invoke();
			assertEquals(numberOfProcessCalls, inputProcessor.currentNumProcessCalls);
		}
	}

	private MockEnvironment setupEnvironment(boolean[] outputAvailabilities) {
		final Configuration configuration = new Configuration();
		new MockStreamConfig(configuration, outputAvailabilities.length);

		final List<ResultPartitionWriter> writers = new ArrayList<>(outputAvailabilities.length);
		for (int i = 0; i < outputAvailabilities.length; i++) {
			writers.add(new AvailabilityTestResultPartitionWriter(outputAvailabilities[i]));
		}

		final MockEnvironment environment = new MockEnvironmentBuilder()
			.setTaskConfiguration(configuration)
			.build();
		environment.addOutputs(writers);
		return environment;
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static <T> OneInputStreamOperator<T, T> streamOperatorWithSnapshot(OperatorSnapshotFutures operatorSnapshotResult) throws Exception {
		@SuppressWarnings("unchecked")
		OneInputStreamOperator<T, T> operator = mock(OneInputStreamOperator.class);
		when(operator.getOperatorID()).thenReturn(new OperatorID());

		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class)))
			.thenReturn(operatorSnapshotResult);

		return operator;
	}

	private static <T> OneInputStreamOperator<T, T> streamOperatorWithSnapshotException(Exception exception) throws Exception {
		@SuppressWarnings("unchecked")
		OneInputStreamOperator<T, T> operator = mock(OneInputStreamOperator.class);
		when(operator.getOperatorID()).thenReturn(new OperatorID());

		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class)))
			.thenThrow(exception);

		return operator;
	}

	private static <T> OperatorChain<T, AbstractStreamOperator<T>> operatorChain(OneInputStreamOperator<T, T>... streamOperators) throws Exception {
		return OperatorChainTest.setupOperatorChain(streamOperators);
	}

	private static class RunningTask<T extends StreamTask<?, ?>> {
		final T streamTask;
		final CompletableFuture<Void> invocationFuture;

		RunningTask(T streamTask, CompletableFuture<Void> invocationFuture) {
			this.streamTask = streamTask;
			this.invocationFuture = invocationFuture;
		}

		void waitForTaskCompletion(boolean cancelled) throws Exception {
			try {
				invocationFuture.get();
			} catch (Exception e) {
				if (cancelled) {
					assertThat(e.getCause(), is(instanceOf(CancelTaskException.class)));
				} else {
					throw e;
				}
			}
			assertThat(streamTask.isCanceled(), is(cancelled));
		}
	}

	private static <T extends StreamTask<?, ?>> RunningTask<T> runTask(SupplierWithException<T, Exception> taskFactory) throws Exception {
		CompletableFuture<T> taskCreationFuture = new CompletableFuture<>();
		CompletableFuture<Void> invocationFuture = CompletableFuture.runAsync(
			() -> {
				T task;
				try {
					task = taskFactory.get();
					taskCreationFuture.complete(task);
				} catch (Exception e) {
					taskCreationFuture.completeExceptionally(e);
					return;
				}
				try {
					task.invoke();
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}, Executors.newSingleThreadExecutor());

		// Wait until task is created.
		return new RunningTask<>(taskCreationFuture.get(), invocationFuture);
	}

	/**
	 * Operator that does nothing.
	 *
	 * @param <T>
	 * @param <OP>
	 */
	public static class NoOpStreamTask<T, OP extends StreamOperator<T>> extends StreamTask<T, OP> {

		public NoOpStreamTask(Environment environment) throws Exception {
			super(environment);
		}

		@Override
		protected void init() throws Exception {
			inputProcessor = new EmptyInputProcessor();
		}

		@Override
		protected void cleanup() throws Exception {}
	}

	/**
	 * A stream input processor implementation used to control the returned input status based on
	 * the total number of processing calls.
	 */
	private static class AvailabilityTestInputProcessor implements StreamInputProcessor {
		private final int totalProcessCalls;
		private int currentNumProcessCalls;

		AvailabilityTestInputProcessor(int totalProcessCalls) {
			this.totalProcessCalls = totalProcessCalls;
		}

		@Override
		public InputStatus processInput()  {
			return ++currentNumProcessCalls < totalProcessCalls ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
		}

		@Override
		public CompletableFuture<Void> prepareSnapshot(
				ChannelStateWriter channelStateWriter,
				final long checkpointId) {
			return FutureUtils.completedVoidFuture();
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return AVAILABLE;
		}
	}

	private static class BlockingCloseStreamOperator extends AbstractStreamOperator<Void> {
		private static final long serialVersionUID = -9042150529568008847L;

		private static volatile OneShotLatch inClose;
		private static volatile OneShotLatch finishClose;

		@Override
		public void close() throws Exception {
			checkLatches();
			inClose.trigger();
			finishClose.await();
			super.close();
		}

		private void checkLatches() {
			Preconditions.checkNotNull(inClose);
			Preconditions.checkNotNull(finishClose);
		}

		private static void resetLatches() {
			inClose = new OneShotLatch();
			finishClose = new OneShotLatch();
		}
	}

	public static Task createTask(
			Class<? extends AbstractInvokable> invokable,
			ShuffleEnvironment shuffleEnvironment,
			StreamConfig taskConfig,
			Configuration taskManagerConfig) throws Exception {

		return new TestTaskBuilder(shuffleEnvironment)
			.setTaskManagerConfig(taskManagerConfig)
			.setInvokable(invokable)
			.setTaskConfig(taskConfig.getConfiguration())
			.build();
	}

	// ------------------------------------------------------------------------
	//  Test operators
	// ------------------------------------------------------------------------

	private static class SlowlyDeserializingOperator extends StreamSource<Long, SourceFunction<Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean canceled = false;

		public SlowlyDeserializingOperator() {
			super(new MockSourceFunction());
		}

		@Override
		public void run(Object lockingObject,
						StreamStatusMaintainer streamStatusMaintainer,
						Output<StreamRecord<Long>> collector,
						OperatorChain<?, ?> operatorChain) throws Exception {
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

	/**
	 * Mocked state backend factory which returns mocks for the operator and keyed state backends.
	 */
	public static final class TestMemoryStateBackendFactory implements StateBackendFactory<AbstractStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public AbstractStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader) {
			return new TestSpyWrapperStateBackend(createInnerBackend(config));
		}

		protected AbstractStateBackend createInnerBackend(ReadableConfig config) {
			return new MemoryStateBackend();
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	private static class MockStreamTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final OperatorChain<String, AbstractStreamOperator<String>> overrideOperatorChain;

		MockStreamTask(
				Environment env,
				OperatorChain<String, AbstractStreamOperator<String>> operatorChain,
				Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
			super(env, null, uncaughtExceptionHandler);
			this.overrideOperatorChain = operatorChain;
		}

		@Override
		protected void init() {
			// The StreamTask initializes operatorChain first on it's own in `invoke()` method.
			// Later it calls the `init()` method before actual `run()`, so we are overriding the operatorChain
			// here for test purposes.
			super.operatorChain = this.overrideOperatorChain;
			super.headOperator = super.operatorChain.getHeadOperator();
			super.inputProcessor = new EmptyInputProcessor(false);
		}

		void finishInput() {
			checkState(inputProcessor != null, "Tried to finishInput before MockStreamTask was started");
			((EmptyInputProcessor) inputProcessor).finishInput();
		}
	}

	private static class EmptyInputProcessor implements StreamInputProcessor {
		private volatile boolean isFinished;

		public EmptyInputProcessor() {
			this(true);
		}

		public EmptyInputProcessor(boolean startFinished) {
			isFinished = startFinished;
		}

		@Override
		public InputStatus processInput() throws Exception {
			return isFinished ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
		}

		@Override
		public CompletableFuture<Void> prepareSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) {
			return FutureUtils.completedVoidFuture();
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return AVAILABLE;
		}

		public void finishInput() {
			isFinished = true;
		}
	}

	private static MockStreamTask createMockStreamTask(
			Environment env,
			OperatorChain<String, AbstractStreamOperator<String>> operatorChain) throws Exception {
		return new MockStreamTask(env, operatorChain, FatalExitExceptionHandler.INSTANCE);
	}

	/**
	 * Source that instantiates the operator state backend and the keyed state backend.
	 * The created state backends can be retrieved from the static fields to check if the
	 * CloseableRegistry closed them correctly.
	 */
	public static class StateBackendTestSource extends StreamTask<Long, StreamSource<Long, SourceFunction<Long>>> {

		private static volatile boolean fail;

		public StateBackendTestSource(Environment env) throws Exception {
			super(env);
		}

		@Override
		protected void init() throws Exception {

		}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
			if (fail) {
				throw new RuntimeException();
			}
			controller.allActionsCompleted();
		}

		@Override
		protected void cleanup() throws Exception {}

		@Override
		public StreamTaskStateInitializer createStreamTaskStateInitializer() {
			final StreamTaskStateInitializer streamTaskStateManager = super.createStreamTaskStateInitializer();
			return (operatorID, operatorClassName, processingTimeService, keyContext, keySerializer, closeableRegistry, metricGroup) -> {

				final StreamOperatorStateContext controller = streamTaskStateManager.streamOperatorStateContext(
					operatorID,
					operatorClassName,
					processingTimeService,
					keyContext,
					keySerializer,
					closeableRegistry,
					metricGroup);

				return new StreamOperatorStateContext() {
					@Override
					public boolean isRestored() {
						return controller.isRestored();
					}

					@Override
					public OperatorStateBackend operatorStateBackend() {
						return controller.operatorStateBackend();
					}

					@Override
					public AbstractKeyedStateBackend<?> keyedStateBackend() {
						return controller.keyedStateBackend();
					}

					@Override
					public InternalTimeServiceManager<?> internalTimerServiceManager() {
						InternalTimeServiceManager<?> timeServiceManager = controller.internalTimerServiceManager();
						return timeServiceManager != null ? spy(timeServiceManager) : null;
					}

					@Override
					public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
						return replaceWithSpy(controller.rawOperatorStateInputs());
					}

					@Override
					public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
						return replaceWithSpy(controller.rawKeyedStateInputs());
					}

					public <T extends Closeable> T replaceWithSpy(T closeable) {
						T spyCloseable = spy(closeable);
						if (closeableRegistry.unregisterCloseable(closeable)) {
							try {
								closeableRegistry.registerCloseable(spyCloseable);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						return spyCloseable;
					}
				};
			};
		}
	}

	private static class ThreadInspectingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final long taskThreadId;
		private final ClassLoader taskClassLoader;

		/** Flag to wait until time trigger has been called. */
		private transient boolean hasTimerTriggered;

		ThreadInspectingTask(Environment env) throws Exception {
			super(env);
			Thread currentThread = Thread.currentThread();
			taskThreadId = currentThread.getId();
			taskClassLoader = currentThread.getContextClassLoader();
		}

		@Nullable
		ClassLoader getTaskClassLoader() {
			return taskClassLoader;
		}

		@Override
		protected void init() throws Exception {
			checkTaskThreadInfo();

			// Create a time trigger to validate that it would also be invoked in the task's thread.
			getHeadOperator().getProcessingTimeService().registerTimer(0, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					checkTaskThreadInfo();
					hasTimerTriggered = true;
				}
			});
		}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
			checkTaskThreadInfo();
			if (hasTimerTriggered) {
				controller.allActionsCompleted();
			}
		}

		@Override
		protected void cleanup() throws Exception {
			checkTaskThreadInfo();
		}

		private void checkTaskThreadInfo() {
			Thread currentThread = Thread.currentThread();
			Preconditions.checkState(
				taskThreadId == currentThread.getId(),
				"Task's method was called in non task thread.");
			Preconditions.checkState(
				taskClassLoader == currentThread.getContextClassLoader(),
				"Task's controller class loader has been changed during invocation.");
		}
	}

	/**
	 * A {@link ClassLoader} that delegates everything to {@link ClassLoader#getSystemClassLoader()}.
	 */
	private static class TestUserCodeClassLoader extends ClassLoader {
		public TestUserCodeClassLoader() {
			super(ClassLoader.getSystemClassLoader());
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	static class TestStreamSource<OUT, SRC extends SourceFunction<OUT>> extends StreamSource<OUT, SRC> {

		static AbstractKeyedStateBackend<?> keyedStateBackend;
		static OperatorStateBackend operatorStateBackend;
		static CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
		static CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

		public TestStreamSource(SRC sourceFunction) {
			super(sourceFunction);
		}

		@Override
		public void initializeState(StateInitializationContext controller) throws Exception {
			keyedStateBackend = (AbstractKeyedStateBackend<?>) getKeyedStateBackend();
			operatorStateBackend = getOperatorStateBackend();
			rawOperatorStateInputs =
				(CloseableIterable<StatePartitionStreamProvider>) controller.getRawOperatorStateInputs();
			rawKeyedStateInputs =
				(CloseableIterable<KeyGroupStatePartitionStreamProvider>) controller.getRawKeyedStateInputs();
			super.initializeState(controller);
		}
	}

	private static class TestingKeyedStateHandle implements KeyedStateHandle {

		private static final long serialVersionUID = -2473861305282291582L;

		private final transient CompletableFuture<Void> discardFuture = new CompletableFuture<>();

		public CompletableFuture<Void> getDiscardFuture() {
			return discardFuture;
		}

		@Override
		public KeyGroupRange getKeyGroupRange() {
			return KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
		}

		@Override
		public TestingKeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
			return this;
		}

		@Override
		public void registerSharedStates(SharedStateRegistry stateRegistry) {

		}

		@Override
		public void discardState() {
			discardFuture.complete(null);
		}

		@Override
		public long getStateSize() {
			return 0L;
		}
	}

	private static class TestingOperatorStateHandle implements OperatorStateHandle {

		private static final long serialVersionUID = 923794934187614088L;

		private final transient CompletableFuture<Void> discardFuture = new CompletableFuture<>();

		public CompletableFuture<Void> getDiscardFuture() {
			return discardFuture;
		}

		@Override
		public Map<String, StateMetaInfo> getStateNameToPartitionOffsets() {
			return Collections.emptyMap();
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			throw new IOException("Cannot open input streams in testing implementation.");
		}

		@Override
		public StreamStateHandle getDelegateStateHandle() {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public void discardState() throws Exception {
			discardFuture.complete(null);
		}

		@Override
		public long getStateSize() {
			return 0L;
		}
	}

	private static class AcknowledgeDummyEnvironment extends DummyEnvironment {

		private final CompletableFuture<Long> acknowledgeCheckpointFuture = new CompletableFuture<>();

		public CompletableFuture<Long> getAcknowledgeCheckpointFuture() {
			return acknowledgeCheckpointFuture;
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
			acknowledgeCheckpointFuture.complete(checkpointId);
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
			acknowledgeCheckpointFuture.complete(checkpointId);
		}
	}

	private static final class BlockingRunnableFuture<V> implements RunnableFuture<V> {

		private final CompletableFuture<V> future = new CompletableFuture<>();

		private final OneShotLatch signalRunLatch = new OneShotLatch();

		private final CountDownLatch continueRunLatch;

		private final V value;

		private BlockingRunnableFuture(int parties, V value) {
			this.continueRunLatch = new CountDownLatch(parties);
			this.value = value;
		}

		@Override
		public void run() {
			signalRunLatch.trigger();
			continueRunLatch.countDown();

			try {
				// poor man's barrier because it can happen that the async operations thread gets
				// interrupted by the mail box thread. The CyclicBarrier would in this case fail
				// all participants of the barrier, leaving the future uncompleted
				continueRunLatch.await();
			} catch (InterruptedException e) {
				ExceptionUtils.rethrow(e);
			}

			future.complete(value);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return future.isDone();
		}

		@Override
		public V get() throws InterruptedException, ExecutionException {
			return future.get();
		}

		@Override
		public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return future.get(timeout, unit);
		}

		void awaitRun() throws InterruptedException {
			signalRunLatch.await();
		}
	}

	private static class FailingDummyEnvironment extends DummyEnvironment {

		final RuntimeException failingCause;

		private FailingDummyEnvironment(RuntimeException failingCause) {
			this.failingCause = failingCause;
		}

		@Override
		public void declineCheckpoint(long checkpointId, Throwable cause) {
			throw failingCause;
		}

		@Override
		public void failExternally(Throwable cause) {
			throw failingCause;
		}
	}

	static final class DeclineDummyEnvironment extends DummyEnvironment {

		private long lastDeclinedCheckpointId;
		private Throwable lastDeclinedCheckpointCause;

		DeclineDummyEnvironment() {
			super("test", 1, 0);
			this.lastDeclinedCheckpointId = Long.MIN_VALUE;
			this.lastDeclinedCheckpointCause = null;
		}

		@Override
		public void declineCheckpoint(long checkpointId, Throwable cause) {
			this.lastDeclinedCheckpointId = checkpointId;
			this.lastDeclinedCheckpointCause = cause;
		}

		long getLastDeclinedCheckpointId() {
			return lastDeclinedCheckpointId;
		}

		Throwable getLastDeclinedCheckpointCause() {
			return lastDeclinedCheckpointCause;
		}
	}

	private static class UnusedOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			throw new UnsupportedOperationException("This shouldn't be called");
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			throw new UnsupportedOperationException();
		}
	}

	private static class RecoveryResultPartition extends MockResultPartitionWriter {
		private boolean isStateRecovered;

		RecoveryResultPartition() {
		}

		@Override
		public void readRecoveredState(ChannelStateReader stateReader) {
			isStateRecovered = true;
		}

		boolean isStateRecovered() {
			return isStateRecovered;
		}
	}

	private static class RecoveryInputGate extends MockIndexedInputGate {
		private final RecoveryResultPartition[] partitions;
		private boolean isStateRecovered;
		private boolean isPartitionRequested;

		RecoveryInputGate(RecoveryResultPartition[] partitions) {
			this.partitions = checkNotNull(partitions);
		}

		@Override
		public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) {
			for (RecoveryResultPartition partition : partitions) {
				checkState(partition.isStateRecovered(), "The output state recovery should happen before input state recovery.");
				checkState(!isPartitionRequested, "The partition request should happen after completing all input gates recovery.");
			}
			isStateRecovered = true;
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public void requestPartitions() {
			isPartitionRequested = true;
		}

		boolean isStateRecovered() {
			return isStateRecovered;
		}

		boolean isPartitionRequested() {
			return isPartitionRequested;
		}
	}

	private static class ClosingOperator<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {
		static AtomicBoolean closed = new AtomicBoolean();
		static AtomicInteger notified = new AtomicInteger();

		@Override
		public void open() throws Exception {
			super.open();
			closed.set(false);
			notified.set(0);
		}

		@Override
		public void close() throws Exception {
			super.close();
			closed.set(true);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			super.notifyCheckpointComplete(checkpointId);
			notified.incrementAndGet();
		}

		@Override
		public void processElement(StreamRecord<T> element) throws Exception {
		}
	}
}
