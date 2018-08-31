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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.testutils.BackendForTestStream;
import org.apache.flink.runtime.state.testutils.BackendForTestStream.StreamFactory;
import org.apache.flink.runtime.state.testutils.TestCheckpointStreamFactory;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.FIRST_BIT_IN_BYTE_MASK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.setMetaDataFollowsFlagInKey;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests for asynchronous RocksDB Key/Value state checkpoints.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*", "org.apache.log4j.*"})
@SuppressWarnings("serial")
public class RocksDBAsyncSnapshotTest extends TestLogger {

	/**
	 * Temporary fold for test.
	 */
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * This ensures that asynchronous state handles are actually materialized asynchronously.
	 *
	 * <p>We use latches to block at various stages and see if the code still continues through
	 * the parts that are not asynchronous. If the checkpoint is not done asynchronously the
	 * test will simply lock forever.
	 */
	@Test
	public void testFullyAsyncSnapshot() throws Exception {

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		testHarness.configureForKeyedStream(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		File dbDir = temporaryFolder.newFolder();

		RocksDBStateBackend backend = new RocksDBStateBackend(new MemoryStateBackend());
		backend.setDbStoragePath(dbDir.getAbsolutePath());

		streamConfig.setStateBackend(backend);

		streamConfig.setStreamOperator(new AsyncCheckpointOperator());
		streamConfig.setOperatorID(new OperatorID());

		final OneShotLatch delayCheckpointLatch = new OneShotLatch();
		final OneShotLatch ensureCheckpointLatch = new OneShotLatch();

		CheckpointResponder checkpointResponderMock = new CheckpointResponder() {

			@Override
			public void acknowledgeCheckpoint(
				JobID jobID,
				ExecutionAttemptID executionAttemptID,
				long checkpointId,
				CheckpointMetrics checkpointMetrics,
				TaskStateSnapshot subtaskState) {
				// block on the latch, to verify that triggerCheckpoint returns below,
				// even though the async checkpoint would not finish
				try {
					delayCheckpointLatch.await();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				boolean hasManagedKeyedState = false;
				for (Map.Entry<OperatorID, OperatorSubtaskState> entry : subtaskState.getSubtaskStateMappings()) {
					OperatorSubtaskState state = entry.getValue();
					if (state != null) {
						hasManagedKeyedState |= state.getManagedKeyedState() != null;
					}
				}

				// should be one k/v state
				assertTrue(hasManagedKeyedState);

				// we now know that the checkpoint went through
				ensureCheckpointLatch.trigger();
			}

			@Override
			public void declineCheckpoint(
				JobID jobID, ExecutionAttemptID executionAttemptID,
				long checkpointId, Throwable cause) {

			}
		};

		JobID jobID = new JobID();
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(0L, 0L);
		TestTaskStateManager taskStateManagerTestMock = new TestTaskStateManager(
			jobID,
			executionAttemptID,
			checkpointResponderMock,
			TestLocalRecoveryConfig.disabled());

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize,
			taskStateManagerTestMock);

		testHarness.invoke(mockEnv);

		final OneInputStreamTask<String, String> task = testHarness.getTask();

		// wait for the task to be running
		for (Field field: StreamTask.class.getDeclaredFields()) {
			if (field.getName().equals("isRunning")) {
				field.setAccessible(true);
				while (!field.getBoolean(task)) {
					Thread.sleep(10);
				}
			}
		}

		task.triggerCheckpoint(new CheckpointMetaData(42, 17), CheckpointOptions.forCheckpointWithDefaultLocation());

		testHarness.processElement(new StreamRecord<>("Wohoo", 0));

		// now we allow the checkpoint
		delayCheckpointLatch.trigger();

		// wait for the checkpoint to go through
		ensureCheckpointLatch.await();

		testHarness.endInput();

		ExecutorService threadPool = task.getAsyncOperationsThreadPool();
		threadPool.shutdown();
		Assert.assertTrue(threadPool.awaitTermination(60_000, TimeUnit.MILLISECONDS));

		testHarness.waitForTaskCompletion();
		if (mockEnv.wasFailedExternally()) {
			fail("Unexpected exception during execution.");
		}
	}

	/**
	 * This tests ensures that canceling of asynchronous snapshots works as expected and does not block.
	 */
	@Test
	public void testCancelFullyAsyncCheckpoints() throws Exception {
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();

		testHarness.configureForKeyedStream(value -> value, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		File dbDir = temporaryFolder.newFolder();

		final RocksDBStateBackend.PriorityQueueStateType timerServicePriorityQueueType = RocksDBStateBackend.PriorityQueueStateType.valueOf(RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue());

		final int skipStreams;

		if (timerServicePriorityQueueType == RocksDBStateBackend.PriorityQueueStateType.HEAP) {
			// we skip the first created stream, because it is used to checkpoint the timer service, which is
			// currently not asynchronous.
			skipStreams = 1;
		} else if (timerServicePriorityQueueType == RocksDBStateBackend.PriorityQueueStateType.ROCKSDB) {
			skipStreams = 0;
		} else {
			throw new AssertionError(String.format("Unknown timer service priority queue type %s.", timerServicePriorityQueueType));
		}

		// this is the proper instance that we need to call.
		BlockerCheckpointStreamFactory blockerCheckpointStreamFactory =
			new BlockerCheckpointStreamFactory(4 * 1024 * 1024) {

			int count = skipStreams;

			@Override
			public CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) throws IOException {
				if (count > 0) {
					--count;
					return new BlockingCheckpointOutputStream(
						new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(maxSize),
						null,
						null,
						Integer.MAX_VALUE);
				} else {
					return super.createCheckpointStateOutputStream(scope);
				}
			}
		};

		// to avoid serialization of the above factory instance, we need to pass it in
		// through a static variable

		StateBackend stateBackend = new BackendForTestStream(new StaticForwardFactory(blockerCheckpointStreamFactory));

		RocksDBStateBackend backend = new RocksDBStateBackend(stateBackend);
		backend.setDbStoragePath(dbDir.getAbsolutePath());

		streamConfig.setStateBackend(backend);

		streamConfig.setStreamOperator(new AsyncCheckpointOperator());
		streamConfig.setOperatorID(new OperatorID());

		TestTaskStateManager taskStateManagerTestMock = new TestTaskStateManager();

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize,
				taskStateManagerTestMock);

		blockerCheckpointStreamFactory.setBlockerLatch(new OneShotLatch());
		blockerCheckpointStreamFactory.setWaiterLatch(new OneShotLatch());

		testHarness.invoke(mockEnv);

		final OneInputStreamTask<String, String> task = testHarness.getTask();

		// wait for the task to be running
		for (Field field: StreamTask.class.getDeclaredFields()) {
			if (field.getName().equals("isRunning")) {
				field.setAccessible(true);
				while (!field.getBoolean(task)) {
					Thread.sleep(10);
				}
			}
		}

		task.triggerCheckpoint(
			new CheckpointMetaData(42, 17),
			CheckpointOptions.forCheckpointWithDefaultLocation());

		testHarness.processElement(new StreamRecord<>("Wohoo", 0));
		blockerCheckpointStreamFactory.getWaiterLatch().await();
		task.cancel();
		blockerCheckpointStreamFactory.getBlockerLatch().trigger();
		testHarness.endInput();

		ExecutorService threadPool = task.getAsyncOperationsThreadPool();
		threadPool.shutdown();
		Assert.assertTrue(threadPool.awaitTermination(60_000, TimeUnit.MILLISECONDS));

		Set<BlockingCheckpointOutputStream> createdStreams = blockerCheckpointStreamFactory.getAllCreatedStreams();

		for (BlockingCheckpointOutputStream stream : createdStreams) {
			Assert.assertTrue(
				"Not all of the " + createdStreams.size() + " created streams have been closed.",
				stream.isClosed());
		}

		try {
			testHarness.waitForTaskCompletion();
			fail("Operation completed. Cancel failed.");
		} catch (Exception expected) {

			Throwable cause = expected.getCause();

			if (!(cause instanceof CancelTaskException)) {
				fail("Unexpected exception: " + expected);
			}
		}
	}

	/**
	 * Test that the snapshot files are cleaned up in case of a failure during the snapshot
	 * procedure.
	 */
	@Test
	public void testCleanupOfSnapshotsInFailureCase() throws Exception {
		long checkpointId = 1L;
		long timestamp = 42L;

		Environment env = new DummyEnvironment("test task", 1, 0);

		final IOException testException = new IOException("Test exception");
		CheckpointStateOutputStream outputStream = spy(new FailingStream(testException));

		RocksDBStateBackend backend = new RocksDBStateBackend((StateBackend) new MemoryStateBackend());

		backend.setDbStoragePath(temporaryFolder.newFolder().toURI().toString());

		AbstractKeyedStateBackend<Void> keyedStateBackend = backend.createKeyedStateBackend(
			env,
			new JobID(),
			"test operator",
			VoidSerializer.INSTANCE,
			1,
			new KeyGroupRange(0, 0),
			null);

		try {

			keyedStateBackend.restore(null);

			// register a state so that the state backend has to checkpoint something
			keyedStateBackend.getPartitionedState(
				"namespace",
				StringSerializer.INSTANCE,
				new ValueStateDescriptor<>("foobar", String.class));

			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture = keyedStateBackend.snapshot(
				checkpointId, timestamp,
				new TestCheckpointStreamFactory(() -> outputStream),
				CheckpointOptions.forCheckpointWithDefaultLocation());

			try {
				FutureUtil.runIfNotDoneAndGet(snapshotFuture);
				fail("Expected an exception to be thrown here.");
			} catch (ExecutionException e) {
				Assert.assertEquals(testException, e.getCause());
			}

			verify(outputStream).close();
		} finally {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
	}

	@Test
	public void testConsistentSnapshotSerializationFlagsAndMasks() {

		Assert.assertEquals(0xFFFF, END_OF_KEY_GROUP_MARK);
		Assert.assertEquals(0x80, FIRST_BIT_IN_BYTE_MASK);

		byte[] expectedKey = new byte[] {42, 42};
		byte[] modKey = expectedKey.clone();

		Assert.assertFalse(hasMetaDataFollowsFlag(modKey));

		setMetaDataFollowsFlagInKey(modKey);
		Assert.assertTrue(hasMetaDataFollowsFlag(modKey));

		clearMetaDataFollowsFlag(modKey);
		Assert.assertFalse(hasMetaDataFollowsFlag(modKey));

		Assert.assertTrue(Arrays.equals(expectedKey, modKey));
	}

	// ------------------------------------------------------------------------

	private static class AsyncCheckpointOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String> {

		@Override
		public void open() throws Exception {
			super.open();

			// also get the state in open, this way we are sure that it was created before
			// we trigger the test checkpoint
			ValueState<String> state = getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("count", StringSerializer.INSTANCE));

		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			// we also don't care

			ValueState<String> state = getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("count", StringSerializer.INSTANCE));

			state.update(element.getValue());
		}
	}

	// ------------------------------------------------------------------------
	// failing stream
	// ------------------------------------------------------------------------

	private static class StaticForwardFactory implements StreamFactory {

		static CheckpointStreamFactory factory;

		StaticForwardFactory(CheckpointStreamFactory factory) {
			StaticForwardFactory.factory = factory;
		}

		@Override
		public CheckpointStateOutputStream get() throws IOException {
			return factory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
		}
	}

	private static class FailingStream extends CheckpointStateOutputStream {

		private final IOException testException;

		FailingStream(IOException testException) {
			this.testException = testException;
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getPos() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void write(int b) throws IOException {
			throw testException;
		}

		@Override
		public void flush() throws IOException {
			throw testException;
		}

		@Override
		public void sync() throws IOException {
			throw testException;
		}

		@Override
		public void close() {
			throw new UnsupportedOperationException();
		}
	}
}
