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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.AsynchronousException;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OperatingSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for asynchronous RocksDB Key/Value state checkpoints.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class, FileSystem.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
@SuppressWarnings("serial")
public class RocksDBAsyncSnapshotTest {

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	/**
	 * This ensures that asynchronous state handles are actually materialized asynchronously.
	 *
	 * <p>We use latches to block at various stages and see if the code still continues through
	 * the parts that are not asynchronous. If the checkpoint is not done asynchronously the
	 * test will simply lock forever.
	 */
	@Test
	public void testFullyAsyncSnapshot() throws Exception {

		LocalFileSystem localFS = new LocalFileSystem();
		localFS.initialize(new URI("file:///"), new Configuration());
		PowerMockito.stub(PowerMockito.method(FileSystem.class, "get", URI.class, Configuration.class)).toReturn(localFS);

		final OneInputStreamTask<String, String> task = new OneInputStreamTask<>();

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(task, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.configureForKeyedStream(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		File dbDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "state");
		File chkDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "snapshots");

		RocksDBStateBackend backend = new RocksDBStateBackend(chkDir.getAbsoluteFile().toURI(), new MemoryStateBackend());
		backend.setDbStoragePath(dbDir.getAbsolutePath());

		streamConfig.setStateBackend(backend);

		streamConfig.setStreamOperator(new AsyncCheckpointOperator());

		final OneShotLatch delayCheckpointLatch = new OneShotLatch();
		final OneShotLatch ensureCheckpointLatch = new OneShotLatch();

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize) {

			@Override
			public void acknowledgeCheckpoint(
					CheckpointMetaData checkpointMetaData,
					SubtaskState checkpointStateHandles) {

				super.acknowledgeCheckpoint(checkpointMetaData);

				// block on the latch, to verify that triggerCheckpoint returns below,
				// even though the async checkpoint would not finish
				try {
					delayCheckpointLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// should be one k/v state
				assertNotNull(checkpointStateHandles.getManagedKeyedState());

				// we now know that the checkpoint went through
				ensureCheckpointLatch.trigger();
			}
		};

		testHarness.invoke(mockEnv);

		// wait for the task to be running
		for (Field field: StreamTask.class.getDeclaredFields()) {
			if (field.getName().equals("isRunning")) {
				field.setAccessible(true);
				while (!field.getBoolean(task)) {
					Thread.sleep(10);
				}
			}
		}

		task.triggerCheckpoint(new CheckpointMetaData(42, 17));

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
			Assert.fail("Unexpected exception during execution.");
		}
	}

	/**
	 * This tests ensures that canceling of asynchronous snapshots works as expected and does not block.
	 * @throws Exception
	 */
	@Test
	@Ignore
	public void testCancelFullyAsyncCheckpoints() throws Exception {
		LocalFileSystem localFS = new LocalFileSystem();
		localFS.initialize(new URI("file:///"), new Configuration());
		PowerMockito.stub(PowerMockito.method(FileSystem.class, "get", URI.class, Configuration.class)).toReturn(localFS);

		final OneInputStreamTask<String, String> task = new OneInputStreamTask<>();

		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(task, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.configureForKeyedStream(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();

		File dbDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "state");
		File chkDir = new File(new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString()), "snapshots");

		BlockingStreamMemoryStateBackend memoryStateBackend = new BlockingStreamMemoryStateBackend();

		RocksDBStateBackend backend = new RocksDBStateBackend(chkDir.getAbsoluteFile().toURI(), memoryStateBackend);
		backend.setDbStoragePath(dbDir.getAbsolutePath());

		streamConfig.setStateBackend(backend);

		streamConfig.setStreamOperator(new AsyncCheckpointOperator());

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize);

		BlockingStreamMemoryStateBackend.waitFirstWriteLatch = new OneShotLatch();
		BlockingStreamMemoryStateBackend.unblockCancelLatch = new OneShotLatch();

		testHarness.invoke(mockEnv);

		// wait for the task to be running
		for (Field field: StreamTask.class.getDeclaredFields()) {
			if (field.getName().equals("isRunning")) {
				field.setAccessible(true);
				while (!field.getBoolean(task)) {
					Thread.sleep(10);
				}
			}
		}

		task.triggerCheckpoint(new CheckpointMetaData(42, 17));
		testHarness.processElement(new StreamRecord<>("Wohoo", 0));
		BlockingStreamMemoryStateBackend.waitFirstWriteLatch.await();
		task.cancel();
		BlockingStreamMemoryStateBackend.unblockCancelLatch.trigger();
		testHarness.endInput();
		try {

			ExecutorService threadPool = task.getAsyncOperationsThreadPool();
			threadPool.shutdown();
			Assert.assertTrue(threadPool.awaitTermination(60_000, TimeUnit.MILLISECONDS));
			testHarness.waitForTaskCompletion();

			if (mockEnv.wasFailedExternally()) {
				throw new AsynchronousException(new InterruptedException("Exception was thrown as expected."));
			}
			Assert.fail("Operation completed. Cancel failed.");
		} catch (Exception expected) {
			AsynchronousException asynchronousException = null;

			if (expected instanceof AsynchronousException) {
				asynchronousException = (AsynchronousException) expected;
			} else if (expected.getCause() instanceof AsynchronousException) {
				asynchronousException = (AsynchronousException) expected.getCause();
			} else {
				Assert.fail("Unexpected exception: " + expected);
			}

			// we expect the exception from canceling snapshots
			Throwable innerCause = asynchronousException.getCause();
			Assert.assertTrue("Unexpected inner cause: " + innerCause,
					innerCause instanceof CancellationException //future canceled
							|| innerCause instanceof InterruptedException); //thread interrupted
		}
	}

	@Test
	public void testConsistentSnapshotSerializationFlagsAndMasks() {

		Assert.assertEquals(0xFFFF, RocksDBKeyedStateBackend.RocksDBSnapshotOperation.END_OF_KEY_GROUP_MARK);
		Assert.assertEquals(0x80, RocksDBKeyedStateBackend.RocksDBSnapshotOperation.FIRST_BIT_IN_BYTE_MASK);

		byte[] expectedKey = new byte[] {42, 42};
		byte[] modKey = expectedKey.clone();

		Assert.assertFalse(RocksDBKeyedStateBackend.RocksDBSnapshotOperation.hasMetaDataFollowsFlag(modKey));

		RocksDBKeyedStateBackend.RocksDBSnapshotOperation.setMetaDataFollowsFlagInKey(modKey);
		Assert.assertTrue(RocksDBKeyedStateBackend.RocksDBSnapshotOperation.hasMetaDataFollowsFlag(modKey));

		RocksDBKeyedStateBackend.RocksDBSnapshotOperation.clearMetaDataFollowsFlag(modKey);
		Assert.assertFalse(RocksDBKeyedStateBackend.RocksDBSnapshotOperation.hasMetaDataFollowsFlag(modKey));

		Assert.assertTrue(Arrays.equals(expectedKey, modKey));
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates us a CheckpointStateOutputStream that blocks write ops on a latch to delay writing of snapshots.
	 */
	static class BlockingStreamMemoryStateBackend extends MemoryStateBackend {

		public static volatile OneShotLatch waitFirstWriteLatch = null;

		public static volatile OneShotLatch unblockCancelLatch = null;

		private volatile boolean closed = false;

		@Override
		public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
			return new MemCheckpointStreamFactory(4 * 1024 * 1024) {
				@Override
				public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {

					return new MemoryCheckpointOutputStream(4 * 1024 * 1024) {
						@Override
						public void write(int b) throws IOException {
							waitFirstWriteLatch.trigger();
							try {
								unblockCancelLatch.await();
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							if(closed) {
								throw new IOException("Stream closed.");
							}
							super.write(b);
						}

						@Override
						public void write(byte[] b, int off, int len) throws IOException {
							waitFirstWriteLatch.trigger();
							try {
								unblockCancelLatch.await();
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							if(closed) {
								throw new IOException("Stream closed.");
							}
							super.write(b, off, len);
						}

						@Override
						public void close() {
							closed = true;
							super.close();
						}
					};
				}
			};
		}
	}

	public static class AsyncCheckpointOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String>, StreamCheckpointedOperator {

		@Override
		public void open() throws Exception {
			super.open();

			// also get the state in open, this way we are sure that it was created before
			// we trigger the test checkpoint
			ValueState<String> state = getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("count",
							StringSerializer.INSTANCE, "hello"));

		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			// we also don't care

			ValueState<String> state = getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					new ValueStateDescriptor<>("count",
							StringSerializer.INSTANCE, "hello"));

			state.update(element.getValue());
		}

		@Override
		public void snapshotState(
				FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
			// do nothing so that we don't block
		}

		@Override
		public void restoreState(FSDataInputStream in) throws Exception {
			// do nothing so that we don't block
		}

	}

	public static class DummyMapFunction<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) { return value; }
	}
}
