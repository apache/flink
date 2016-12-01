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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsynchronousKvStateSnapshot;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.runtime.tasks.StreamTaskStateList;
import org.apache.flink.util.OperatingSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests for asynchronous RocksDB Key/Value state checkpoints.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class, FileSystem.class})
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
@SuppressWarnings("serial")
public class RocksDBAsyncKVSnapshotTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
	public void testAsyncCheckpoints() throws Exception {
		LocalFileSystem localFS = new LocalFileSystem();
		localFS.initialize(new URI("file:///"), new Configuration());
		PowerMockito.stub(PowerMockito.method(FileSystem.class, "get", URI.class, Configuration.class)).toReturn(localFS);

		final OneShotLatch delayCheckpointLatch = new OneShotLatch();
		final OneShotLatch ensureCheckpointLatch = new OneShotLatch();

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

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize) {

			@Override
			public void acknowledgeCheckpoint(long checkpointId) {
				super.acknowledgeCheckpoint(checkpointId);
			}

			@Override
			public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
				super.acknowledgeCheckpoint(checkpointId, state);

				// block on the latch, to verify that triggerCheckpoint returns below,
				// even though the async checkpoint would not finish
				try {
					delayCheckpointLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				assertTrue(state instanceof StreamTaskStateList);
				StreamTaskStateList stateList = (StreamTaskStateList) state;

				// should be only one k/v state
				StreamTaskState taskState = stateList.getState(this.getUserClassLoader())[0];
				assertEquals(1, taskState.getKvStates().size());

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

		testHarness.processElement(new StreamRecord<>("Wohoo", 0));

		task.triggerCheckpoint(42, 17);

		// now we allow the checkpoint
		delayCheckpointLatch.trigger();

		// wait for the checkpoint to go through
		ensureCheckpointLatch.await();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	/**
	 * This ensures that asynchronous state handles are actually materialized asynchronously.
	 *
	 * <p>We use latches to block at various stages and see if the code still continues through
	 * the parts that are not asynchronous. If the checkpoint is not done asynchronously the
	 * test will simply lock forever.
	 */
	@Test
	public void testFullyAsyncCheckpoints() throws Exception {
		LocalFileSystem localFS = new LocalFileSystem();
		localFS.initialize(new URI("file:///"), new Configuration());
		PowerMockito.stub(PowerMockito.method(FileSystem.class, "get", URI.class, Configuration.class)).toReturn(localFS);

		final OneShotLatch delayCheckpointLatch = new OneShotLatch();
		final OneShotLatch ensureCheckpointLatch = new OneShotLatch();

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
		backend.enableFullyAsyncSnapshots();

		streamConfig.setStateBackend(backend);

		streamConfig.setStreamOperator(new AsyncCheckpointOperator());

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize) {

			@Override
			public void acknowledgeCheckpoint(long checkpointId) {
				super.acknowledgeCheckpoint(checkpointId);
			}

			@Override
			public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
				super.acknowledgeCheckpoint(checkpointId, state);

				// block on the latch, to verify that triggerCheckpoint returns below,
				// even though the async checkpoint would not finish
				try {
					delayCheckpointLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				assertTrue(state instanceof StreamTaskStateList);
				StreamTaskStateList stateList = (StreamTaskStateList) state;

				// should be only one k/v state
				StreamTaskState taskState = stateList.getState(this.getUserClassLoader())[0];
				assertEquals(1, taskState.getKvStates().size());

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

		testHarness.processElement(new StreamRecord<>("Wohoo", 0));

		task.triggerCheckpoint(42, 17);

		// now we allow the checkpoint
		delayCheckpointLatch.trigger();

		// wait for the checkpoint to go through
		ensureCheckpointLatch.await();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}

	@Test
	public void testCleanupOfFullyAsyncSnapshotsInFailureCase() throws Exception {
		long checkpointId = 1L;
		long timestamp = 42L;

		File chkDir = temporaryFolder.newFolder();
		File dbDir = temporaryFolder.newFolder();

		Environment env = new DummyEnvironment("test task", 1, 0);
		AbstractStateBackend.CheckpointStateOutputStream outputStream = mock(AbstractStateBackend.CheckpointStateOutputStream.class);
		doThrow(new IOException("Test exception")).when(outputStream).write(anyInt());

		RocksDBStateBackend backend = spy(new RocksDBStateBackend(chkDir.getAbsoluteFile().toURI(), new MemoryStateBackend()));
		doReturn(outputStream).when(backend).createCheckpointStateOutputStream(anyLong(), anyLong());
		backend.setDbStoragePath(dbDir.getAbsolutePath());
		backend.enableFullyAsyncSnapshots();

		backend.initializeForJob(
			env,
			"test operator",
			VoidSerializer.INSTANCE
		);
		backend.getPartitionedState(null, VoidSerializer.INSTANCE, new ValueStateDescriptor<>("foobar", Object.class, new Object()));

		Map<String, KvStateSnapshot<?, ?, ?, ?, ?>> kvStateSnapshotHashMap = backend.snapshotPartitionedState(checkpointId, timestamp);

		for (KvStateSnapshot<?, ?, ?, ?, ?> kvStateSnapshot : kvStateSnapshotHashMap.values()) {
			if (kvStateSnapshot instanceof AsynchronousKvStateSnapshot) {
				AsynchronousKvStateSnapshot<?, ?, ?, ?, ?> asynchronousKvStateSnapshot = (AsynchronousKvStateSnapshot<?, ?, ?, ?, ?>) kvStateSnapshot;
				try {
					asynchronousKvStateSnapshot.materialize();
					fail("Expected an Exception here.");
				} catch (Exception expected) {
					//expected exception
				}
			} else {
				fail("Expected an asynchronous kv state snapshot.");
			}
		}

		verify(outputStream).close();
	}


	// ------------------------------------------------------------------------

	public static class AsyncCheckpointOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String> {

		@Override
		public void open() throws Exception {
			super.open();

			// also get the state in open, this way we are sure that it was created before
			// we trigger the test checkpoint
			ValueState<String> state = getPartitionedState(null,
					VoidSerializer.INSTANCE,
					new ValueStateDescriptor<>("count",
							StringSerializer.INSTANCE, "hello"));

		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			// we also don't care

			ValueState<String> state = getPartitionedState(null,
					VoidSerializer.INSTANCE,
					new ValueStateDescriptor<>("count",
							StringSerializer.INSTANCE, "hello"));

			state.update(element.getValue());
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			// not interested
		}
	}
	
	public static class DummyMapFunction<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) { return value; }
	}
}
