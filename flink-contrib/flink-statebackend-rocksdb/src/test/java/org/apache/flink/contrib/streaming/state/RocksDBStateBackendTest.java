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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.OperatingSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		String dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath));
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	@Test
	public void testSnap() throws Exception {
		OneShotLatch blocker = new OneShotLatch();
		OneShotLatch waiter = new OneShotLatch();
		BlockerCheckpointStreamFactory testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		testStreamFactory.setBlockerLatch(blocker);
		testStreamFactory.setWaiterLatch(waiter);
		testStreamFactory.setAfterNumberInvocations(100);

		RocksDBStateBackend backend = getStateBackend();
		Environment env = new DummyEnvironment("TestTask", 1, 0);

		final RocksDBKeyedStateBackend<Integer> keyedStateBackend = (RocksDBKeyedStateBackend<Integer>) backend.createKeyedStateBackend(
				env,
				new JobID(),
				"Test",
				IntSerializer.INSTANCE,
				2,
				new KeyGroupRange(0, 1),
				mock(TaskKvStateRegistry.class));

		ValueState<Integer> testState1 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-1", Integer.class, 0));

		ValueState<String> testState2 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-2", String.class, ""));

		final List<RocksIterator> allCreatedIterators = new ArrayList<>();
		final RocksDB realDB = keyedStateBackend.db;
		keyedStateBackend.db = spy(realDB);
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Object[] args = invocationOnMock.getArguments();
				RocksIterator rocksIterator = spy((RocksIterator) invocationOnMock.getMethod().invoke(realDB, args));
				allCreatedIterators.add(rocksIterator);
				return rocksIterator;
			}
		}).when(keyedStateBackend.db).newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));

		for (int i = 0; i < 100; ++i) {
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}

		RunnableFuture<KeyGroupsStateHandle> snapshot = keyedStateBackend.snapshot(0L, 0L, testStreamFactory);
		Thread asyncSnapshotThread = new Thread(snapshot);
		asyncSnapshotThread.start();
		//TODO replace with reset-latch wait!!!!
		Thread.sleep(100);
		for (int i = 50; i < 150; ++i) {
			if (i % 10 == 0) {
				Thread.sleep(1);
			}
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}
		blocker.trigger();
		snapshot.cancel(true);
		assertTrue(testStreamFactory.getLastCreatedStream().isClosed());
		waiter.await();
		KeyGroupsStateHandle keyGroupsStateHandle = null;
		try {
			keyGroupsStateHandle = snapshot.get();
			fail();
		} catch (Exception expected) {
			//expected.printStackTrace();
			//assertTrue(expected.getCause() instanceof IOException);
		}
		for (RocksIterator iterator : allCreatedIterators) {
			verify(iterator, times(1)).dispose();
		}

		assertNotNull(null, keyedStateBackend.db);
		RocksDB spyDB = keyedStateBackend.db;
		keyedStateBackend.dispose();

		verify(spyDB, atLeastOnce()).dispose();
		assertEquals(null, keyedStateBackend.db);

		//System.out.println(keyGroupsStateHandle.getStateSize());
		//System.out.println(keyGroupsStateHandle.getGroupRangeOffsets());
		asyncSnapshotThread.join();
	}

	static class BlockerCheckpointStreamFactory implements CheckpointStreamFactory {

		private final int maxSize;
		private int afterNumberInvocations;
		private OneShotLatch blocker;
		private OneShotLatch waiter;

		MemCheckpointStreamFactory.MemoryCheckpointOutputStream lastCreatedStream;

		public MemCheckpointStreamFactory.MemoryCheckpointOutputStream getLastCreatedStream() {
			return lastCreatedStream;
		}

		public BlockerCheckpointStreamFactory(int maxSize) {
			this.maxSize = maxSize;
		}

		public void setAfterNumberInvocations(int afterNumberInvocations) {
			this.afterNumberInvocations = afterNumberInvocations;
		}

		public void setBlockerLatch(OneShotLatch latch) {
			this.blocker = latch;
		}

		public void setWaiterLatch(OneShotLatch latch) {
			this.waiter = latch;
		}

		@Override
		public MemCheckpointStreamFactory.MemoryCheckpointOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
			this.lastCreatedStream = new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(maxSize) {

				private int afterNInvocations = afterNumberInvocations;
				private final OneShotLatch streamBlocker = blocker;
				private final OneShotLatch streamWaiter = waiter;

				@Override
				public void write(int b) throws IOException {

					if (afterNInvocations > 0) {
						--afterNInvocations;
					}

					if (0 == afterNInvocations && null != streamBlocker) {
						try {
							streamBlocker.await();
						} catch (InterruptedException ignored) {
						}
					}
					try {
						super.write(b);
					} catch (IOException ex) {
						if (null != streamWaiter) {
							streamWaiter.trigger();
						}
						throw ex;
					}

					if (0 == afterNInvocations && null != streamWaiter) {
						streamWaiter.trigger();
					}
				}

				@Override
				public void close() {
					super.close();
					if (null != streamWaiter) {
						streamWaiter.trigger();
					}
				}
			};
			return lastCreatedStream;
		}

		@Override
		public void close() throws Exception {

		}
	}
}
