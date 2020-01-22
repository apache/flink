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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.util.IOUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Snapshot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.RunnableFuture;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder.DB_INSTANCE_DIR_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
@RunWith(Parameterized.class)
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	private OneShotLatch blocker;
	private OneShotLatch waiter;
	private BlockerCheckpointStreamFactory testStreamFactory;
	private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
	private List<RocksObject> allCreatedCloseables;
	private ValueState<Integer> testState1;
	private ValueState<String> testState2;

	@Parameterized.Parameters(name = "Incremental checkpointing: {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean enableIncrementalCheckpointing;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	// Store it because we need it for the cleanup test.
	private String dbPath;
	private RocksDB db = null;
	private ColumnFamilyHandle defaultCFHandle = null;
	private final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

	public void prepareRocksDB() throws Exception {
		String dbPath = new File(tempFolder.newFolder(), DB_INSTANCE_DIR_STRING).getAbsolutePath();
		ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();

		ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		db = RocksDBOperationUtils.openDB(dbPath, Collections.emptyList(),
			columnFamilyHandles, columnOptions, optionsContainer.getDbOptions());
		defaultCFHandle = columnFamilyHandles.remove(0);
	}

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);
		Configuration configuration = new Configuration();
		configuration.setString(
			RocksDBOptions.TIMER_SERVICE_FACTORY,
			RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());
		backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return false;
	}

	// small safety net for instance cleanups, so that no native objects are left
	@After
	public void cleanupRocksDB() {
		if (keyedStateBackend != null) {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
		IOUtils.closeQuietly(defaultCFHandle);
		IOUtils.closeQuietly(db);
		IOUtils.closeQuietly(optionsContainer);

		if (allCreatedCloseables != null) {
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(1)).close();
			}
			allCreatedCloseables = null;
		}
	}

	public void setupRocksKeyedStateBackend() throws Exception {

		blocker = new OneShotLatch();
		waiter = new OneShotLatch();
		testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		testStreamFactory.setBlockerLatch(blocker);
		testStreamFactory.setWaiterLatch(waiter);
		testStreamFactory.setAfterNumberInvocations(10);

		prepareRocksDB();

		keyedStateBackend = RocksDBTestUtils.builderForTestDB(
				tempFolder.newFolder(), // this is not used anyways because the DB is injected
				IntSerializer.INSTANCE,
				spy(db),
				defaultCFHandle,
				optionsContainer.getColumnOptions())
			.setEnableIncrementalCheckpointing(enableIncrementalCheckpointing)
			.build();

		testState1 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-1", Integer.class, 0));

		testState2 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-2", String.class, ""));

		allCreatedCloseables = new ArrayList<>();

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				RocksIterator rocksIterator = spy((RocksIterator) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(rocksIterator);
				return rocksIterator;
			}
		}).when(keyedStateBackend.db).newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Snapshot snapshot = spy((Snapshot) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(snapshot);
				return snapshot;
			}
		}).when(keyedStateBackend.db).getSnapshot();

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				ColumnFamilyHandle snapshot = spy((ColumnFamilyHandle) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(snapshot);
				return snapshot;
			}
		}).when(keyedStateBackend.db).createColumnFamily(any(ColumnFamilyDescriptor.class));

		for (int i = 0; i < 100; ++i) {
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}
	}

	@Test
	public void testCorrectMergeOperatorSet() throws Exception {
		prepareRocksDB();
		final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());
		RocksDBKeyedStateBackend<Integer> test = null;

		try {
			test = RocksDBTestUtils.builderForTestDB(
				tempFolder.newFolder(),
				IntSerializer.INSTANCE,
				db,
				defaultCFHandle,
				columnFamilyOptions)
				.setEnableIncrementalCheckpointing(enableIncrementalCheckpointing)
				.build();

			ValueStateDescriptor<String> stubState1 =
				new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
			test.createInternalState(StringSerializer.INSTANCE, stubState1);
			ValueStateDescriptor<String> stubState2 =
				new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
			test.createInternalState(StringSerializer.INSTANCE, stubState2);

			// The default CF is pre-created so sum up to 2 times (once for each stub state)
			verify(columnFamilyOptions, Mockito.times(2))
				.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
		} finally {
			if (test != null) {
				IOUtils.closeQuietly(test);
				test.dispose();
			}
			columnFamilyOptions.close();
		}
	}

	@Test
	public void testReleasingSnapshotAfterBackendClosed() throws Exception {
		setupRocksKeyedStateBackend();

		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

			RocksDB spyDB = keyedStateBackend.db;

			if (!enableIncrementalCheckpointing) {
				verify(spyDB, times(1)).getSnapshot();
				verify(spyDB, times(0)).releaseSnapshot(any(Snapshot.class));
			}

			//Ensure every RocksObjects not closed yet
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(0)).close();
			}

			snapshot.cancel(true);

			this.keyedStateBackend.dispose();

			verify(spyDB, times(1)).close();
			assertEquals(true, keyedStateBackend.isDisposed());

			//Ensure every RocksObjects was closed exactly once
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(1)).close();
			}

		} finally {
			keyedStateBackend.dispose();
			keyedStateBackend = null;
		}
	}

	@Test
	public void testDismissingSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			snapshot.cancel(true);
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testDismissingSnapshotNotRunnable() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			snapshot.cancel(true);
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {

			}
			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testCompletingSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			blocker.trigger(); // allow checkpointing to start writing
			waiter.await(); // wait for snapshot stream writing to run

			SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
			KeyedStateHandle keyedStateHandle = snapshotResult.getJobManagerOwnedSnapshot();
			assertNotNull(keyedStateHandle);
			assertTrue(keyedStateHandle.getStateSize() > 0);
			assertEquals(2, keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());

			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testCancelRunningSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			snapshot.cancel(true);
			blocker.trigger(); // allow checkpointing to start writing

			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			waiter.await(); // wait for snapshot stream writing to run
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {
			}

			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testDisposeDeletesAllDirectories() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
		Collection<File> allFilesInDbDir =
			FileUtils.listFilesAndDirs(new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());
		try {
			ValueStateDescriptor<String> kvId =
				new ValueStateDescriptor<>("id", String.class, null);

			kvId.initializeSerializerUnlessSet(new ExecutionConfig());

			ValueState<String> state =
				backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.update("Hello");

			// more than just the root directory
			assertTrue(allFilesInDbDir.size() > 1);
		} finally {
			IOUtils.closeQuietly(backend);
			backend.dispose();
		}
		allFilesInDbDir =
			FileUtils.listFilesAndDirs(new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());

		// just the root directory left
		assertEquals(1, allFilesInDbDir.size());
	}

	@Test
	public void testSharedIncrementalStateDeRegistration() throws Exception {
		if (enableIncrementalCheckpointing) {
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
			try {
				ValueStateDescriptor<String> kvId =
					new ValueStateDescriptor<>("id", String.class, null);

				kvId.initializeSerializerUnlessSet(new ExecutionConfig());

				ValueState<String> state =
					backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				Queue<IncrementalRemoteKeyedStateHandle> previousStateHandles = new LinkedList<>();
				SharedStateRegistry sharedStateRegistry = spy(new SharedStateRegistry());
				for (int checkpointId = 0; checkpointId < 3; ++checkpointId) {

					reset(sharedStateRegistry);

					backend.setCurrentKey(checkpointId);
					state.update("Hello-" + checkpointId);

					RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = backend.snapshot(
						checkpointId,
						checkpointId,
						createStreamFactory(),
						CheckpointOptions.forCheckpointWithDefaultLocation());

					snapshot.run();

					SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();

					IncrementalRemoteKeyedStateHandle stateHandle =
						(IncrementalRemoteKeyedStateHandle) snapshotResult.getJobManagerOwnedSnapshot();

					Map<StateHandleID, StreamStateHandle> sharedState =
						new HashMap<>(stateHandle.getSharedState());

					stateHandle.registerSharedStates(sharedStateRegistry);

					for (Map.Entry<StateHandleID, StreamStateHandle> e : sharedState.entrySet()) {
						verify(sharedStateRegistry).registerReference(
							stateHandle.createSharedStateRegistryKeyFromFileName(e.getKey()),
							e.getValue());
					}

					previousStateHandles.add(stateHandle);
					backend.notifyCheckpointComplete(checkpointId);

					//-----------------------------------------------------------------

					if (previousStateHandles.size() > 1) {
						checkRemove(previousStateHandles.remove(), sharedStateRegistry);
					}
				}

				while (!previousStateHandles.isEmpty()) {

					reset(sharedStateRegistry);

					checkRemove(previousStateHandles.remove(), sharedStateRegistry);
				}
			} finally {
				IOUtils.closeQuietly(backend);
				backend.dispose();
			}
		}
	}

	private void checkRemove(IncrementalRemoteKeyedStateHandle remove, SharedStateRegistry registry) throws Exception {
		for (StateHandleID id : remove.getSharedState().keySet()) {
			verify(registry, times(0)).unregisterReference(
				remove.createSharedStateRegistryKeyFromFileName(id));
		}

		remove.discardState();

		for (StateHandleID id : remove.getSharedState().keySet()) {
			verify(registry).unregisterReference(
				remove.createSharedStateRegistryKeyFromFileName(id));
		}
	}

	private void runStateUpdates() throws Exception{
		for (int i = 50; i < 150; ++i) {
			if (i % 10 == 0) {
				Thread.sleep(1);
			}
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}
	}

	private void verifyRocksObjectsReleased() {
		//Ensure every RocksObject was closed exactly once
		for (RocksObject rocksCloseable : allCreatedCloseables) {
			verify(rocksCloseable, times(1)).close();
		}

		assertNotNull(null, keyedStateBackend.db);
		RocksDB spyDB = keyedStateBackend.db;

		if (!enableIncrementalCheckpointing) {
			verify(spyDB, times(1)).getSnapshot();
			verify(spyDB, times(1)).releaseSnapshot(any(Snapshot.class));
		}

		keyedStateBackend.dispose();
		verify(spyDB, times(1)).close();
		assertEquals(true, keyedStateBackend.isDisposed());
	}

	private static class AcceptAllFilter implements IOFileFilter {
		@Override
		public boolean accept(File file) {
			return true;
		}

		@Override
		public boolean accept(File file, String s) {
			return true;
		}
	}
}
