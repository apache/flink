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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.persistence.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultCompletedCheckpointStore}.
 */
public class DefaultCompletedCheckpointStoreTest extends TestLogger {

	private final long timeout = 100L;

	private TestingStateHandleStore.Builder<CompletedCheckpoint> builder;

	private TestingRetrievableStateStorageHelper<CompletedCheckpoint> checkpointStorageHelper;

	private ExecutorService executorService;

	@Before
	public void setup() {
		builder = TestingStateHandleStore.builder();
		checkpointStorageHelper = new TestingRetrievableStateStorageHelper<>();
		executorService = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("IO-Executor"));
	}

	@After
	public void after() {
		executorService.shutdownNow();
	}

	/**
	 * We have three completed checkpoints(1, 2, 3) in the state handle store. We expect that
	 * {@link DefaultCompletedCheckpointStore#recover()} should recover the sorted checkpoints by name.
	 */
	@Test
	public void testRecoverSortedCheckpoints() throws Exception {
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();

		final List<CompletedCheckpoint> recoveredCompletedCheckpoint = completedCheckpointStore.getAllCheckpoints();
		assertThat(recoveredCompletedCheckpoint.size(), is(3));
		final List<Long> checkpointIds = recoveredCompletedCheckpoint.stream()
			.map(CompletedCheckpoint::getCheckpointID)
			.collect(Collectors.toList());
		assertThat(checkpointIds, contains(1L, 2L, 3L));

	}

	/**
	 * We got an {@link IOException} when retrieving checkpoint 2. It should be skipped.
	 */
	@Test
	public void testCorruptDataInStateHandleStoreShouldBeSkipped() throws Exception {
		final long corruptCkpId = 2L;
		checkpointStorageHelper.setRetrieveStateFunction(state -> {
			if (state.getCheckpointID() == corruptCkpId) {
				throw new IOException("Failed to retrieve checkpoint " + corruptCkpId);
			}
			return state;
		});

		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();

		final List<CompletedCheckpoint> recoveredCompletedCheckpoint = completedCheckpointStore.getAllCheckpoints();
		assertThat(recoveredCompletedCheckpoint.size(), is(2));
		final List<Long> checkpointIds = recoveredCompletedCheckpoint.stream()
			.map(CompletedCheckpoint::getCheckpointID)
			.collect(Collectors.toList());
		// Checkpoint 2 should be skipped.
		assertThat(checkpointIds, contains(1L, 3L));
	}

	/**
	 * {@link DefaultCompletedCheckpointStore#recover()} should throw exception when all the checkpoints retrieved
	 * failed while the checkpoint pointers are not empty.
	 */
	@Test
	public void testRecoverFailedWhenRetrieveCheckpointAllFailed() {
		final int ckpNum = 3;
		checkpointStorageHelper.setRetrieveStateFunction((state) -> {
			throw new IOException("Failed to retrieve checkpoint " + state.getCheckpointID());
		});

		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(ckpNum))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		try {
			completedCheckpointStore.recover();
			fail("We should get an exception when retrieving state failed.");
		} catch (Exception ex) {
			final String errMsg = "Could not read any of the " + ckpNum + " checkpoints from storage.";
			assertThat(ex, FlinkMatchers.containsMessage(errMsg));
		}
	}

	@Test
	public void testAddCheckpointSuccessfullyShouldRemoveOldOnes () throws Exception {
		final int num = 1;
		final CompletableFuture<CompletedCheckpoint> addFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setAddFunction((ignore, ckp) -> {
				addFuture.complete(ckp);
				return null;
			})
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));

		final long ckpId = 100L;
		final CompletedCheckpoint ckp = CompletedCheckpointStoreTest.createCheckpoint(ckpId, new SharedStateRegistry());
		completedCheckpointStore.addCheckpoint(ckp, new CheckpointsCleaner(), () -> {});

		// We should persist the completed checkpoint to state handle store.
		final CompletedCheckpoint addedCkp = addFuture.get(timeout, TimeUnit.MILLISECONDS);
		assertThat(addedCkp.getCheckpointID(), is(ckpId));

		// Check the old checkpoint is removed and new one is added.
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(ckpId));
	}

	@Test
	public void testAddCheckpointFailedShouldNotRemoveOldOnes() throws Exception {
		final int num = 1;
		final String errMsg = "Add to state handle failed.";
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setAddFunction((ignore, ckp) -> {
				throw new FlinkException(errMsg);
			})
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));

		final long ckpId = 100L;
		final CompletedCheckpoint ckp = CompletedCheckpointStoreTest.createCheckpoint(ckpId, new SharedStateRegistry());

		try {
			completedCheckpointStore.addCheckpoint(ckp, new CheckpointsCleaner(), () -> {});
			fail("We should get an exception when add checkpoint to failed..");
		} catch (FlinkException ex) {
			assertThat(ex, FlinkMatchers.containsMessage(errMsg));
		}
		// Check the old checkpoint still exists.
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));
	}

	@Test
	public void testShutdownShouldDiscardStateHandleWhenJobIsGloballyTerminalState() throws Exception {
		final int num = 3;
		final AtomicInteger removeCalledNum = new AtomicInteger(0);
		final CompletableFuture<Void> clearEntriesAllFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setRemoveFunction(ignore -> {
				removeCalledNum.incrementAndGet();
				return true;
			})
			.setClearEntriesRunnable(() -> clearEntriesAllFuture.complete(null))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));

		completedCheckpointStore.shutdown(JobStatus.CANCELED, new CheckpointsCleaner(), () -> {});
		assertThat(removeCalledNum.get(), is(num));
		assertThat(clearEntriesAllFuture.isDone(), is(true));
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(0));
	}

	@Test
	public void testShutdownShouldNotDiscardStateHandleWhenJobIsNotGloballyTerminalState() throws Exception {
		final AtomicInteger removeCalledNum = new AtomicInteger(0);
		final CompletableFuture<Void> removeAllFuture = new CompletableFuture<>();
		final CompletableFuture<Void> releaseAllFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.setRemoveFunction(ignore -> {
				removeCalledNum.incrementAndGet();
				return true;
			})
			.setReleaseAllHandlesRunnable(() -> releaseAllFuture.complete(null))
			.setClearEntriesRunnable(() -> removeAllFuture.complete(null))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(3));

		completedCheckpointStore.shutdown(JobStatus.CANCELLING, new CheckpointsCleaner(), () -> {});
		try {
			removeAllFuture.get(timeout, TimeUnit.MILLISECONDS);
			fail("We should get an expected timeout because the job is not globally terminated.");
		} catch (TimeoutException ex) {
			// expected
		}
		assertThat(removeCalledNum.get(), is(0));
		assertThat(removeAllFuture.isDone(), is(false));
		assertThat(releaseAllFuture.isDone(), is(true));
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(0));
	}

	private  List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> createStateHandles(int num) {
		final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> stateHandles =
			new ArrayList<>();
		for (int i = 1; i <= num; i++) {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint completedCheckpoint =
				CompletedCheckpointStoreTest.createCheckpoint(i, new SharedStateRegistry());
			final RetrievableStateHandle<CompletedCheckpoint> checkpointStateHandle =
				checkpointStorageHelper.store(completedCheckpoint);
			stateHandles.add(new Tuple2<>(checkpointStateHandle, String.valueOf(i)));
		}
		return stateHandles;
	}

	private CompletedCheckpointStore createCompletedCheckpointStore(
			TestingStateHandleStore<CompletedCheckpoint> stateHandleStore) {
		return new DefaultCompletedCheckpointStore<>(
			1,
			stateHandleStore,
			new CheckpointStoreUtil() {
				@Override
				public String checkpointIDToName(long checkpointId) {
					return String.valueOf(checkpointId);
				}

				@Override
				public long nameToCheckpointID(String name) {
					return Long.valueOf(name);
				}
			},
			executorService);
	}
}
