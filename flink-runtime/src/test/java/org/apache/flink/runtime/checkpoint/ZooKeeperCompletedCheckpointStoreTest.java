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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.checkpoint.CompletedCheckpointStoreTest.createCheckpoint;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultCompletedCheckpointStore} with {@link ZooKeeperStateHandleStore}.
 */
public class ZooKeeperCompletedCheckpointStoreTest extends TestLogger {

	@ClassRule
	public static ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	private static final ZooKeeperCheckpointStoreUtil zooKeeperCheckpointStoreUtil =
		ZooKeeperCheckpointStoreUtil.INSTANCE;

	@Test
	public void testPathConversion() {
		final long checkpointId = 42L;

		final String path = zooKeeperCheckpointStoreUtil.checkpointIDToName(checkpointId);

		assertEquals(checkpointId, zooKeeperCheckpointStoreUtil.nameToCheckpointID(path));
	}

	@Test(expected = ExpectedTestException.class)
	public void testRecoverFailsIfDownloadFails() throws Exception {
		testDownloadInternal((store, checkpointsInZk, sharedStateRegistry) -> {
			try {
				checkpointsInZk.add(createHandle(1, id -> {
					throw new ExpectedTestException();
				}));
				store.recover();
			} catch (Exception exception) {
				findThrowable(exception, ExpectedTestException.class).ifPresent(ExceptionUtils::rethrow);
				rethrow(exception);
			}
		});
	}

	@Test
	public void testNoDownloadIfCheckpointsNotChanged() throws Exception {
		testDownloadInternal((store, checkpointsInZk, sharedStateRegistry) -> {
			try {
				checkpointsInZk.add(createHandle(1, id -> {
					throw new AssertionError("retrieveState was attempted for checkpoint " + id);
				}));
				store.addCheckpoint(createCheckpoint(1, sharedStateRegistry), new CheckpointsCleaner(), () -> { /*no op*/});
				store.recover(); // will fail in case of attempt to retrieve state
			} catch (Exception exception) {
				throw new RuntimeException(exception);
			}
		});
	}

	@Test
	public void testDownloadIfCheckpointsChanged() throws Exception {
		testDownloadInternal((store, checkpointsInZk, sharedStateRegistry) -> {
			try {
				int lastInZk = 10;
				IntStream.range(0, lastInZk + 1).forEach(i -> checkpointsInZk.add(createHandle(i, id -> createCheckpoint(id, sharedStateRegistry))));
				store.addCheckpoint(createCheckpoint(1, sharedStateRegistry), new CheckpointsCleaner(), () -> { /*no op*/});
				store.addCheckpoint(createCheckpoint(5, sharedStateRegistry), new CheckpointsCleaner(), () -> { /*no op*/});
				store.recover();
				assertEquals(lastInZk, store.getLatestCheckpoint(false).getCheckpointID());
			} catch (Exception exception) {
				throw new RuntimeException(exception);
			}
		});
	}

	private void testDownloadInternal(TriConsumer<CompletedCheckpointStore, List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>>, SharedStateRegistry> test) throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
		List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointsInZk = new ArrayList<>();
		ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper = new ZooKeeperStateHandleStore<CompletedCheckpoint>(
			ZooKeeperUtils.startCuratorFramework(configuration),
			new TestingRetrievableStateStorageHelper<>()) {
				@Override
				public List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> getAllAndLock() {
					return checkpointsInZk;
				}
			};

		CompletedCheckpointStore store = new DefaultCompletedCheckpointStore<>(
			10,
			checkpointsInZooKeeper,
			zooKeeperCheckpointStoreUtil,
			Executors.directExecutor());
		try {
			test.accept(store, checkpointsInZk, sharedStateRegistry);
		} finally {
			store.shutdown(JobStatus.FINISHED, new CheckpointsCleaner(), () -> { /* no op */ });
			sharedStateRegistry.close();
		}
	}

	private Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> createHandle(long id, Function<Long, CompletedCheckpoint> checkpointSupplier) {
		return Tuple2.of(
			new CheckpointStateHandle(checkpointSupplier, id),
			zooKeeperCheckpointStoreUtil.checkpointIDToName(id)
		);
	}

	/**
	 * Tests that subsumed checkpoints are discarded.
	 */
	@Test
	public void testDiscardingSubsumedCheckpoints() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final CompletedCheckpointStore checkpointStore = createZooKeeperCheckpointStore(client);

		try {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 = CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

			checkpointStore.addCheckpoint(checkpoint1, new CheckpointsCleaner(), () -> {
			});
			assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint2 = CompletedCheckpointStoreTest.createCheckpoint(1, sharedStateRegistry);
			checkpointStore.addCheckpoint(checkpoint2, new CheckpointsCleaner(), () -> {
			});
			final List<CompletedCheckpoint> allCheckpoints = checkpointStore.getAllCheckpoints();
			assertThat(allCheckpoints, Matchers.contains(checkpoint2));
			assertThat(allCheckpoints, Matchers.not(Matchers.contains(checkpoint1)));

			// verify that the subsumed checkpoint is discarded
			CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
		} finally {
			client.close();
		}
	}

	/**
	 * Tests that checkpoints are discarded when the completed checkpoint store is shut
	 * down with a globally terminal state.
	 */
	@Test
	public void testDiscardingCheckpointsAtShutDown() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final CompletedCheckpointStore checkpointStore = createZooKeeperCheckpointStore(client);

		try {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 = CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

			checkpointStore.addCheckpoint(checkpoint1, new CheckpointsCleaner(), () -> {
			});
			assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

			checkpointStore.shutdown(JobStatus.FINISHED, new CheckpointsCleaner(), () -> {
			});

			// verify that the checkpoint is discarded
			CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
		} finally {
			client.close();
		}
	}

	@Nonnull
	private CompletedCheckpointStore createZooKeeperCheckpointStore(CuratorFramework client) throws Exception {
		final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper = ZooKeeperUtils.createZooKeeperStateHandleStore(
			client,
			"/checkpoints",
			new TestingRetrievableStateStorageHelper<>());

		return new DefaultCompletedCheckpointStore<>(
			1,
			checkpointsInZooKeeper,
			zooKeeperCheckpointStoreUtil,
			Executors.directExecutor());
	}

	private static class CheckpointStateHandle implements RetrievableStateHandle<CompletedCheckpoint> {
		private static final long serialVersionUID = 1L;
		private final Function<Long, CompletedCheckpoint> checkpointSupplier;
		private final long id;

		CheckpointStateHandle(Function<Long, CompletedCheckpoint> checkpointSupplier, long id) {
			this.checkpointSupplier = checkpointSupplier;
			this.id = id;
		}

		@Override
		public CompletedCheckpoint retrieveState() {
			return checkpointSupplier.apply(id);
		}

		@Override
		public void discardState() {
		}

		@Override
		public long getStateSize() {
			return 0;
		}
	}
}
