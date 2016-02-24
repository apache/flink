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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.zookeeper.StateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link RecoveryMode#ZOOKEEPER}.
 *
 * <p>Checkpoints are added under a ZNode per job:
 * <pre>
 * +----O /flink/checkpoints/&lt;job-id&gt;  [persistent]
 * .    |
 * .    +----O /flink/checkpoints/&lt;job-id&gt;/1 [persistent]
 * .    .                                  .
 * .    .                                  .
 * .    .                                  .
 * .    +----O /flink/checkpoints/&lt;job-id&gt;/N [persistent]
 * </pre>
 *
 * <p>During recovery, the latest checkpoint is read from ZooKeeper. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 *
 * <p>If there is a network partition and multiple JobManagers run concurrent checkpoints for the
 * same program, it is OK to take any valid successful checkpoint as long as the "history" of
 * checkpoints is consistent. Currently, after recovery we start out with only a single
 * checkpoint to circumvent those situations.
 */
public class ZooKeeperCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCompletedCheckpointStore.class);

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	/** Completed checkpoints in ZooKeeper */
	private final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/** User class loader for discarding {@link CompletedCheckpoint} instances. */
	private final ClassLoader userClassLoader;

	/** Local completed checkpoints. */
	private final ArrayDeque<Tuple2<StateHandle<CompletedCheckpoint>, String>> checkpointStateHandles;

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded. On recovery,
	 *                                       we will only start with a single checkpoint.
	 * @param userClassLoader                The user class loader used to discard checkpoints
	 * @param client                         The Curator ZooKeeper client
	 * @param checkpointsPath                The ZooKeeper path for the checkpoints (needs to
	 *                                       start with a '/')
	 * @param stateStorage                   State storage to be used to persist the completed
	 *                                       checkpoint
	 * @throws Exception
	 */
	public ZooKeeperCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader,
			CuratorFramework client,
			String checkpointsPath,
			StateStorageHelper<CompletedCheckpoint> stateStorage) throws Exception {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		checkNotNull(stateStorage, "State storage");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.userClassLoader = checkNotNull(userClassLoader, "User class loader");

		checkNotNull(client, "Curator client");
		checkNotNull(checkpointsPath, "Checkpoints path");

		// Ensure that the checkpoints path exists
		client.newNamespaceAwareEnsurePath(checkpointsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		this.client = client.usingNamespace(client.getNamespace() + checkpointsPath);

		this.checkpointsInZooKeeper = new ZooKeeperStateHandleStore<>(this.client, stateStorage);

		this.checkpointStateHandles = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

		LOG.info("Initialized in '{}'.", checkpointsPath);
	}

	/**
	 * Gets the latest checkpoint from ZooKeeper and removes all others.
	 *
	 * <p><strong>Important</strong>: Even if there are more than one checkpoint in ZooKeeper,
	 * this will only recover the latest and discard the others. Otherwise, there is no guarantee
	 * that the history of checkpoints is consistent.
	 */
	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints from ZooKeeper.");

		// Clear local handles in order to prevent duplicates on
		// recovery. The local handles should reflect the state
		// of ZooKeeper.
		checkpointStateHandles.clear();

		// Get all there is first
		List<Tuple2<StateHandle<CompletedCheckpoint>, String>> initialCheckpoints;
		while (true) {
			try {
				initialCheckpoints = checkpointsInZooKeeper.getAllSortedByName();
				break;
			}
			catch (ConcurrentModificationException e) {
				LOG.warn("Concurrent modification while reading from ZooKeeper. Retrying.");
			}
		}

		int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in ZooKeeper.", numberOfInitialCheckpoints);

		if (numberOfInitialCheckpoints > 0) {
			// Take the last one. This is the latest checkpoints, because path names are strictly
			// increasing (checkpoint ID).
			Tuple2<StateHandle<CompletedCheckpoint>, String> latest = initialCheckpoints
					.get(numberOfInitialCheckpoints - 1);

			CompletedCheckpoint latestCheckpoint = latest.f0.getState(userClassLoader);

			checkpointStateHandles.add(latest);

			LOG.info("Initialized with {}. Removing all older checkpoints.", latestCheckpoint);

			for (int i = 0; i < numberOfInitialCheckpoints - 1; i++) {
				try {
					removeFromZooKeeperAndDiscardCheckpoint(initialCheckpoints.get(i));
				}
				catch (Exception e) {
					LOG.error("Failed to discard checkpoint", e);
				}
			}
		}
	}

	/**
	 * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		// First add the new one. If it fails, we don't want to loose existing data.
		String path = String.format("/%s", checkpoint.getCheckpointID());

		final StateHandle<CompletedCheckpoint> stateHandle = checkpointsInZooKeeper.add(path, checkpoint);

		checkpointStateHandles.addLast(new Tuple2<>(stateHandle, path));

		// Everything worked, let's remove a previous checkpoint if necessary.
		if (checkpointStateHandles.size() > maxNumberOfCheckpointsToRetain) {
			removeFromZooKeeperAndDiscardCheckpoint(checkpointStateHandles.removeFirst());
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() throws Exception {
		if (checkpointStateHandles.isEmpty()) {
			return null;
		}
		else {
			return checkpointStateHandles.getLast().f0.getState(userClassLoader);
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		List<CompletedCheckpoint> checkpoints = new ArrayList<>(checkpointStateHandles.size());

		for (Tuple2<StateHandle<CompletedCheckpoint>, String> stateHandle : checkpointStateHandles) {
			checkpoints.add(stateHandle.f0.getState(userClassLoader));
		}

		return checkpoints;
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return checkpointStateHandles.size();
	}

	@Override
	public void discardAllCheckpoints() throws Exception {
		for (Tuple2<StateHandle<CompletedCheckpoint>, String> checkpoint : checkpointStateHandles) {
			try {
				removeFromZooKeeperAndDiscardCheckpoint(checkpoint);
			}
			catch (Exception e) {
				LOG.error("Failed to discard checkpoint.", e);
			}
		}

		checkpointStateHandles.clear();

		String path = "/" + client.getNamespace();

		LOG.info("Removing {} from ZooKeeper", path);
		ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
	}

	/**
	 * Removes the state handle from ZooKeeper, discards the checkpoints, and the state handle.
	 */
	private void removeFromZooKeeperAndDiscardCheckpoint(
			final Tuple2<StateHandle<CompletedCheckpoint>, String> stateHandleAndPath) throws Exception {

		final BackgroundCallback callback = new BackgroundCallback() {
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
				try {
					if (event.getType() == CuratorEventType.DELETE) {
						if (event.getResultCode() == 0) {
							// The checkpoint
							CompletedCheckpoint checkpoint = stateHandleAndPath
									.f0.getState(userClassLoader);

							checkpoint.discard(userClassLoader);

							// Discard the state handle
							stateHandleAndPath.f0.discardState();

							// Discard the checkpoint
							LOG.debug("Discarded " + checkpoint);
						}
						else {
							throw new IllegalStateException("Unexpected result code " +
									event.getResultCode() + " in '" + event + "' callback.");
						}
					}
					else {
						throw new IllegalStateException("Unexpected event type " +
								event.getType() + " in '" + event + "' callback.");
					}
				}
				catch (Exception e) {
					LOG.error("Failed to discard checkpoint.", e);
				}
			}
		};

		// Remove state handle from ZooKeeper first. If this fails, we can still recover, but if
		// we remove a state handle and fail to remove it from ZooKeeper, we end up in an
		// inconsistent state.
		checkpointsInZooKeeper.remove(stateHandleAndPath.f1, callback);
	}
}
