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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
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

	/** Local completed checkpoints. */
	private final ArrayDeque<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointStateHandles;

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded. On recovery,
	 *                                       we will only start with a single checkpoint.
	 * @param client                         The Curator ZooKeeper client
	 * @param checkpointsPath                The ZooKeeper path for the checkpoints (needs to
	 *                                       start with a '/')
	 * @param stateStorage                   State storage to be used to persist the completed
	 *                                       checkpoint
	 * @param executor to give to the ZooKeeperStateHandleStore to run ZooKeeper callbacks
	 * @throws Exception
	 */
	public ZooKeeperCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			CuratorFramework client,
			String checkpointsPath,
			RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage,
			Executor executor) throws Exception {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		checkNotNull(stateStorage, "State storage");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

		checkNotNull(client, "Curator client");
		checkNotNull(checkpointsPath, "Checkpoints path");

		// Ensure that the checkpoints path exists
		client.newNamespaceAwareEnsurePath(checkpointsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		this.client = client.usingNamespace(client.getNamespace() + checkpointsPath);

		this.checkpointsInZooKeeper = new ZooKeeperStateHandleStore<>(this.client, stateStorage, executor);

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
		List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints;
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
			Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> latest = initialCheckpoints
					.get(numberOfInitialCheckpoints - 1);

			CompletedCheckpoint latestCheckpoint;
			long checkpointId = pathToCheckpointId(latest.f1);

			LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

			try {
				latestCheckpoint = latest.f0.retrieveState();
			} catch (Exception e) {
				throw new Exception("Could not retrieve the completed checkpoint " + checkpointId +
				" from the state storage.", e);
			}

			checkpointStateHandles.add(latest);

			LOG.info("Initialized with {}. Removing all older checkpoints.", latestCheckpoint);

			for (int i = 0; i < numberOfInitialCheckpoints - 1; i++) {
				try {
					removeSubsumed(initialCheckpoints.get(i));
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
		String path = checkpointIdToPath(checkpoint.getCheckpointID());

		final RetrievableStateHandle<CompletedCheckpoint> stateHandle =
				checkpointsInZooKeeper.add(path, checkpoint);

		checkpointStateHandles.addLast(new Tuple2<>(stateHandle, path));

		// Everything worked, let's remove a previous checkpoint if necessary.
		if (checkpointStateHandles.size() > maxNumberOfCheckpointsToRetain) {
			removeSubsumed(checkpointStateHandles.removeFirst());
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() throws Exception {
		if (checkpointStateHandles.isEmpty()) {
			return null;
		}
		else {
			return checkpointStateHandles.getLast().f0.retrieveState();
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		List<CompletedCheckpoint> checkpoints = new ArrayList<>(checkpointStateHandles.size());

		for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandle : checkpointStateHandles) {
			checkpoints.add(stateHandle.f0.retrieveState());
		}

		return checkpoints;
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return checkpointStateHandles.size();
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpoint : checkpointStateHandles) {
				try {
					removeShutdown(checkpoint, jobStatus);
				} catch (Exception e) {
					LOG.error("Failed to discard checkpoint.", e);
				}
			}

			checkpointStateHandles.clear();

			String path = "/" + client.getNamespace();

			LOG.info("Removing {} from ZooKeeper", path);
			ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			checkpointStateHandles.clear();
		}
	}

	// ------------------------------------------------------------------------

	private void removeSubsumed(final Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandleAndPath) throws Exception {
		Callable<Void> action = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				stateHandleAndPath.f0.retrieveState().subsume();
				return null;
			}
		};

		remove(stateHandleAndPath, action);
	}

	private void removeShutdown(
			final Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandleAndPath,
			final JobStatus jobStatus) throws Exception {

		Callable<Void> action = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				CompletedCheckpoint checkpoint = stateHandleAndPath.f0.retrieveState();
				checkpoint.discard(jobStatus);
				return null;
			}
		};

		remove(stateHandleAndPath, action);
	}

	/**
	 * Removes the state handle from ZooKeeper, discards the checkpoints, and the state handle.
	 */
	private void remove(
			final Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandleAndPath,
			final Callable<Void> action) throws Exception {

		BackgroundCallback callback = new BackgroundCallback() {
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
				final long checkpointId = pathToCheckpointId(stateHandleAndPath.f1);

				try {
					if (event.getType() == CuratorEventType.DELETE) {
						if (event.getResultCode() == 0) {
							Exception exception = null;

							try {
								action.call();
							} catch (Exception e) {
								exception = new Exception("Could not execute callable action " +
									"for checkpoint " + checkpointId + '.', e);
							}

							try {
								// Discard the state handle
								stateHandleAndPath.f0.discardState();
							} catch (Exception e) {
								Exception newException = new Exception("Could not discard meta " +
									"data for completed checkpoint " + checkpointId + '.', e);

								if (exception == null) {
									exception = newException;
								} else {
									exception.addSuppressed(newException);
								}
							}

							if (exception != null) {
								throw exception;
							}
						} else {
							throw new IllegalStateException("Unexpected result code " +
									event.getResultCode() + " in '" + event + "' callback.");
						}
					} else {
						throw new IllegalStateException("Unexpected event type " +
								event.getType() + " in '" + event + "' callback.");
					}
				} catch (Exception e) {
					LOG.warn("Failed to discard checkpoint {}.", checkpointId, e);
				}
			}
		};

		// Remove state handle from ZooKeeper first. If this fails, we can still recover, but if
		// we remove a state handle and fail to remove it from ZooKeeper, we end up in an
		// inconsistent state.
		checkpointsInZooKeeper.remove(stateHandleAndPath.f1, callback);
	}

	/**
	 * Convert a checkpoint id into a ZooKeeper path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	protected static String checkpointIdToPath(long checkpointId) {
		return String.format("/%s", checkpointId);
	}

	/**
	 * Converts a path to the checkpoint id.
	 *
	 * @param path in ZooKeeper
	 * @return Checkpoint id parsed from the path
	 */
	protected static long pathToCheckpointId(String path) {
		try {
			String numberString;

			// check if we have a leading slash
			if ('/' == path.charAt(0) ) {
				numberString = path.substring(1);
			} else {
				numberString = path;
			}
			return Long.parseLong(numberString);
		} catch (NumberFormatException e) {
			LOG.warn("Could not parse checkpoint id from {}. This indicates that the " +
				"checkpoint id to path conversion has changed.", path);

			return -1L;
		}
	}
}
