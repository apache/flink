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
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

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

	private static final Comparator<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> STRING_COMPARATOR = Comparator.comparing(o -> o.f1);

	/** Completed checkpoints in ZooKeeper. */
	private final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/**
	 * Local copy of the completed checkpoints in ZooKeeper. This is restored from ZooKeeper
	 * when recovering and is maintained in parallel to the state in ZooKeeper during normal
	 * operations.
	 */
	private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

	private final Executor executor;

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded. On recovery,
	 *                                       we will only start with a single checkpoint.
	 * @param checkpointsInZooKeeper         Completed checkpoints in ZooKeeper
	 * @param executor                       to execute blocking calls
	 */
	public ZooKeeperCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper,
			Executor executor) {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

		this.checkpointsInZooKeeper = checkNotNull(checkpointsInZooKeeper);

		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

		this.executor = checkNotNull(executor);
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
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

		// Get all there is first
		List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints;
		while (true) {
			try {
				initialCheckpoints = checkpointsInZooKeeper.getAllAndLock();
				break;
			}
			catch (ConcurrentModificationException e) {
				LOG.warn("Concurrent modification while reading from ZooKeeper. Retrying.");
			}
		}

		Collections.sort(initialCheckpoints, STRING_COMPARATOR);

		int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in ZooKeeper.", numberOfInitialCheckpoints);
		if (haveAllDownloaded(initialCheckpoints)) {
			LOG.info("All {} checkpoints found are already downloaded.", numberOfInitialCheckpoints);
			return;
		}

		// Try and read the state handles from storage. We try until we either successfully read
		// all of them or when we reach a stable state, i.e. when we successfully read the same set
		// of checkpoints in two tries. We do it like this to protect against transient outages
		// of the checkpoint store (for example a DFS): if the DFS comes online midway through
		// reading a set of checkpoints we would run the risk of reading only a partial set
		// of checkpoints while we could in fact read the other checkpoints as well if we retried.
		// Waiting until a stable state protects against this while also being resilient against
		// checkpoints being actually unreadable.
		//
		// These considerations are also important in the scope of incremental checkpoints, where
		// we use ref-counting for shared state handles and might accidentally delete shared state
		// of checkpoints that we don't read due to transient storage outages.
		List<CompletedCheckpoint> lastTryRetrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		List<CompletedCheckpoint> retrievedCheckpoints = new ArrayList<>(numberOfInitialCheckpoints);
		Exception retrieveException = null;
		do {
			LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

			lastTryRetrievedCheckpoints.clear();
			lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);

			retrievedCheckpoints.clear();

			for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpointStateHandle : initialCheckpoints) {

				CompletedCheckpoint completedCheckpoint = null;

				try {
					completedCheckpoint = retrieveCompletedCheckpoint(checkpointStateHandle);
					if (completedCheckpoint != null) {
						retrievedCheckpoints.add(completedCheckpoint);
					}
				} catch (Exception e) {
					LOG.warn("Could not retrieve checkpoint, not adding to list of recovered checkpoints.", e);
					retrieveException = e;
				}
			}

		} while (retrievedCheckpoints.size() != numberOfInitialCheckpoints &&
			!CompletedCheckpoint.checkpointsMatch(lastTryRetrievedCheckpoints, retrievedCheckpoints));

		// Clear local handles in order to prevent duplicates on
		// recovery. The local handles should reflect the state
		// of ZooKeeper.
		completedCheckpoints.clear();
		completedCheckpoints.addAll(retrievedCheckpoints);

		if (completedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
			throw new FlinkException(
				"Could not read any of the " + numberOfInitialCheckpoints + " checkpoints from storage.",
				retrieveException);
		} else if (completedCheckpoints.size() != numberOfInitialCheckpoints) {
			LOG.warn(
				"Could only fetch {} of {} checkpoints from storage.",
				completedCheckpoints.size(),
				numberOfInitialCheckpoints);
		}
	}

	private boolean haveAllDownloaded(List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointPointers) {
		if (completedCheckpoints.size() != checkpointPointers.size()) {
			return false;
		}
		Set<Long> localIds = completedCheckpoints.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet());
		for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> initialCheckpoint : checkpointPointers) {
			if (!localIds.contains(pathToCheckpointId(initialCheckpoint.f1))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(final CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		final String path = checkpointIdToPath(checkpoint.getCheckpointID());

		// Now add the new one. If it fails, we don't want to loose existing data.
		checkpointsInZooKeeper.addAndLock(path, checkpoint);

		completedCheckpoints.addLast(checkpoint);

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			final CompletedCheckpoint completedCheckpoint = completedCheckpoints.removeFirst();
			tryRemoveCompletedCheckpoint(completedCheckpoint, CompletedCheckpoint::discardOnSubsume);
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	private void tryRemoveCompletedCheckpoint(CompletedCheckpoint completedCheckpoint, ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {
		try {
			if (tryRemove(completedCheckpoint.getCheckpointID())) {
				executor.execute(() -> {
					try {
						discardCallback.accept(completedCheckpoint);
					} catch (Exception e) {
						LOG.warn("Could not discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), e);
					}
				});

			}
		} catch (Exception e) {
			LOG.warn("Failed to subsume the old checkpoint", e);
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		List<CompletedCheckpoint> checkpoints = new ArrayList<>(completedCheckpoints);
		return checkpoints;
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (CompletedCheckpoint checkpoint : completedCheckpoints) {
				tryRemoveCompletedCheckpoint(
					checkpoint,
					completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus));
			}

			completedCheckpoints.clear();
			checkpointsInZooKeeper.deleteChildren();
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();

			// Release the state handle locks in ZooKeeper such that they can be deleted
			checkpointsInZooKeeper.releaseAll();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Tries to remove the checkpoint identified by the given checkpoint id.
	 *
	 * @param checkpointId identifying the checkpoint to remove
	 * @return true if the checkpoint could be removed
	 */
	private boolean tryRemove(long checkpointId) throws Exception {
		return checkpointsInZooKeeper.releaseAndTryRemove(checkpointIdToPath(checkpointId));
	}

	/**
	 * Convert a checkpoint id into a ZooKeeper path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	public static String checkpointIdToPath(long checkpointId) {
		return String.format("/%019d", checkpointId);
	}

	/**
	 * Converts a path to the checkpoint id.
	 *
	 * @param path in ZooKeeper
	 * @return Checkpoint id parsed from the path
	 */
	public static long pathToCheckpointId(String path) {
		try {
			String numberString;

			// check if we have a leading slash
			if ('/' == path.charAt(0)) {
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

	private static CompletedCheckpoint retrieveCompletedCheckpoint(Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandlePath) throws FlinkException {
		long checkpointId = pathToCheckpointId(stateHandlePath.f1);

		LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

		try {
			return stateHandlePath.f0.retrieveState();
		} catch (ClassNotFoundException cnfe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that you are trying to recover from state written by an " +
				"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
		} catch (IOException ioe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that the retrieved state handle is broken. Try cleaning the " +
				"state handle store.", ioe);
		}
	}
}
