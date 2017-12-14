/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * This class will service as a task-manager-level local storage for local checkpointed state. The purpose is to provide
 * access to a state that is stored locally for a faster recovery compared to the state that is stored remotely in a
 * stable store DFS. For now, this storage is only complementary to the stable storage and local state is typically
 * lost in case of machine failures. In such cases (and others), client code of this class must fall back to using the
 * slower but highly available store.
 */
public class TaskLocalStateStore {

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(TaskLocalStateStore.class);

	/** Maximum number of retained snapshots. */
	private static final int MAX_RETAINED_SNAPSHOTS = 5;

	/** Dummy value to use instead of null to satisfy {@link ConcurrentHashMap}. */
	private final TaskStateSnapshot NULL_DUMMY = new TaskStateSnapshot();

	/** JobID from the owning subtask. */
	private final JobID jobID;

	/** JobVertexID of the owning subtask. */
	private final JobVertexID jobVertexID;

	/** Subtask index of the owning subtask. */
	private final int subtaskIndex;

	/** The root directories for all local state of this {@link TaskLocalStateStore}. */
	private final File[] localStateRootDirectories;

	/** Executor that runs the discarding of released state objects. */
	private final Executor discardExecutor;

	/** Lock for synchronisation on the storage map and the discarded status. */
	private final Object lock;

	/** Status flag if this store was already discarded. */
	@GuardedBy("lock")
	private boolean discarded;

	/** Maps checkpoint ids to local TaskStateSnapshots. */
	@GuardedBy("lock")
	private final SortedMap<Long, TaskStateSnapshot> storedTaskStateByCheckpointID;

	public TaskLocalStateStore(
		@Nonnull JobID jobID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex,
		@Nonnull File[] localStateRootDirectories,
		@Nonnull Executor discardExecutor) {

		this.jobID = jobID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
		this.localStateRootDirectories = localStateRootDirectories;
		this.discardExecutor = discardExecutor;
		this.lock = new Object();
		this.storedTaskStateByCheckpointID = new TreeMap<>();
		this.discarded = false;
	}

	@Nonnull
	protected String createSubtaskPath() {
		return jobID + File.separator + jobVertexID + File.separator + subtaskIndex;
	}

	/**
	 * Stores the local state for the given checkpoint id.
	 *
	 * @param checkpointId id for the checkpoint that created the local state that will be stored.
	 * @param localState the local state to store.
	 */
	public void storeLocalState(
		@Nonnegative long checkpointId,
		@Nullable TaskStateSnapshot localState) {

		if (localState == null) {
			localState = NULL_DUMMY;
		}

		LOG.info("Storing local state for checkpoint {}.", checkpointId);
		LOG.debug("Local state for checkpoint {} is {}.", checkpointId, localState);

		synchronized (lock) {
			if (discarded) {
				// we ignore late stores and simply discard the state.
				discardStateObject(localState, checkpointId);
			} else {
				TaskStateSnapshot previous =
					storedTaskStateByCheckpointID.put(checkpointId, localState);

				if (previous != null) {
					// this should never happen.
					discardStateObject(previous, checkpointId);
					throw new IllegalStateException("Found previously registered local state for checkpoint with id " +
						checkpointId + "! This indicated a problem.");
				}

				// prune history.
				while (storedTaskStateByCheckpointID.size() > MAX_RETAINED_SNAPSHOTS) {
					Long removeCheckpointID = storedTaskStateByCheckpointID.firstKey();
					TaskStateSnapshot snapshot =
						storedTaskStateByCheckpointID.remove(storedTaskStateByCheckpointID.firstKey());
					discardStateObject(snapshot, removeCheckpointID);
				}
			}
		}
	}

	/**
	 * Returns the local state that is stored under the given checkpoint id or null if nothing was stored under the id.
	 *
	 * @param checkpointID the checkpoint id by which we search for local state.
	 * @return the local state found for the given checkpoint id. Can be null
	 */
	@Nullable
	public TaskStateSnapshot retrieveLocalState(long checkpointID) {
		synchronized (lock) {
			TaskStateSnapshot snapshot = storedTaskStateByCheckpointID.get(checkpointID);
			return snapshot != NULL_DUMMY ? snapshot : null;
		}
	}

	/**
	 * Disposes the state of all local snapshots managed by this object.
	 */
	public void dispose() {
		synchronized (lock) {
			for (Map.Entry<Long, TaskStateSnapshot> entry : storedTaskStateByCheckpointID.entrySet()) {
				discardStateObject(entry.getValue(), entry.getKey());
			}
			discarded = true;
		}
	}

	/**
	 * Returns a new {@link LocalRecoveryDirectoryProvider} for the {@link #localStateRootDirectories}.
	 */
	@Nonnull
	public LocalRecoveryDirectoryProvider createLocalRecoveryRootDirectoryProvider() {
		return new LocalRecoveryDirectoryProvider(localStateRootDirectories, createSubtaskPath());
	}

	/**
	 * Notifies that the checkpoint with the given id was confirmed as complete. This prunes the checkpoint history
	 * and removes all local states with a checkpoint id that is smaller than the newly confirmed checkpoint id.
	 */
	public void confirmCheckpoint(long confirmedCheckpointId) {

		LOG.debug("Received confirmation for checkpoint {}. Starting to prune history.", confirmedCheckpointId);

		synchronized (lock) {

			Iterator<Map.Entry<Long, TaskStateSnapshot>> entryIterator =
				storedTaskStateByCheckpointID.entrySet().iterator();

			// remove entries for outdated checkpoints and discard their state.
			while (entryIterator.hasNext()) {
				Map.Entry<Long, TaskStateSnapshot> snapshotEntry = entryIterator.next();
				long entryCheckpointId = snapshotEntry.getKey();
				if (entryCheckpointId < confirmedCheckpointId) {
					entryIterator.remove();
					discardStateObject(snapshotEntry.getValue(), entryCheckpointId);
				} else {
					// we can stop because the map is sorted.
					break;
				}
			}
		}
	}

	/**
	 * Helper method that discards state objects with an executor and reports exceptions to the log.
	 */
	private void discardStateObject(StateObject o, long checkpointID) {
		discardExecutor.execute(() -> {
			try {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Discarding local state object of checkpoint {} for {}/{}/{}.",
						checkpointID, jobID, jobVertexID, subtaskIndex);
				} else {
					LOG.debug("Discarding local state object {} of checkpoint {} for {}/{}/{}.",
						o, checkpointID, jobID, jobVertexID, subtaskIndex);
				}
				o.discardState();
			} catch (Exception discardEx) {
				LOG.warn("Exception while discarding local state of checkpoint " + checkpointID + ".", discardEx);
			}
		});
	}

	@Override
	public String toString() {
		return "TaskLocalStateStore{" +
			"jobID=" + jobID +
			", jobVertexID=" + jobVertexID +
			", subtaskIndex=" + subtaskIndex +
			", localStateRootDirectories=" + Arrays.toString(localStateRootDirectories) +
			'}';
	}
}
