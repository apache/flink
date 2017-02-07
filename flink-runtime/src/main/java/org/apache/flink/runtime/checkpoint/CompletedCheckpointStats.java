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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Statistics for a successfully completed checkpoint.
 *
 * <p>The reported statistics are immutable except for the discarded flag, which
 * is updated via the {@link DiscardCallback} and the {@link CompletedCheckpoint}
 * after an instance of this class has been created.
 */
public class CompletedCheckpointStats extends AbstractCheckpointStats {

	private static final long serialVersionUID = 138833868551861343L;

	/** Total checkpoint state size over all subtasks. */
	private final long stateSize;

	/** Buffered bytes during alignment over all subtasks. */
	private final long alignmentBuffered;

	/** The latest acknowledged subtask stats. */
	private final SubtaskStateStats latestAcknowledgedSubtask;

	/** Optional external path if persisted externally. */
	@Nullable
	private final String externalPath;

	/** Flag indicating whether the checkpoint was discarded. */
	private volatile boolean discarded;

	/**
	 * Creates a tracker for a {@link CompletedCheckpoint}.
	 *
	 * @param checkpointId ID of the checkpoint.
	 * @param triggerTimestamp Timestamp when the checkpoint was triggered.
	 * @param props Checkpoint properties of the checkpoint.
	 * @param totalSubtaskCount Total number of subtasks for the checkpoint.
	 * @param taskStats Task stats for each involved operator.
	 * @param numAcknowledgedSubtasks Number of acknowledged subtasks.
	 * @param stateSize Total checkpoint state size over all subtasks.
	 * @param alignmentBuffered Buffered bytes during alignment over all subtasks.
	 * @param latestAcknowledgedSubtask The latest acknowledged subtask stats.
	 * @param externalPath Optional external path if persisted externally.
	 */
	CompletedCheckpointStats(
			long checkpointId,
			long triggerTimestamp,
			CheckpointProperties props,
			int totalSubtaskCount,
			Map<JobVertexID, TaskStateStats> taskStats,
			int numAcknowledgedSubtasks,
			long stateSize,
			long alignmentBuffered,
			SubtaskStateStats latestAcknowledgedSubtask,
			@Nullable String externalPath) {

		super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
		checkArgument(numAcknowledgedSubtasks == totalSubtaskCount, "Did not acknowledge all subtasks.");
		checkArgument(stateSize >= 0, "Negative state size");
		this.stateSize = stateSize;
		this.alignmentBuffered = alignmentBuffered;
		this.latestAcknowledgedSubtask = checkNotNull(latestAcknowledgedSubtask);
		this.externalPath = externalPath;
	}

	@Override
	public CheckpointStatsStatus getStatus() {
		return CheckpointStatsStatus.COMPLETED;
	}

	@Override
	public int getNumberOfAcknowledgedSubtasks() {
		return numberOfSubtasks;
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	@Override
	public long getAlignmentBuffered() {
		return alignmentBuffered;
	}

	@Override
	@Nullable
	public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
		return latestAcknowledgedSubtask;
	}

	// ------------------------------------------------------------------------
	// Completed checkpoint specific methods
	// ------------------------------------------------------------------------

	/**
	 * Returns the external path if this checkpoint was persisted externally.
	 *
	 * @return External path of this checkpoint or <code>null</code>.
	 */
	@Nullable
	public String getExternalPath() {
		return externalPath;
	}

	/**
	 * Returns whether the checkpoint has been discarded.
	 *
	 * @return <code>true</code> if the checkpoint has been discarded, <code>false</code> otherwise.
	 */
	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Returns the callback for the {@link CompletedCheckpoint}.
	 *
	 * @return Callback for the {@link CompletedCheckpoint}.
	 */
	DiscardCallback getDiscardCallback() {
		return new DiscardCallback();
	}

	/**
	 * Callback for the {@link CompletedCheckpoint} instance to notify about
	 * disposal of the checkpoint (most commonly when the checkpoint has been
	 * subsumed by a newer one).
	 */
	class DiscardCallback {

		/**
		 * Updates the discarded flag of the checkpoint stats.
		 *
		 * <p>After this notification, {@link #isDiscarded()} will return
		 * <code>true</code>.
		 */
		void notifyDiscardedCheckpoint() {
			discarded = true;
		}

	}

	@Override
	public String toString() {
		return "CompletedCheckpoint(id=" + getCheckpointId() + ")";
	}
}
