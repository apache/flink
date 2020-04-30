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

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Statistics for a single subtask that is part of a checkpoint.
 *
 * <p>Collects data that is spread over different close places:
 * {@link CheckpointMetaData},
 * {@link SubtaskState}, and
 * {@link PendingCheckpoint}.
 *
 * <p>This is the smallest immutable unit of the stats.
 */
public class SubtaskStateStats implements Serializable {

	private static final long serialVersionUID = 8928594531621862214L;

	private final int subtaskIndex;

	/**
	 * Timestamp when the ack from this sub task was received at the
	 * coordinator.
	 */
	private final long ackTimestamp;

	/** Size of the checkpointed state at this subtask. */
	private final long stateSize;

	/** Checkpoint duration at the operator (sync part) in milliseconds. */
	private final long syncCheckpointDuration;

	/** Checkpoint duration at the operator (async part) in milliseconds. */
	private final long asyncCheckpointDuration;

	/** Alignment duration in milliseconds. */
	private final long alignmentDuration;

	/** Checkpoint start delay in milliseconds. */
	private final long checkpointStartDelay;

	SubtaskStateStats(
			int subtaskIndex,
			long ackTimestamp,
			long stateSize,
			long syncCheckpointDuration,
			long asyncCheckpointDuration,
			long alignmentDuration,
			long checkpointStartDelay) {

		checkArgument(subtaskIndex >= 0, "Negative subtask index");
		this.subtaskIndex = subtaskIndex;
		checkArgument(stateSize >= 0, "Negative state size");
		this.stateSize = stateSize;
		this.ackTimestamp = ackTimestamp;
		this.syncCheckpointDuration = syncCheckpointDuration;
		this.asyncCheckpointDuration = asyncCheckpointDuration;
		this.alignmentDuration = alignmentDuration;
		this.checkpointStartDelay = checkpointStartDelay;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	/**
	 * Returns the size of the checkpointed state at this subtask.
	 *
	 * @return Checkpoint state size of the sub task.
	 */
	public long getStateSize() {
		return stateSize;
	}

	/**
	 * Returns the timestamp when the acknowledgement of this subtask was
	 * received at the coordinator.
	 *
	 * @return ACK timestamp at the coordinator.
	 */
	public long getAckTimestamp() {
		return ackTimestamp;
	}

	/**
	 * Computes the duration since the given trigger timestamp.
	 *
	 * <p>If the trigger timestamp is greater than the ACK timestamp, this
	 * returns <code>0</code>.
	 *
	 * @param triggerTimestamp Trigger timestamp of the checkpoint.
	 * @return Duration since the given trigger timestamp.
	 */
	public long getEndToEndDuration(long triggerTimestamp) {
		return Math.max(0, ackTimestamp - triggerTimestamp);
	}

	/**
	 * @return Duration of the synchronous part of the checkpoint or <code>-1</code> if the runtime
	 * did not report this.
	 */
	public long getSyncCheckpointDuration() {
		return syncCheckpointDuration;
	}

	/**
	 * @return Duration of the asynchronous part of the checkpoint or <code>-1</code> if the runtime
	 * did not report this.
	 */
	public long getAsyncCheckpointDuration() {
		return asyncCheckpointDuration;
	}

	/**
	 * @return Duration of the stream alignment (for exactly-once only) or <code>-1</code> if the
	 * runtime did not report this.
	 */
	public long getAlignmentDuration() {
		return alignmentDuration;
	}

	public long getCheckpointStartDelay() {
		return checkpointStartDelay;
	}
}
