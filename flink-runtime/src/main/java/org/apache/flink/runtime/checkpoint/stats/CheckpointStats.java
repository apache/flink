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

package org.apache.flink.runtime.checkpoint.stats;

/**
 * Statistics for a specific checkpoint.
 */
public class CheckpointStats {

	/** ID of the checkpoint. */
	private final long checkpointId;

	/** Timestamp when the checkpoint was triggered. */
	private final long triggerTimestamp;

	/** Duration of the checkpoint in milliseconds. */
	private final long duration;

	/** State size in bytes. */
	private final long stateSize;

	/**
	 * Creates a checkpoint statistic.
	 *
	 * @param checkpointId     Checkpoint ID
	 * @param triggerTimestamp Timestamp when the checkpoint was triggered
	 * @param duration         Duration (in milliseconds)
	 * @param stateSize        State size (in bytes)
	 */
	public CheckpointStats(
			long checkpointId,
			long triggerTimestamp,
			long duration,
			long stateSize) {

		this.checkpointId = checkpointId;
		this.triggerTimestamp = triggerTimestamp;
		this.duration = duration;
		this.stateSize = stateSize;
	}

	/**
	 * Returns the ID of the checkpoint.
	 *
	 * @return ID of the checkpoint.
	 */
	public long getCheckpointId() {
		return checkpointId;
	}

	/**
	 * Returns the timestamp when the checkpoint was triggered.
	 *
	 * @return Timestamp when the checkpoint was triggered.
	 */
	public long getTriggerTimestamp() {
		return triggerTimestamp;
	}

	/**
	 * Returns the duration in milliseconds.
	 *
	 * @return Duration in milliseconds.
	 */
	public long getDuration() {
		return duration;
	}

	/**
	 * Returns the state size in bytes.
	 *
	 * @return The state size in bytes.
	 */
	public long getStateSize() {
		return stateSize;
	}

	@Override
	public String toString() {
		return "CheckpointStats{" +
				"checkpointId=" + checkpointId +
				", triggerTimestamp=" + triggerTimestamp +
				", duration=" + duration +
				", stateSize=" + stateSize +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointStats that = (CheckpointStats) o;

		return checkpointId == that.checkpointId &&
				triggerTimestamp == that.triggerTimestamp &&
				duration == that.duration &&
				stateSize == that.stateSize;
	}

	@Override
	public int hashCode() {
		int result = (int) (checkpointId ^ (checkpointId >>> 32));
		result = 31 * result + (int) (triggerTimestamp ^ (triggerTimestamp >>> 32));
		result = 31 * result + (int) (duration ^ (duration >>> 32));
		result = 31 * result + (int) (stateSize ^ (stateSize >>> 32));
		return result;
	}
}
