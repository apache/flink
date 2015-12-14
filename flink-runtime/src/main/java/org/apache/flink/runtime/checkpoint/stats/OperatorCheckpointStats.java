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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Statistics for a specific checkpoint per operator.
 */
public class OperatorCheckpointStats extends CheckpointStats {

	/** Duration in milliseconds and state sizes in bytes per sub task. */
	private final long[][] subTaskStats;

	/**
	 * Creates a checkpoint statistic for an operator.
	 *
	 * @param checkpointId     Checkpoint ID this statistic belongs to
	 * @param triggerTimestamp Timestamp when the corresponding checkpoint was triggered
	 * @param duration         Duration (in milliseconds) to complete this statistic
	 * @param stateSize        State size (in bytes)
	 * @param subTaskStats     Stats per subtask ([i][0] and [i][1] encode the duration and state
	 *                         size for sub task i respectively).
	 */
	public OperatorCheckpointStats(
			long checkpointId,
			long triggerTimestamp,
			long duration,
			long stateSize,
			long[][] subTaskStats) {

		super(checkpointId, triggerTimestamp, duration, stateSize);

		this.subTaskStats = checkNotNull(subTaskStats);
	}

	/**
	 * Returns the number of sub tasks.
	 *
	 * @return Number of sub tasks.
	 */
	public int getNumberOfSubTasks() {
		return subTaskStats.length;
	}

	/**
	 * Returns the duration of a specific sub task.
	 *
	 * @return Duration of the sub task.
	 */
	public long getSubTaskDuration(int index) {
		return subTaskStats[index][0];
	}

	/**
	 * Returns the state size of a specific sub task.
	 *
	 * @return The state size in bytes.
	 */
	public long getSubTaskStateSize(int index) {
		return subTaskStats[index][1];
	}

	@Override
	public String toString() {
		return "OperatorCheckpointStats{" +
				"checkpointId=" + getCheckpointId() +
				", subTaskStats=" + Arrays.deepToString(subTaskStats) +
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

		OperatorCheckpointStats that = (OperatorCheckpointStats) o;

		return getCheckpointId() == that.getCheckpointId() &&
				getTriggerTimestamp() == that.getTriggerTimestamp() &&
				Arrays.deepEquals(subTaskStats, that.subTaskStats);
	}

	@Override
	public int hashCode() {
		int result = (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
		result = 31 * result + (int) (getTriggerTimestamp() ^ (getTriggerTimestamp() >>> 32));
		result = 31 * result + (int) (subTaskStats.length ^ (subTaskStats.length >>> 32));
		return result;
	}
}
