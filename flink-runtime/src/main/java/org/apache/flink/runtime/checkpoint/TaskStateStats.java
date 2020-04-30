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

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Statistics for a single task/operator that gathers all statistics of its
 * subtasks and provides summary statistics about all subtasks.
 */
public class TaskStateStats implements Serializable {

	private static final long serialVersionUID = 531803101206574444L;

	/** ID of the task the stats belong to. */
	private final JobVertexID jobVertexId;

	private final SubtaskStateStats[] subtaskStats;

	/** A summary of the subtask stats. */
	private final TaskStateStatsSummary summaryStats = new TaskStateStatsSummary();

	private int numAcknowledgedSubtasks;

	@Nullable
	private SubtaskStateStats latestAckedSubtaskStats;

	TaskStateStats(JobVertexID jobVertexId, int numSubtasks) {
		this.jobVertexId = checkNotNull(jobVertexId, "JobVertexID");
		checkArgument(numSubtasks > 0, "Number of subtasks <= 0");
		this.subtaskStats = new SubtaskStateStats[numSubtasks];
	}

	boolean reportSubtaskStats(SubtaskStateStats subtask) {
		checkNotNull(subtask, "Subtask stats");
		int subtaskIndex = subtask.getSubtaskIndex();

		if (subtaskIndex < 0 || subtaskIndex >= subtaskStats.length) {
			return false;
		}

		if (subtaskStats[subtaskIndex] == null) {
			subtaskStats[subtaskIndex] = subtask;

			latestAckedSubtaskStats = subtask;
			numAcknowledgedSubtasks++;

			summaryStats.updateSummary(subtask);

			return true;
		} else {
			return false;
		}
	}

	/**
	 * @return ID of the operator the statistics belong to.
	 */
	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	public int getNumberOfSubtasks() {
		return subtaskStats.length;
	}

	public int getNumberOfAcknowledgedSubtasks() {
		return numAcknowledgedSubtasks;
	}

	/**
	 * @return The latest acknowledged subtask stats or <code>null</code>
	 * if none was acknowledged yet.
	 */
	@Nullable
	public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
		return latestAckedSubtaskStats;
	}

	/**
	 * @return Ack timestamp of the latest acknowledged subtask or <code>-1</code> if none was
	 * acknowledged yet..
	 */
	public long getLatestAckTimestamp() {
		SubtaskStateStats subtask = latestAckedSubtaskStats;
		if (subtask != null) {
			return subtask.getAckTimestamp();
		} else {
			return -1;
		}
	}

	/**
	 * @return Total checkpoint state size over all subtasks.
	 */
	public long getStateSize() {
		return summaryStats.getStateSizeStats().getSum();
	}

	/**
	 * Returns the duration of this checkpoint at the task/operator calculated
	 * as the time since triggering until the latest acknowledged subtask
	 * or <code>-1</code> if no subtask was acknowledged yet.
	 *
	 * @return Duration of this checkpoint at the task/operator or <code>-1</code> if no subtask was acknowledged yet.
	 */
	public long getEndToEndDuration(long triggerTimestamp) {
		SubtaskStateStats subtask = getLatestAcknowledgedSubtaskStats();
		if (subtask != null) {
			return Math.max(0, subtask.getAckTimestamp() - triggerTimestamp);
		} else {
			return -1;
		}
	}

	/**
	 * Returns the stats for all subtasks.
	 *
	 * <p>Elements of the returned array are <code>null</code> if no stats are
	 * available yet for the respective subtask.
	 *
	 * <p>Note: The returned array must not be modified.
	 *
	 * @return Array of subtask stats (elements are <code>null</code> if no stats available yet).
	 */
	public SubtaskStateStats[] getSubtaskStats() {
		return subtaskStats;
	}

	/**
	 * @return Summary of the subtask stats.
	 */
	public TaskStateStatsSummary getSummaryStats() {
		return summaryStats;
	}

	/**
	 * Summary of the subtask stats of a single task/operator.
	 */
	public static class TaskStateStatsSummary implements Serializable {

		private static final long serialVersionUID = 1009476026522091909L;

		private MinMaxAvgStats stateSize = new MinMaxAvgStats();
		private MinMaxAvgStats ackTimestamp = new MinMaxAvgStats();
		private MinMaxAvgStats syncCheckpointDuration = new MinMaxAvgStats();
		private MinMaxAvgStats asyncCheckpointDuration = new MinMaxAvgStats();
		private MinMaxAvgStats alignmentDuration = new MinMaxAvgStats();
		private MinMaxAvgStats checkpointStartDelay = new MinMaxAvgStats();

		void updateSummary(SubtaskStateStats subtaskStats) {
			stateSize.add(subtaskStats.getStateSize());
			ackTimestamp.add(subtaskStats.getAckTimestamp());
			syncCheckpointDuration.add(subtaskStats.getSyncCheckpointDuration());
			asyncCheckpointDuration.add(subtaskStats.getAsyncCheckpointDuration());
			alignmentDuration.add(subtaskStats.getAlignmentDuration());
			checkpointStartDelay.add(subtaskStats.getCheckpointStartDelay());
		}

		public MinMaxAvgStats getStateSizeStats() {
			return stateSize;
		}

		public MinMaxAvgStats getAckTimestampStats() {
			return ackTimestamp;
		}

		public MinMaxAvgStats getSyncCheckpointDurationStats() {
			return syncCheckpointDuration;
		}

		public MinMaxAvgStats getAsyncCheckpointDurationStats() {
			return asyncCheckpointDuration;
		}

		public MinMaxAvgStats getAlignmentDurationStats() {
			return alignmentDuration;
		}

		public MinMaxAvgStats getCheckpointStartDelayStats() {
			return checkpointStartDelay;
		}
	}

}
