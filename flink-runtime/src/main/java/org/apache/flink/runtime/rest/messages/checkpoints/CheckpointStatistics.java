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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.PendingCheckpointStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeySerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Statistics for a checkpoint.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({
	@JsonSubTypes.Type(value = CheckpointStatistics.CompletedCheckpointStatistics.class, name = "completed"),
	@JsonSubTypes.Type(value = CheckpointStatistics.FailedCheckpointStatistics.class, name = "failed"),
	@JsonSubTypes.Type(value = CheckpointStatistics.PendingCheckpointStatistics.class, name = "in_progress")})
public class CheckpointStatistics implements ResponseBody {

	public static final String FIELD_NAME_ID = "id";

	public static final String FIELD_NAME_STATUS = "status";

	public static final String FIELD_NAME_IS_SAVEPOINT = "is_savepoint";

	public static final String FIELD_NAME_TRIGGER_TIMESTAMP = "trigger_timestamp";

	public static final String FIELD_NAME_LATEST_ACK_TIMESTAMP = "latest_ack_timestamp";

	public static final String FIELD_NAME_STATE_SIZE = "state_size";

	public static final String FIELD_NAME_DURATION = "end_to_end_duration";

	public static final String FIELD_NAME_ALIGNMENT_BUFFERED = "alignment_buffered";

	public static final String FIELD_NAME_NUM_SUBTASKS = "num_subtasks";

	public static final String FIELD_NAME_NUM_ACK_SUBTASKS = "num_acknowledged_subtasks";

	public static final String FIELD_NAME_TASKS = "tasks";

	@JsonProperty(FIELD_NAME_ID)
	private final long id;

	@JsonProperty(FIELD_NAME_STATUS)
	private final CheckpointStatsStatus status;

	@JsonProperty(FIELD_NAME_IS_SAVEPOINT)
	private final boolean savepoint;

	@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP)
	private final long triggerTimestamp;

	@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP)
	private final long latestAckTimestamp;

	@JsonProperty(FIELD_NAME_STATE_SIZE)
	private final long stateSize;

	@JsonProperty(FIELD_NAME_DURATION)
	private final long duration;

	@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED)
	private final long alignmentBuffered;

	@JsonProperty(FIELD_NAME_NUM_SUBTASKS)
	private final int numSubtasks;

	@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS)
	private final int numAckSubtasks;

	@JsonProperty(FIELD_NAME_TASKS)
	@JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
	private final Map<JobVertexID, TaskCheckpointStatistics> checkpointStatisticsPerTask;

	@JsonCreator
	private CheckpointStatistics(
			@JsonProperty(FIELD_NAME_ID) long id,
			@JsonProperty(FIELD_NAME_STATUS) CheckpointStatsStatus status,
			@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
			@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) long triggerTimestamp,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks,
			@JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class) @JsonProperty(FIELD_NAME_TASKS) Map<JobVertexID, TaskCheckpointStatistics> checkpointStatisticsPerTask) {
		this.id = id;
		this.status = Preconditions.checkNotNull(status);
		this.savepoint = savepoint;
		this.triggerTimestamp = triggerTimestamp;
		this.latestAckTimestamp = latestAckTimestamp;
		this.stateSize = stateSize;
		this.duration = duration;
		this.alignmentBuffered = alignmentBuffered;
		this.numSubtasks = numSubtasks;
		this.numAckSubtasks = numAckSubtasks;
		this.checkpointStatisticsPerTask = Preconditions.checkNotNull(checkpointStatisticsPerTask);
	}

	public long getId() {
		return id;
	}

	public CheckpointStatsStatus getStatus() {
		return status;
	}

	public boolean isSavepoint() {
		return savepoint;
	}

	public long getTriggerTimestamp() {
		return triggerTimestamp;
	}

	public long getLatestAckTimestamp() {
		return latestAckTimestamp;
	}

	public long getStateSize() {
		return stateSize;
	}

	public long getDuration() {
		return duration;
	}

	public long getAlignmentBuffered() {
		return alignmentBuffered;
	}

	public int getNumSubtasks() {
		return numSubtasks;
	}

	public int getNumAckSubtasks() {
		return numAckSubtasks;
	}

	@Nullable
	public Map<JobVertexID, TaskCheckpointStatistics> getCheckpointStatisticsPerTask() {
		return checkpointStatisticsPerTask;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointStatistics that = (CheckpointStatistics) o;
		return id == that.id &&
			savepoint == that.savepoint &&
			triggerTimestamp == that.triggerTimestamp &&
			latestAckTimestamp == that.latestAckTimestamp &&
			stateSize == that.stateSize &&
			duration == that.duration &&
			alignmentBuffered == that.alignmentBuffered &&
			numSubtasks == that.numSubtasks &&
			numAckSubtasks == that.numAckSubtasks &&
			status == that.status &&
			Objects.equals(checkpointStatisticsPerTask, that.checkpointStatisticsPerTask);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, status, savepoint, triggerTimestamp, latestAckTimestamp, stateSize, duration, alignmentBuffered, numSubtasks, numAckSubtasks, checkpointStatisticsPerTask);
	}

	// -------------------------------------------------------------------------
	// Static factory methods
	// -------------------------------------------------------------------------

	public static CheckpointStatistics generateCheckpointStatistics(AbstractCheckpointStats checkpointStats, boolean includeTaskCheckpointStatistics) {
		Preconditions.checkNotNull(checkpointStats);

		Map<JobVertexID, TaskCheckpointStatistics> checkpointStatisticsPerTask;

		if (includeTaskCheckpointStatistics) {
			Collection<TaskStateStats> taskStateStats = checkpointStats.getAllTaskStateStats();

			checkpointStatisticsPerTask = new HashMap<>(taskStateStats.size());

			for (TaskStateStats taskStateStat : taskStateStats) {
				checkpointStatisticsPerTask.put(
					taskStateStat.getJobVertexId(),
					new TaskCheckpointStatistics(
							checkpointStats.getCheckpointId(),
							checkpointStats.getStatus(),
						taskStateStat.getLatestAckTimestamp(),
						taskStateStat.getStateSize(),
						taskStateStat.getEndToEndDuration(checkpointStats.getTriggerTimestamp()),
						taskStateStat.getAlignmentBuffered(),
						taskStateStat.getNumberOfSubtasks(),
						taskStateStat.getNumberOfAcknowledgedSubtasks()));
			}
		} else {
			checkpointStatisticsPerTask = Collections.emptyMap();
		}

		if (checkpointStats instanceof CompletedCheckpointStats) {
			final CompletedCheckpointStats completedCheckpointStats = ((CompletedCheckpointStats) checkpointStats);

			return new CheckpointStatistics.CompletedCheckpointStatistics(
				completedCheckpointStats.getCheckpointId(),
				completedCheckpointStats.getStatus(),
				completedCheckpointStats.getProperties().isSavepoint(),
				completedCheckpointStats.getTriggerTimestamp(),
				completedCheckpointStats.getLatestAckTimestamp(),
				completedCheckpointStats.getStateSize(),
				completedCheckpointStats.getEndToEndDuration(),
				completedCheckpointStats.getAlignmentBuffered(),
				completedCheckpointStats.getNumberOfSubtasks(),
				completedCheckpointStats.getNumberOfAcknowledgedSubtasks(),
				checkpointStatisticsPerTask,
				completedCheckpointStats.getExternalPath(),
				completedCheckpointStats.isDiscarded());
		} else if (checkpointStats instanceof FailedCheckpointStats) {
			final FailedCheckpointStats failedCheckpointStats = ((FailedCheckpointStats) checkpointStats);

			return new CheckpointStatistics.FailedCheckpointStatistics(
				failedCheckpointStats.getCheckpointId(),
				failedCheckpointStats.getStatus(),
				failedCheckpointStats.getProperties().isSavepoint(),
				failedCheckpointStats.getTriggerTimestamp(),
				failedCheckpointStats.getLatestAckTimestamp(),
				failedCheckpointStats.getStateSize(),
				failedCheckpointStats.getEndToEndDuration(),
				failedCheckpointStats.getAlignmentBuffered(),
				failedCheckpointStats.getNumberOfSubtasks(),
				failedCheckpointStats.getNumberOfAcknowledgedSubtasks(),
				checkpointStatisticsPerTask,
				failedCheckpointStats.getFailureTimestamp(),
				failedCheckpointStats.getFailureMessage());
		} else if (checkpointStats instanceof PendingCheckpointStats) {
			final PendingCheckpointStats pendingCheckpointStats = ((PendingCheckpointStats) checkpointStats);

			return new CheckpointStatistics.PendingCheckpointStatistics(
				pendingCheckpointStats.getCheckpointId(),
				pendingCheckpointStats.getStatus(),
				pendingCheckpointStats.getProperties().isSavepoint(),
				pendingCheckpointStats.getTriggerTimestamp(),
				pendingCheckpointStats.getLatestAckTimestamp(),
				pendingCheckpointStats.getStateSize(),
				pendingCheckpointStats.getEndToEndDuration(),
				pendingCheckpointStats.getAlignmentBuffered(),
				pendingCheckpointStats.getNumberOfSubtasks(),
				pendingCheckpointStats.getNumberOfAcknowledgedSubtasks(),
				checkpointStatisticsPerTask
			);
		} else {
			throw new IllegalArgumentException("Given checkpoint stats object of type "
				+ checkpointStats.getClass().getName() + " cannot be converted.");
		}
	}

	// ---------------------------------------------------------------------
	// Static inner classes
	// ---------------------------------------------------------------------

	/**
	 * Statistics for a completed checkpoint.
	 */
	public static final class CompletedCheckpointStatistics extends CheckpointStatistics {

		public static final String FIELD_NAME_EXTERNAL_PATH = "external_path";

		public static final String FIELD_NAME_DISCARDED = "discarded";

		@JsonProperty(FIELD_NAME_EXTERNAL_PATH)
		@Nullable
		private final String externalPath;

		@JsonProperty(FIELD_NAME_DISCARDED)
		private final boolean discarded;

		@JsonCreator
		public CompletedCheckpointStatistics(
			@JsonProperty(FIELD_NAME_ID) long id,
			@JsonProperty(FIELD_NAME_STATUS) CheckpointStatsStatus status,
			@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
			@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) long triggerTimestamp,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks,
			@JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class) @JsonProperty(FIELD_NAME_TASKS) Map<JobVertexID, TaskCheckpointStatistics> checkpointingStatisticsPerTask,
			@JsonProperty(FIELD_NAME_EXTERNAL_PATH) @Nullable String externalPath,
			@JsonProperty(FIELD_NAME_DISCARDED) boolean discarded) {
			super(
				id,
				status,
				savepoint,
				triggerTimestamp,
				latestAckTimestamp,
				stateSize,
				duration,
				alignmentBuffered,
				numSubtasks,
				numAckSubtasks,
				checkpointingStatisticsPerTask);

			this.externalPath = externalPath;
			this.discarded = discarded;
		}

		@Nullable
		public String getExternalPath() {
			return externalPath;
		}

		public boolean isDiscarded() {
			return discarded;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			CompletedCheckpointStatistics that = (CompletedCheckpointStatistics) o;
			return discarded == that.discarded &&
				Objects.equals(externalPath, that.externalPath);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), externalPath, discarded);
		}
	}

	/**
	 * Statistics for a failed checkpoint.
	 */
	public static final class FailedCheckpointStatistics extends CheckpointStatistics {

		public static final String FIELD_NAME_FAILURE_TIMESTAMP = "failure_timestamp";

		public static final String FIELD_NAME_FAILURE_MESSAGE = "failure_message";

		@JsonProperty(FIELD_NAME_FAILURE_TIMESTAMP)
		private final long failureTimestamp;

		@JsonProperty(FIELD_NAME_FAILURE_MESSAGE)
		@Nullable
		private final String failureMessage;

		@JsonCreator
		public FailedCheckpointStatistics(
			@JsonProperty(FIELD_NAME_ID) long id,
			@JsonProperty(FIELD_NAME_STATUS) CheckpointStatsStatus status,
			@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
			@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) long triggerTimestamp,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks,
			@JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class) @JsonProperty(FIELD_NAME_TASKS) Map<JobVertexID, TaskCheckpointStatistics> checkpointingStatisticsPerTask,
			@JsonProperty(FIELD_NAME_FAILURE_TIMESTAMP) long failureTimestamp,
			@JsonProperty(FIELD_NAME_FAILURE_MESSAGE) @Nullable String failureMessage) {
			super(
				id,
				status,
				savepoint,
				triggerTimestamp,
				latestAckTimestamp,
				stateSize,
				duration,
				alignmentBuffered,
				numSubtasks,
				numAckSubtasks,
				checkpointingStatisticsPerTask);

			this.failureTimestamp = failureTimestamp;
			this.failureMessage = failureMessage;
		}

		public long getFailureTimestamp() {
			return failureTimestamp;
		}

		@Nullable
		public String getFailureMessage() {
			return failureMessage;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			FailedCheckpointStatistics that = (FailedCheckpointStatistics) o;
			return failureTimestamp == that.failureTimestamp &&
				Objects.equals(failureMessage, that.failureMessage);
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode(), failureTimestamp, failureMessage);
		}
	}

	/**
	 * Statistics for a pending checkpoint.
	 */
	public static final class PendingCheckpointStatistics extends CheckpointStatistics {

		@JsonCreator
		public PendingCheckpointStatistics(
			@JsonProperty(FIELD_NAME_ID) long id,
			@JsonProperty(FIELD_NAME_STATUS) CheckpointStatsStatus status,
			@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
			@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) long triggerTimestamp,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks,
			@JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class) @JsonProperty(FIELD_NAME_TASKS) Map<JobVertexID, TaskCheckpointStatistics> checkpointingStatisticsPerTask) {
			super(
				id,
				status,
				savepoint,
				triggerTimestamp,
				latestAckTimestamp,
				stateSize,
				duration,
				alignmentBuffered,
				numSubtasks,
				numAckSubtasks,
				checkpointingStatisticsPerTask);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return Objects.hash(super.hashCode());
		}

	}
}
