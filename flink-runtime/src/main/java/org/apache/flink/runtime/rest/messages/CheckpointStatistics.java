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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatisticsHandler;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Response of the {@link CheckpointStatisticsHandler}.
 */
public class CheckpointStatistics implements ResponseBody {

	public static final String FIELD_NAME_COUNTS = "counts";

	public static final String FIELD_NAME_SUMMARY = "summary";

	public static final String FIELD_NAME_LATEST_CHECKPOINTS = "latest";

	public static final String FIELD_NAME_HISTORY = "history";

	@JsonProperty(FIELD_NAME_COUNTS)
	private final Counts counts;

	@JsonProperty(FIELD_NAME_SUMMARY)
	private final Summary summary;

	@JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS)
	private final LatestCheckpoints latestCheckpoints;

	@JsonProperty(FIELD_NAME_HISTORY)
	private final List<BaseCheckpointStatistics> history;

	@JsonCreator
	public CheckpointStatistics(
			@JsonProperty(FIELD_NAME_COUNTS) Counts counts,
			@JsonProperty(FIELD_NAME_SUMMARY) Summary summary,
			@JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS) LatestCheckpoints latestCheckpoints,
			@JsonProperty(FIELD_NAME_HISTORY) List<BaseCheckpointStatistics> history) {
		this.counts = Preconditions.checkNotNull(counts);
		this.summary = Preconditions.checkNotNull(summary);
		this.latestCheckpoints = Preconditions.checkNotNull(latestCheckpoints);
		this.history = Preconditions.checkNotNull(history);
	}

	public Counts getCounts() {
		return counts;
	}

	public Summary getSummary() {
		return summary;
	}

	public LatestCheckpoints getLatestCheckpoints() {
		return latestCheckpoints;
	}

	public List<BaseCheckpointStatistics> getHistory() {
		return history;
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
		return Objects.equals(counts, that.counts) &&
			Objects.equals(summary, that.summary) &&
			Objects.equals(latestCheckpoints, that.latestCheckpoints) &&
			Objects.equals(history, that.history);
	}

	@Override
	public int hashCode() {
		return Objects.hash(counts, summary, latestCheckpoints, history);
	}

	// ------------------------------------------------------------------
	// Inner classes
	// ------------------------------------------------------------------

	/**
	 * Checkpoint counts.
	 */
	public static final class Counts {

		public static final String FIELD_NAME_RESTORED_CHECKPOINTS = "restored";

		public static final String FIELD_NAME_TOTAL_CHECKPOINTS = "total";

		public static final String FIELD_NAME_IN_PROGRESS_CHECKPOINTS = "in_progress";

		public static final String FIELD_NAME_COMPLETED_CHECKPOINTS = "completed";

		public static final String FIELD_NAME_FAILED_CHECKPOINTS = "failed";

		@JsonProperty(FIELD_NAME_RESTORED_CHECKPOINTS)
		private final long numberRestoredCheckpoints;

		@JsonProperty(FIELD_NAME_TOTAL_CHECKPOINTS)
		private final long totalNumberCheckpoints;

		@JsonProperty(FIELD_NAME_IN_PROGRESS_CHECKPOINTS)
		private final int numberInProgressCheckpoints;

		@JsonProperty(FIELD_NAME_COMPLETED_CHECKPOINTS)
		private final long numberCompletedCheckpoints;

		@JsonProperty(FIELD_NAME_FAILED_CHECKPOINTS)
		private final long numberFailedCheckpoints;

		@JsonCreator
		public Counts(
				@JsonProperty(FIELD_NAME_RESTORED_CHECKPOINTS) long numberRestoredCheckpoints,
				@JsonProperty(FIELD_NAME_TOTAL_CHECKPOINTS) long totalNumberCheckpoints,
				@JsonProperty(FIELD_NAME_IN_PROGRESS_CHECKPOINTS) int numberInProgressCheckpoints,
				@JsonProperty(FIELD_NAME_COMPLETED_CHECKPOINTS) long numberCompletedCheckpoints,
				@JsonProperty(FIELD_NAME_FAILED_CHECKPOINTS) long numberFailedCheckpoints) {
			this.numberRestoredCheckpoints = numberRestoredCheckpoints;
			this.totalNumberCheckpoints = totalNumberCheckpoints;
			this.numberInProgressCheckpoints = numberInProgressCheckpoints;
			this.numberCompletedCheckpoints = numberCompletedCheckpoints;
			this.numberFailedCheckpoints = numberFailedCheckpoints;
		}

		public long getNumberRestoredCheckpoints() {
			return numberRestoredCheckpoints;
		}

		public long getTotalNumberCheckpoints() {
			return totalNumberCheckpoints;
		}

		public int getNumberInProgressCheckpoints() {
			return numberInProgressCheckpoints;
		}

		public long getNumberCompletedCheckpoints() {
			return numberCompletedCheckpoints;
		}

		public long getNumberFailedCheckpoints() {
			return numberFailedCheckpoints;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Counts counts = (Counts) o;
			return numberRestoredCheckpoints == counts.numberRestoredCheckpoints &&
				totalNumberCheckpoints == counts.totalNumberCheckpoints &&
				numberInProgressCheckpoints == counts.numberInProgressCheckpoints &&
				numberCompletedCheckpoints == counts.numberCompletedCheckpoints &&
				numberFailedCheckpoints == counts.numberFailedCheckpoints;
		}

		@Override
		public int hashCode() {
			return Objects.hash(numberRestoredCheckpoints, totalNumberCheckpoints, numberInProgressCheckpoints, numberCompletedCheckpoints, numberFailedCheckpoints);
		}
	}

	/**
	 * Checkpoint summary.
	 */
	public static final class Summary {

		public static final String FIELD_NAME_STATE_SIZE = "state_size";

		public static final String FIELD_NAME_DURATION = "end_to_end_duration";

		public static final String FIELD_NAME_ALIGNMENT_BUFFERED = "alignment_buffered";

		@JsonProperty(FIELD_NAME_STATE_SIZE)
		private final MinMaxAvgStatistics stateSize;

		@JsonProperty(FIELD_NAME_DURATION)
		private final MinMaxAvgStatistics duration;

		@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED)
		private final MinMaxAvgStatistics alignmentBuffered;

		@JsonCreator
		public Summary(
				@JsonProperty(FIELD_NAME_STATE_SIZE) MinMaxAvgStatistics stateSize,
				@JsonProperty(FIELD_NAME_DURATION) MinMaxAvgStatistics duration,
				@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) MinMaxAvgStatistics alignmentBuffered) {
			this.stateSize = stateSize;
			this.duration = duration;
			this.alignmentBuffered = alignmentBuffered;
		}

		public MinMaxAvgStatistics getStateSize() {
			return stateSize;
		}

		public MinMaxAvgStatistics getDuration() {
			return duration;
		}

		public MinMaxAvgStatistics getAlignmentBuffered() {
			return alignmentBuffered;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Summary summary = (Summary) o;
			return Objects.equals(stateSize, summary.stateSize) &&
				Objects.equals(duration, summary.duration) &&
				Objects.equals(alignmentBuffered, summary.alignmentBuffered);
		}

		@Override
		public int hashCode() {
			return Objects.hash(stateSize, duration, alignmentBuffered);
		}
	}

	/**
	 * Minimum, maximum and average statistics.
	 */
	public static final class MinMaxAvgStatistics {

		public static final String FIELD_NAME_MINIMUM = "min";

		public static final String FIELD_NAME_MAXIMUM = "max";

		public static final String FIELD_NAME_AVERAGE = "avg";

		@JsonProperty(FIELD_NAME_MINIMUM)
		private final long minimum;

		@JsonProperty(FIELD_NAME_MAXIMUM)
		private final long maximum;

		@JsonProperty(FIELD_NAME_AVERAGE)
		private final long average;

		@JsonCreator
		public MinMaxAvgStatistics(
				@JsonProperty(FIELD_NAME_MINIMUM) long minimum,
				@JsonProperty(FIELD_NAME_MAXIMUM) long maximum,
				@JsonProperty(FIELD_NAME_AVERAGE) long average) {
			this.minimum = minimum;
			this.maximum = maximum;
			this.average = average;
		}

		public long getMinimum() {
			return minimum;
		}

		public long getMaximum() {
			return maximum;
		}

		public long getAverage() {
			return average;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			MinMaxAvgStatistics that = (MinMaxAvgStatistics) o;
			return minimum == that.minimum &&
				maximum == that.maximum &&
				average == that.average;
		}

		@Override
		public int hashCode() {
			return Objects.hash(minimum, maximum, average);
		}
	}

	/**
	 * Statistics about the latest checkpoints.
	 */
	public static final class LatestCheckpoints {

		public static final String FIELD_NAME_COMPLETED = "completed";

		public static final String FIELD_NAME_SAVEPOINT = "savepoint";

		public static final String FIELD_NAME_FAILED = "failed";

		public static final String FIELD_NAME_RESTORED = "restored";

		@JsonProperty(FIELD_NAME_COMPLETED)
		@Nullable
		private final CompletedCheckpointStatistics completedCheckpointStatistics;

		@JsonProperty(FIELD_NAME_SAVEPOINT)
		@Nullable
		private final CompletedCheckpointStatistics savepointStatistics;

		@JsonProperty(FIELD_NAME_FAILED)
		@Nullable
		private final FailedCheckpointStatistics failedCheckpointStatistics;

		@JsonProperty(FIELD_NAME_RESTORED)
		@Nullable
		private final RestoredCheckpointStatistics restoredCheckpointStatistics;

		@JsonCreator
		public LatestCheckpoints(
				@JsonProperty(FIELD_NAME_COMPLETED) @Nullable CompletedCheckpointStatistics completedCheckpointStatistics,
				@JsonProperty(FIELD_NAME_SAVEPOINT) @Nullable CompletedCheckpointStatistics savepointStatistics,
				@JsonProperty(FIELD_NAME_FAILED) @Nullable FailedCheckpointStatistics failedCheckpointStatistics,
				@JsonProperty(FIELD_NAME_RESTORED) @Nullable RestoredCheckpointStatistics restoredCheckpointStatistics) {
			this.completedCheckpointStatistics = completedCheckpointStatistics;
			this.savepointStatistics = savepointStatistics;
			this.failedCheckpointStatistics = failedCheckpointStatistics;
			this.restoredCheckpointStatistics = restoredCheckpointStatistics;
		}

		@Nullable
		public CompletedCheckpointStatistics getCompletedCheckpointStatistics() {
			return completedCheckpointStatistics;
		}

		@Nullable
		public CompletedCheckpointStatistics getSavepointStatistics() {
			return savepointStatistics;
		}

		@Nullable
		public FailedCheckpointStatistics getFailedCheckpointStatistics() {
			return failedCheckpointStatistics;
		}

		@Nullable
		public RestoredCheckpointStatistics getRestoredCheckpointStatistics() {
			return restoredCheckpointStatistics;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			LatestCheckpoints that = (LatestCheckpoints) o;
			return Objects.equals(completedCheckpointStatistics, that.completedCheckpointStatistics) &&
				Objects.equals(savepointStatistics, that.savepointStatistics) &&
				Objects.equals(failedCheckpointStatistics, that.failedCheckpointStatistics) &&
				Objects.equals(restoredCheckpointStatistics, that.restoredCheckpointStatistics);
		}

		@Override
		public int hashCode() {
			return Objects.hash(completedCheckpointStatistics, savepointStatistics, failedCheckpointStatistics, restoredCheckpointStatistics);
		}
	}

	/**
	 * Statistics for a checkpoint.
	 */
	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@class")
	@JsonSubTypes({
		@JsonSubTypes.Type(value = CompletedCheckpointStatistics.class, name = "completed"),
		@JsonSubTypes.Type(value = FailedCheckpointStatistics.class, name = "failed")})
	public static class BaseCheckpointStatistics {

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

		@JsonCreator
		protected BaseCheckpointStatistics(
				@JsonProperty(FIELD_NAME_ID) long id,
				@JsonProperty(FIELD_NAME_STATUS) CheckpointStatsStatus status,
				@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
				@JsonProperty(FIELD_NAME_TRIGGER_TIMESTAMP) long triggerTimestamp,
				@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
				@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
				@JsonProperty(FIELD_NAME_DURATION) long duration,
				@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
				@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
				@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks) {
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

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			BaseCheckpointStatistics that = (BaseCheckpointStatistics) o;
			return id == that.id &&
				savepoint == that.savepoint &&
				triggerTimestamp == that.triggerTimestamp &&
				latestAckTimestamp == that.latestAckTimestamp &&
				stateSize == that.stateSize &&
				duration == that.duration &&
				alignmentBuffered == that.alignmentBuffered &&
				numSubtasks == that.numSubtasks &&
				numAckSubtasks == that.numAckSubtasks &&
				status == that.status;
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, status, savepoint, triggerTimestamp, latestAckTimestamp, stateSize, duration, alignmentBuffered, numSubtasks, numAckSubtasks);
		}
	}

	/**
	 * Statistics for a completed checkpoint.
	 */
	public static final class CompletedCheckpointStatistics extends BaseCheckpointStatistics {

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
				@JsonProperty(FIELD_NAME_EXTERNAL_PATH) @Nullable String externalPath,
				@JsonProperty(FIELD_NAME_DISCARDED) boolean discarded) {
			super(id, status, savepoint, triggerTimestamp, latestAckTimestamp, stateSize, duration, alignmentBuffered, numSubtasks, numAckSubtasks);

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
	public static final class FailedCheckpointStatistics extends BaseCheckpointStatistics {

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
				@JsonProperty(FIELD_NAME_FAILURE_TIMESTAMP) long failureTimestamp,
				@JsonProperty(FIELD_NAME_FAILURE_MESSAGE) @Nullable String failureMessage) {
			super(id, status, savepoint, triggerTimestamp, latestAckTimestamp, stateSize, duration, alignmentBuffered, numSubtasks, numAckSubtasks);

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
	 * Statistics for a restored checkpoint.
	 */
	public static final class RestoredCheckpointStatistics {

		public static final String FIELD_NAME_ID = "id";

		public static final String FIELD_NAME_RESTORE_TIMESTAMP = "restore_timestamp";

		public static final String FIELD_NAME_IS_SAVEPOINT = "is_savepoint";

		public static final String FIELD_NAME_EXTERNAL_PATH = "external_path";

		@JsonProperty(FIELD_NAME_ID)
		private final long id;

		@JsonProperty(FIELD_NAME_RESTORE_TIMESTAMP)
		private final long restoreTimestamp;

		@JsonProperty(FIELD_NAME_IS_SAVEPOINT)
		private final boolean savepoint;

		@JsonProperty(FIELD_NAME_EXTERNAL_PATH)
		@Nullable
		private final String externalPath;

		@JsonCreator
		public RestoredCheckpointStatistics(
				@JsonProperty(FIELD_NAME_ID) long id,
				@JsonProperty(FIELD_NAME_RESTORE_TIMESTAMP) long restoreTimestamp,
				@JsonProperty(FIELD_NAME_IS_SAVEPOINT) boolean savepoint,
				@JsonProperty(FIELD_NAME_EXTERNAL_PATH) @Nullable String externalPath) {
			this.id = id;
			this.restoreTimestamp = restoreTimestamp;
			this.savepoint = savepoint;
			this.externalPath = externalPath;
		}

		public long getId() {
			return id;
		}

		public long getRestoreTimestamp() {
			return restoreTimestamp;
		}

		public boolean isSavepoint() {
			return savepoint;
		}

		@Nullable
		public String getExternalPath() {
			return externalPath;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			RestoredCheckpointStatistics that = (RestoredCheckpointStatistics) o;
			return id == that.id &&
				restoreTimestamp == that.restoreTimestamp &&
				savepoint == that.savepoint &&
				Objects.equals(externalPath, that.externalPath);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, restoreTimestamp, savepoint, externalPath);
		}
	}
}
