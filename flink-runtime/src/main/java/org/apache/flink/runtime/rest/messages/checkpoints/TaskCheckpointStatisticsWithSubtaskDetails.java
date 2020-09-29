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

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Task checkpoint statistics which also includes information about the sub task
 * checkpoint statistics.
 */
public final class TaskCheckpointStatisticsWithSubtaskDetails extends TaskCheckpointStatistics {

	public static final String FIELD_NAME_SUMMARY = "summary";

	public static final String FIELD_NAME_SUBTASKS_CHECKPOINT_STATISTICS = "subtasks";

	@JsonProperty(FIELD_NAME_SUMMARY)
	private final Summary summary;

	@JsonProperty(FIELD_NAME_SUBTASKS_CHECKPOINT_STATISTICS)
	private final List<SubtaskCheckpointStatistics> subtaskCheckpointStatistics;

	@JsonCreator
	public TaskCheckpointStatisticsWithSubtaskDetails(
			@JsonProperty(FIELD_NAME_ID) long checkpointId,
			@JsonProperty(FIELD_NAME_CHECKPOINT_STATUS) CheckpointStatsStatus checkpointStatus,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_PROCESSED_DATA) long processedData,
			@JsonProperty(FIELD_NAME_PERSISTED_DATA) long persistedData,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks,
			@JsonProperty(FIELD_NAME_SUMMARY) Summary summary,
			@JsonProperty(FIELD_NAME_SUBTASKS_CHECKPOINT_STATISTICS) List<SubtaskCheckpointStatistics> subtaskCheckpointStatistics) {
		super(
			checkpointId,
			checkpointStatus,
			latestAckTimestamp,
			stateSize,
			duration,
			alignmentBuffered,
			processedData,
			persistedData,
			numSubtasks,
			numAckSubtasks);

		this.summary = Preconditions.checkNotNull(summary);
		this.subtaskCheckpointStatistics = Preconditions.checkNotNull(subtaskCheckpointStatistics);
	}

	public Summary getSummary() {
		return summary;
	}

	public List<SubtaskCheckpointStatistics> getSubtaskCheckpointStatistics() {
		return subtaskCheckpointStatistics;
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
		TaskCheckpointStatisticsWithSubtaskDetails that = (TaskCheckpointStatisticsWithSubtaskDetails) o;
		return Objects.equals(summary, that.summary) &&
			Objects.equals(subtaskCheckpointStatistics, that.subtaskCheckpointStatistics);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), summary, subtaskCheckpointStatistics);
	}

	// -----------------------------------------------------------------------------------
	// Static inner classes
	// -----------------------------------------------------------------------------------

	/**
	 * Summary of the checkpoint statistics for a given task.
	 */
	public static final class Summary {

		/**
		 * The accurate name of this field should be 'checkpointed_data_size',
		 * keep it as before to not break backwards compatibility for old web UI.
		 *
		 * @see <a href="https://issues.apache.org/jira/browse/FLINK-13390">FLINK-13390</a>
		 */
		public static final String FIELD_NAME_STATE_SIZE = "state_size";

		public static final String FIELD_NAME_DURATION = "end_to_end_duration";

		public static final String FIELD_NAME_CHECKPOINT_DURATION = "checkpoint_duration";

		public static final String FIELD_NAME_ALIGNMENT = "alignment";

		public static final String FIELD_NAME_START_DELAY = "start_delay";

		@JsonProperty(FIELD_NAME_STATE_SIZE)
		private final MinMaxAvgStatistics stateSize;

		@JsonProperty(FIELD_NAME_DURATION)
		private final MinMaxAvgStatistics duration;

		@JsonProperty(FIELD_NAME_CHECKPOINT_DURATION)
		private final CheckpointDuration checkpointDuration;

		@JsonProperty(FIELD_NAME_ALIGNMENT)
		private final CheckpointAlignment checkpointAlignment;

		@JsonProperty(FIELD_NAME_START_DELAY)
		private final MinMaxAvgStatistics checkpointStartDelay;

		@JsonCreator
		public Summary(
				@JsonProperty(FIELD_NAME_STATE_SIZE) MinMaxAvgStatistics stateSize,
				@JsonProperty(FIELD_NAME_DURATION) MinMaxAvgStatistics duration,
				@JsonProperty(FIELD_NAME_CHECKPOINT_DURATION) CheckpointDuration checkpointDuration,
				@JsonProperty(FIELD_NAME_ALIGNMENT) CheckpointAlignment checkpointAlignment,
				@JsonProperty(FIELD_NAME_START_DELAY) MinMaxAvgStatistics checkpointStartDelay) {
			this.stateSize = Preconditions.checkNotNull(stateSize);
			this.duration = Preconditions.checkNotNull(duration);
			this.checkpointDuration = Preconditions.checkNotNull(checkpointDuration);
			this.checkpointAlignment = Preconditions.checkNotNull(checkpointAlignment);
			this.checkpointStartDelay = Preconditions.checkNotNull(checkpointStartDelay);
		}

		public MinMaxAvgStatistics getStateSize() {
			return stateSize;
		}

		public MinMaxAvgStatistics getDuration() {
			return duration;
		}

		public CheckpointDuration getCheckpointDuration() {
			return checkpointDuration;
		}

		public CheckpointAlignment getCheckpointAlignment() {
			return checkpointAlignment;
		}

		public MinMaxAvgStatistics getCheckpointStartDelay() {
			return checkpointStartDelay;
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
				Objects.equals(checkpointDuration, summary.checkpointDuration) &&
				Objects.equals(checkpointAlignment, summary.checkpointAlignment) &&
				Objects.equals(checkpointStartDelay, summary.checkpointStartDelay);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				stateSize,
				duration,
				checkpointDuration,
				checkpointAlignment,
				checkpointStartDelay);
		}
	}

	/**
	 * Duration of a checkpoint split up into its synchronous and asynchronous part.
	 */
	public static final class CheckpointDuration {

		public static final String FIELD_NAME_SYNCHRONOUS_DURATION = "sync";

		public static final String FIELD_NAME_ASYNCHRONOUS_DURATION = "async";

		@JsonProperty(FIELD_NAME_SYNCHRONOUS_DURATION)
		private final MinMaxAvgStatistics synchronousDuration;

		@JsonProperty(FIELD_NAME_ASYNCHRONOUS_DURATION)
		private final MinMaxAvgStatistics asynchronousDuration;

		@JsonCreator
		public CheckpointDuration(
			@JsonProperty(FIELD_NAME_SYNCHRONOUS_DURATION) MinMaxAvgStatistics synchronousDuration,
			@JsonProperty(FIELD_NAME_ASYNCHRONOUS_DURATION) MinMaxAvgStatistics asynchronousDuration) {
			this.synchronousDuration = Preconditions.checkNotNull(synchronousDuration);
			this.asynchronousDuration = Preconditions.checkNotNull(asynchronousDuration);
		}

		public MinMaxAvgStatistics getSynchronousDuration() {
			return synchronousDuration;
		}

		public MinMaxAvgStatistics getAsynchronousDuration() {
			return asynchronousDuration;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CheckpointDuration that = (CheckpointDuration) o;
			return Objects.equals(synchronousDuration, that.synchronousDuration) &&
				Objects.equals(asynchronousDuration, that.asynchronousDuration);
		}

		@Override
		public int hashCode() {
			return Objects.hash(synchronousDuration, asynchronousDuration);
		}
	}

	/**
	 * Alignment information for a specific checkpoint at a given task.
	 */
	public static final class CheckpointAlignment {

		public static final String FIELD_NAME_BUFFERED_DATA = "buffered";

		public static final String FIELD_NAME_PROCESSED = "processed";

		public static final String FIELD_NAME_PERSISTED = "persisted";

		public static final String FIELD_NAME_DURATION = "duration";

		@JsonProperty(FIELD_NAME_BUFFERED_DATA)
		private final MinMaxAvgStatistics bufferedData;

		@JsonProperty(FIELD_NAME_PROCESSED)
		private final MinMaxAvgStatistics processedData;

		@JsonProperty(FIELD_NAME_PERSISTED)
		private final MinMaxAvgStatistics persistedData;

		@JsonProperty(FIELD_NAME_DURATION)
		private final MinMaxAvgStatistics duration;

		@JsonCreator
		public CheckpointAlignment(
			@JsonProperty(FIELD_NAME_BUFFERED_DATA) MinMaxAvgStatistics bufferedData,
			@JsonProperty(FIELD_NAME_PROCESSED) MinMaxAvgStatistics processedData,
			@JsonProperty(FIELD_NAME_PERSISTED) MinMaxAvgStatistics persistedData,
			@JsonProperty(FIELD_NAME_DURATION) MinMaxAvgStatistics duration) {
			this.bufferedData = bufferedData;
			this.processedData = processedData;
			this.persistedData = persistedData;
			this.duration = duration;
		}

		public MinMaxAvgStatistics getBufferedData() {
			return bufferedData;
		}

		public MinMaxAvgStatistics getProcessedData() {
			return processedData;
		}

		public MinMaxAvgStatistics getPersistedData() {
			return persistedData;
		}

		public MinMaxAvgStatistics getDuration() {
			return duration;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CheckpointAlignment alignment = (CheckpointAlignment) o;
			return Objects.equals(bufferedData, alignment.bufferedData) &&
				Objects.equals(processedData, alignment.processedData) &&
				Objects.equals(persistedData, alignment.persistedData) &&
				Objects.equals(duration, alignment.duration);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				bufferedData,
				processedData,
				persistedData,
				duration);
		}
	}
}
