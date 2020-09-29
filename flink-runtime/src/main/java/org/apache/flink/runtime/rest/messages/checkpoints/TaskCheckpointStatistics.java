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
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Checkpoint statistics for a single task.
 */
public class TaskCheckpointStatistics implements ResponseBody {

	public static final String FIELD_NAME_ID = "id";

	public static final String FIELD_NAME_CHECKPOINT_STATUS = "status";

	public static final String FIELD_NAME_LATEST_ACK_TIMESTAMP = "latest_ack_timestamp";

	/**
	 * The accurate name of this field should be 'checkpointed_data_size',
	 * keep it as before to not break backwards compatibility for old web UI.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-13390">FLINK-13390</a>
	 */
	public static final String FIELD_NAME_STATE_SIZE = "state_size";

	public static final String FIELD_NAME_DURATION = "end_to_end_duration";

	public static final String FIELD_NAME_ALIGNMENT_BUFFERED = "alignment_buffered";

	public static final String FIELD_NAME_PROCESSED_DATA = "processed_data";

	public static final String FIELD_NAME_PERSISTED_DATA = "persisted_data";

	public static final String FIELD_NAME_NUM_SUBTASKS = "num_subtasks";

	public static final String FIELD_NAME_NUM_ACK_SUBTASKS = "num_acknowledged_subtasks";

	@JsonProperty(FIELD_NAME_ID)
	private final long checkpointId;

	@JsonProperty(FIELD_NAME_CHECKPOINT_STATUS)
	private final CheckpointStatsStatus checkpointStatus;

	@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP)
	private final long latestAckTimestamp;

	@JsonProperty(FIELD_NAME_STATE_SIZE)
	private final long stateSize;

	@JsonProperty(FIELD_NAME_DURATION)
	private final long duration;

	@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED)
	private final long alignmentBuffered;

	@JsonProperty(FIELD_NAME_PROCESSED_DATA)
	private final long processedData;

	@JsonProperty(FIELD_NAME_PERSISTED_DATA)
	private final long persistedData;


	@JsonProperty(FIELD_NAME_NUM_SUBTASKS)
	private final int numSubtasks;

	@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS)
	private final int numAckSubtasks;

	@JsonCreator
	public TaskCheckpointStatistics(
			@JsonProperty(FIELD_NAME_ID) long checkpointId,
			@JsonProperty(FIELD_NAME_CHECKPOINT_STATUS) CheckpointStatsStatus checkpointStatus,
			@JsonProperty(FIELD_NAME_LATEST_ACK_TIMESTAMP) long latestAckTimestamp,
			@JsonProperty(FIELD_NAME_STATE_SIZE) long stateSize,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_ALIGNMENT_BUFFERED) long alignmentBuffered,
			@JsonProperty(FIELD_NAME_PROCESSED_DATA) long processedData,
			@JsonProperty(FIELD_NAME_PERSISTED_DATA) long persistedData,
			@JsonProperty(FIELD_NAME_NUM_SUBTASKS) int numSubtasks,
			@JsonProperty(FIELD_NAME_NUM_ACK_SUBTASKS) int numAckSubtasks) {

		this.checkpointId = checkpointId;
		this.checkpointStatus = Preconditions.checkNotNull(checkpointStatus);
		this.latestAckTimestamp = latestAckTimestamp;
		this.stateSize = stateSize;
		this.duration = duration;
		this.processedData = processedData;
		this.alignmentBuffered = alignmentBuffered;
		this.persistedData = persistedData;
		this.numSubtasks = numSubtasks;
		this.numAckSubtasks = numAckSubtasks;
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

	public int getNumSubtasks() {
		return numSubtasks;
	}

	public int getNumAckSubtasks() {
		return numAckSubtasks;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public CheckpointStatsStatus getCheckpointStatus() {
		return checkpointStatus;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TaskCheckpointStatistics that = (TaskCheckpointStatistics) o;
		return checkpointId == that.checkpointId &&
			latestAckTimestamp == that.latestAckTimestamp &&
			stateSize == that.stateSize &&
			duration == that.duration &&
			alignmentBuffered == that.alignmentBuffered &&
			processedData == that.processedData &&
			persistedData == that.persistedData &&
			numSubtasks == that.numSubtasks &&
			numAckSubtasks == that.numAckSubtasks &&
			checkpointStatus == that.checkpointStatus;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
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
	}
}
