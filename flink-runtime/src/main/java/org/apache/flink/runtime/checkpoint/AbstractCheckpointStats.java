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
import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for checkpoint statistics.
 */
public abstract class AbstractCheckpointStats implements Serializable {

	private static final long serialVersionUID = 1041218202028265151L;

	/** ID of this checkpoint. */
	final long checkpointId;

	/** Timestamp when the checkpoint was triggered at the coordinator. */
	final long triggerTimestamp;

	/** {@link TaskStateStats} accessible by their ID. */
	final Map<JobVertexID, TaskStateStats> taskStats;

	/** Total number of subtasks over all tasks. */
	final int numberOfSubtasks;

	/** Properties of the checkpoint. */
	final CheckpointProperties props;

	AbstractCheckpointStats(
			long checkpointId,
			long triggerTimestamp,
			CheckpointProperties props,
			int numberOfSubtasks,
			Map<JobVertexID, TaskStateStats> taskStats) {

		this.checkpointId = checkpointId;
		this.triggerTimestamp = triggerTimestamp;
		this.taskStats = checkNotNull(taskStats);
		checkArgument(taskStats.size() > 0, "Empty task stats");
		checkArgument(numberOfSubtasks > 0, "Non-positive number of subtasks");
		this.numberOfSubtasks = numberOfSubtasks;
		this.props = checkNotNull(props);
	}

	/**
	 * Returns the status of this checkpoint.
	 *
	 * @return Status of this checkpoint
	 */
	public abstract CheckpointStatsStatus getStatus();

	/**
	 * Returns the number of acknowledged subtasks.
	 *
	 * @return The number of acknowledged subtasks.
	 */
	public abstract int getNumberOfAcknowledgedSubtasks();

	/**
	 * Returns the total checkpoint state size over all subtasks.
	 *
	 * @return Total checkpoint state size over all subtasks.
	 */
	public abstract long getStateSize();

	/**
	 * Returns the total buffered bytes during alignment over all subtasks.
	 *
	 * <p>Can return <code>-1</code> if the runtime did not report this.
	 *
	 * @return Total buffered bytes during alignment over all subtasks.
	 */
	public abstract long getAlignmentBuffered();

	/**
	 * Returns the latest acknowledged subtask stats or <code>null</code> if
	 * none was acknowledged yet.
	 *
	 * @return Latest acknowledged subtask stats or <code>null</code>
	 */
	@Nullable
	public abstract SubtaskStateStats getLatestAcknowledgedSubtaskStats();

	/**
	 * Returns the ID of this checkpoint.
	 *
	 * @return ID of this checkpoint.
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
	 * Returns the properties of this checkpoint.
	 *
	 * @return Properties of this checkpoint.
	 */
	public CheckpointProperties getProperties() {
		return props;
	}

	/**
	 * Returns the total number of subtasks involved in this checkpoint.
	 *
	 * @return Total number of subtasks involved in this checkpoint.
	 */
	public int getNumberOfSubtasks() {
		return numberOfSubtasks;
	}

	/**
	 * Returns the task state stats for the given job vertex ID or
	 * <code>null</code> if no task with such an ID is available.
	 *
	 * @param jobVertexId Job vertex ID of the task stats to look up.
	 * @return The task state stats instance for the given ID or <code>null</code>.
	 */
	public TaskStateStats getTaskStateStats(JobVertexID jobVertexId) {
		return taskStats.get(jobVertexId);
	}

	/**
	 * Returns all task state stats instances.
	 *
	 * @return All task state stats instances.
	 */
	public Collection<TaskStateStats> getAllTaskStateStats() {
		return taskStats.values();
	}

	/**
	 * Returns the ack timestamp of the latest acknowledged subtask or
	 * <code>-1</code> if none was acknowledged yet.
	 *
	 * @return Ack timestamp of the latest acknowledged subtask or <code>-1</code>.
	 */
	public long getLatestAckTimestamp() {
		SubtaskStateStats subtask = getLatestAcknowledgedSubtaskStats();
		if (subtask != null) {
			return subtask.getAckTimestamp();
		} else {
			return -1;
		}
	}

	/**
	 * Returns the duration of this checkpoint calculated as the time since
	 * triggering until the latest acknowledged subtask or <code>-1</code> if
	 * no subtask was acknowledged yet.
	 *
	 * @return Duration of this checkpoint or <code>-1</code> if no subtask was acknowledged yet.
	 */
	public long getEndToEndDuration() {
		SubtaskStateStats subtask = getLatestAcknowledgedSubtaskStats();
		if (subtask != null) {
			return Math.max(0, subtask.getAckTimestamp() - triggerTimestamp);
		} else {
			return -1;
		}
	}

}
