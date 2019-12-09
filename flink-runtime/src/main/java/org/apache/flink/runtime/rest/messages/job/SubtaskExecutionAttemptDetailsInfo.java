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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.SubTaskIOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The sub task execution attempt response.
 */
public class SubtaskExecutionAttemptDetailsInfo implements ResponseBody {

	public static final String FIELD_NAME_SUBTASK_INDEX = "subtask";

	public static final String FIELD_NAME_STATUS = "status";

	public static final String FIELD_NAME_ATTEMPT = "attempt";

	public static final String FIELD_NAME_HOST = "host";

	public static final String FIELD_NAME_START_TIME = "start-time";

	public static final String FIELD_NAME_COMPATIBLE_START_TIME = "start_time";

	public static final String FIELD_NAME_END_TIME = "end-time";

	public static final String FIELD_NAME_DURATION = "duration";

	public static final String FIELD_NAME_METRICS = "metrics";

	public static final String FIELD_NAME_TASKMANAGER_ID = "taskmanager-id";

	@JsonProperty(FIELD_NAME_SUBTASK_INDEX)
	private final int subtaskIndex;

	@JsonProperty(FIELD_NAME_STATUS)
	private final ExecutionState status;

	@JsonProperty(FIELD_NAME_ATTEMPT)
	private final int attempt;

	@JsonProperty(FIELD_NAME_HOST)
	private final String host;

	@JsonProperty(FIELD_NAME_START_TIME)
	private final long startTime;

	@JsonProperty(FIELD_NAME_COMPATIBLE_START_TIME)
	private final long startTimeCompatible;

	@JsonProperty(FIELD_NAME_END_TIME)
	private final long endTime;

	@JsonProperty(FIELD_NAME_DURATION)
	private final long duration;

	@JsonProperty(FIELD_NAME_METRICS)
	private final SubTaskIOMetricsInfo subTaskIOMetricsInfo;

	@JsonProperty(FIELD_NAME_TASKMANAGER_ID)
	private final String taskmanagerId;

	@JsonCreator
	public SubtaskExecutionAttemptDetailsInfo(
			@JsonProperty(FIELD_NAME_SUBTASK_INDEX) int subtaskIndex,
			@JsonProperty(FIELD_NAME_STATUS) ExecutionState status,
			@JsonProperty(FIELD_NAME_ATTEMPT) int attempt,
			@JsonProperty(FIELD_NAME_HOST) String host,
			@JsonProperty(FIELD_NAME_START_TIME) long startTime,
			@JsonProperty(FIELD_NAME_END_TIME) long endTime,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_METRICS) SubTaskIOMetricsInfo subTaskIOMetricsInfo,
			@JsonProperty(FIELD_NAME_TASKMANAGER_ID) String taskmanagerId) {

		this.subtaskIndex = subtaskIndex;
		this.status = Preconditions.checkNotNull(status);
		this.attempt = attempt;
		this.host = Preconditions.checkNotNull(host);
		this.startTime = startTime;
		this.startTimeCompatible = startTime;
		this.endTime = endTime;
		this.duration = duration;
		this.subTaskIOMetricsInfo = Preconditions.checkNotNull(subTaskIOMetricsInfo);
		this.taskmanagerId = Preconditions.checkNotNull(taskmanagerId);
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public ExecutionState getStatus() {
		return status;
	}

	public int getAttempt() {
		return attempt;
	}

	public String getHost() {
		return host;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getStartTimeCompatible() {
		return startTimeCompatible;
	}

	public long getEndTime() {
		return endTime;
	}

	public long getDuration() {
		return duration;
	}

	public SubTaskIOMetricsInfo getSubTaskIOMetricsInfo() {
		return subTaskIOMetricsInfo;
	}

	public String getTaskmanagerId() {
		return taskmanagerId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SubtaskExecutionAttemptDetailsInfo that = (SubtaskExecutionAttemptDetailsInfo) o;

		return subtaskIndex == that.subtaskIndex &&
			status == that.status &&
			attempt == that.attempt &&
			Objects.equals(host, that.host) &&
			startTime == that.startTime &&
			startTimeCompatible == that.startTimeCompatible &&
			endTime == that.endTime &&
			duration == that.duration &&
			Objects.equals(subTaskIOMetricsInfo, that.subTaskIOMetricsInfo) &&
			Objects.equals(taskmanagerId, that.taskmanagerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskIndex, status, attempt, host, startTime, startTimeCompatible, endTime, duration, subTaskIOMetricsInfo, taskmanagerId);
	}

	public static SubtaskExecutionAttemptDetailsInfo create(AccessExecution execution, @Nullable MetricFetcher metricFetcher, JobID jobID, JobVertexID jobVertexID) {
		final ExecutionState status = execution.getState();
		final long now = System.currentTimeMillis();

		final TaskManagerLocation location = execution.getAssignedResourceLocation();
		final String locationString = location == null ? "(unassigned)" : location.getHostname();
		String taskmanagerId = location == null ? "(unassigned)" : location.getResourceID().toString();

		long startTime = execution.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		final long endTime = status.isTerminal() ? execution.getStateTimestamp(status) : -1;
		final long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		final MutableIOMetrics ioMetrics = new MutableIOMetrics();
		ioMetrics.addIOMetrics(
			execution,
			metricFetcher,
			jobID.toString(),
			jobVertexID.toString()
		);

		final SubTaskIOMetricsInfo subTaskIOMetricsInfo = new SubTaskIOMetricsInfo(
			ioMetrics.getNumBytesIn(),
			ioMetrics.isNumBytesInComplete(),
			ioMetrics.getNumBytesOut(),
			ioMetrics.isNumBytesOutComplete(),
			ioMetrics.getNumRecordsIn(),
			ioMetrics.isNumRecordsInComplete(),
			ioMetrics.getNumRecordsOut(),
			ioMetrics.isNumRecordsOutComplete(),
			ioMetrics.getUsageInputExclusiveBuffers(),
			ioMetrics.isUsageInputExclusiveBuffersComplete(),
			ioMetrics.getUsageInputFloatingBuffers(),
			ioMetrics.isUsageInputFloatingBuffersComplete(),
			ioMetrics.getUsageOutPool(),
			ioMetrics.isUsageOutPoolComplete(),
			ioMetrics.isBackPressured(),
			ioMetrics.isBackPressuredComplete()
		);

		return new SubtaskExecutionAttemptDetailsInfo(
			execution.getParallelSubtaskIndex(),
			status,
			execution.getAttemptNumber(),
			locationString,
			startTime,
			endTime,
			duration,
			subTaskIOMetricsInfo,
			taskmanagerId
		);
	}
}
