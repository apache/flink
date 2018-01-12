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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

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

	public static final String FIELD_NAME_END_TIME = "end-time";

	public static final String FIELD_NAME_DURATION = "duration";

	public static final String FIELD_NAME_METRICS = "metrics";

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

	@JsonProperty(FIELD_NAME_END_TIME)
	private final long endTime;

	@JsonProperty(FIELD_NAME_DURATION)
	private final long duration;

	@JsonProperty(FIELD_NAME_METRICS)
	private final IOMetricsInfo ioMetricsInfo;

	@JsonCreator
	public SubtaskExecutionAttemptDetailsInfo(
			@JsonProperty(FIELD_NAME_SUBTASK_INDEX) int subtaskIndex,
			@JsonProperty(FIELD_NAME_STATUS) ExecutionState status,
			@JsonProperty(FIELD_NAME_ATTEMPT) int attempt,
			@JsonProperty(FIELD_NAME_HOST) String host,
			@JsonProperty(FIELD_NAME_START_TIME) long startTime,
			@JsonProperty(FIELD_NAME_END_TIME) long endTime,
			@JsonProperty(FIELD_NAME_DURATION) long duration,
			@JsonProperty(FIELD_NAME_METRICS) IOMetricsInfo ioMetricsInfo) {

		this.subtaskIndex = subtaskIndex;
		this.status = Preconditions.checkNotNull(status);
		this.attempt = attempt;
		this.host = Preconditions.checkNotNull(host);
		this.startTime = startTime;
		this.endTime = endTime;
		this.duration = duration;
		this.ioMetricsInfo = Preconditions.checkNotNull(ioMetricsInfo);
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
			endTime == that.endTime &&
			duration == that.duration &&
			Objects.equals(ioMetricsInfo, that.ioMetricsInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskIndex, status, attempt, host, startTime, endTime, duration, ioMetricsInfo);
	}
}
