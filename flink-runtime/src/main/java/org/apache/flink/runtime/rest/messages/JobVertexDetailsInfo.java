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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Response type of the {@link JobVertexDetailsHandler}.
 */
public class JobVertexDetailsInfo implements ResponseBody {
	public static final String FIELD_NAME_VERTEX_ID = "id";
	public static final String FIELD_NAME_VERTEX_NAME = "name";
	public static final String FIELD_NAME_PARALLELISM = "parallelism";
	public static final String FIELD_NAME_NOW = "now";
	public static final String FIELD_NAME_SUBTASKS = "subtasks";

	@JsonProperty(FIELD_NAME_VERTEX_ID)
	@JsonSerialize(using = JobVertexIDSerializer.class)
	private final JobVertexID id;

	@JsonProperty(FIELD_NAME_VERTEX_NAME)
	private final String name;

	@JsonProperty(FIELD_NAME_PARALLELISM)
	private final int parallelism;

	@JsonProperty(FIELD_NAME_NOW)
	private final long now;

	@JsonProperty(FIELD_NAME_SUBTASKS)
	private final List<VertexTaskDetail> subtasks;

	@JsonCreator
	public JobVertexDetailsInfo(
			@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID id,
			@JsonProperty(FIELD_NAME_VERTEX_NAME) String name,
			@JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
			@JsonProperty(FIELD_NAME_NOW) long now,
			@JsonProperty(FIELD_NAME_SUBTASKS) List<VertexTaskDetail> subtasks) {
		this.id = checkNotNull(id);
		this.name = checkNotNull(name);
		this.parallelism = parallelism;
		this.now = now;
		this.subtasks = checkNotNull(subtasks);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		JobVertexDetailsInfo that = (JobVertexDetailsInfo) o;
		return Objects.equals(id, that.id) &&
			Objects.equals(name, that.name) &&
			parallelism == that.parallelism &&
			now == that.now &&
			Objects.equals(subtasks, that.subtasks);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name, parallelism, now, subtasks);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Vertex task detail class.
	 */
	public static final class VertexTaskDetail {
		public static final String FIELD_NAME_SUBTASK = "subtask";
		public static final String FIELD_NAME_STATUS = "status";
		public static final String FIELD_NAME_ATTEMPT = "attempt";
		public static final String FIELD_NAME_HOST = "host";
		public static final String FIELD_NAME_START_TIME = "start-time";
		public static final String FIELD_NAME_COMPATIBLE_START_TIME = "start_time";
		public static final String FIELD_NAME_END_TIME = "end-time";
		public static final String FIELD_NAME_DURATION = "duration";
		public static final String FIELD_NAME_METRICS = "metrics";

		@JsonProperty(FIELD_NAME_SUBTASK)
		private final int subtask;

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
		private final IOMetricsInfo metrics;

		@JsonCreator
		public VertexTaskDetail(
				@JsonProperty(FIELD_NAME_SUBTASK) int subtask,
				@JsonProperty(FIELD_NAME_STATUS) ExecutionState status,
				@JsonProperty(FIELD_NAME_ATTEMPT) int attempt,
				@JsonProperty(FIELD_NAME_HOST) String host,
				@JsonProperty(FIELD_NAME_START_TIME) long startTime,
				@JsonProperty(FIELD_NAME_END_TIME) long endTime,
				@JsonProperty(FIELD_NAME_DURATION) long duration,
				@JsonProperty(FIELD_NAME_METRICS) IOMetricsInfo metrics) {
			this.subtask = subtask;
			this.status = checkNotNull(status);
			this.attempt = attempt;
			this.host = checkNotNull(host);
			this.startTime = startTime;
			this.startTimeCompatible = startTime;
			this.endTime = endTime;
			this.duration = duration;
			this.metrics = checkNotNull(metrics);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			VertexTaskDetail that = (VertexTaskDetail) o;
			return subtask == that.subtask &&
				Objects.equals(status, that.status) &&
				attempt == that.attempt &&
				Objects.equals(host, that.host) &&
				startTime == that.startTime &&
				startTimeCompatible == that.startTimeCompatible &&
				endTime == that.endTime &&
				duration == that.duration &&
				Objects.equals(metrics, that.metrics);
		}

		@Override
		public int hashCode() {
			return Objects.hash(subtask, status, attempt, host, startTime, startTimeCompatible, endTime, duration, metrics);
		}
	}
}
