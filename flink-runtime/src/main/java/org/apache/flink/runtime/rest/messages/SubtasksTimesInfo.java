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
import org.apache.flink.runtime.rest.handler.job.SubtasksTimesHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Response type of the {@link SubtasksTimesHandler}.
 */
public class SubtasksTimesInfo implements ResponseBody {

	public static final String FIELD_NAME_ID = "id";
	public static final String FIELD_NAME_NAME = "name";
	public static final String FIELD_NAME_NOW = "now";
	public static final String FIELD_NAME_SUBTASKS = "subtasks";

	@JsonProperty(FIELD_NAME_ID)
	private final String id;

	@JsonProperty(FIELD_NAME_NAME)
	private final String name;

	@JsonProperty(FIELD_NAME_NOW)
	private final long now;

	@JsonProperty(FIELD_NAME_SUBTASKS)
	private final List<SubtaskTimeInfo> subtasks;

	@JsonCreator
	public SubtasksTimesInfo(
			@JsonProperty(FIELD_NAME_ID) String id,
			@JsonProperty(FIELD_NAME_NAME) String name,
			@JsonProperty(FIELD_NAME_NOW) long now,
			@JsonProperty(FIELD_NAME_SUBTASKS) List<SubtaskTimeInfo> subtasks) {
		this.id = checkNotNull(id);
		this.name = checkNotNull(name);
		this.now = now;
		this.subtasks = checkNotNull(subtasks);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || this.getClass() != o.getClass()) {
			return false;
		}

		SubtasksTimesInfo that = (SubtasksTimesInfo) o;
		return Objects.equals(id, that.id) &&
			Objects.equals(name, that.name) &&
			now == that.now &&
			Objects.equals(subtasks, that.subtasks);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name, now, subtasks);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Nested class to encapsulate the sub task times info.
	 */
	public static final class SubtaskTimeInfo {

		public static final String FIELD_NAME_SUBTASK = "subtask";
		public static final String FIELD_NAME_HOST = "host";
		public static final String FIELD_NAME_DURATION = "duration";
		public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

		@JsonProperty(FIELD_NAME_SUBTASK)
		private final int subtask;

		@JsonProperty(FIELD_NAME_HOST)
		private final String host;

		@JsonProperty(FIELD_NAME_DURATION)
		private final long duration;

		@JsonProperty(FIELD_NAME_TIMESTAMPS)
		private final Map<ExecutionState, Long> timestamps;

		public SubtaskTimeInfo(
				@JsonProperty(FIELD_NAME_SUBTASK) int subtask,
				@JsonProperty(FIELD_NAME_HOST) String host,
				@JsonProperty(FIELD_NAME_DURATION) long duration,
				@JsonProperty(FIELD_NAME_TIMESTAMPS) Map<ExecutionState, Long> timestamps) {
			this.subtask = subtask;
			this.host = checkNotNull(host);
			this.duration = duration;
			this.timestamps = checkNotNull(timestamps);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			SubtaskTimeInfo that = (SubtaskTimeInfo) o;
			return subtask == that.subtask &&
				Objects.equals(host, that.host) &&
				duration == that.duration &&
				Objects.equals(timestamps, that.timestamps);
		}

		@Override
		public int hashCode() {
			return Objects.hash(subtask, host, duration, timestamps);
		}
	}
}
