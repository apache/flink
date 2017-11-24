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

import org.apache.flink.runtime.rest.handler.job.JobVertexBackPressureHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Response type of the {@link JobVertexBackPressureHandler}.
 */
public class JobVertexBackPressureInfo implements ResponseBody {

	public static final String FIELD_NAME_STATUS = "status";
	public static final String FIELD_NAME_BACKPRESSURE_LEVEL = "backpressure-level";
	public static final String FIELD_NAME_END_TIMESTAMP = "end-timestamp";
	public static final String FIELD_NAME_SUBTASKS = "subtasks";

	@JsonProperty(FIELD_NAME_STATUS)
	private final VertexBackPressureStatus status;

	@JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL)
	private final VertexBackPressureLevel backpressureLevel;

	@JsonProperty(FIELD_NAME_END_TIMESTAMP)
	private final long endTimestamp;

	@JsonProperty(FIELD_NAME_SUBTASKS)
	protected final List<SubtaskBackPressureInfo> subtasks;

	@JsonCreator
	public JobVertexBackPressureInfo(
		@JsonProperty(FIELD_NAME_STATUS) VertexBackPressureStatus status,
		@JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL) VertexBackPressureLevel backpressureLevel,
		@JsonProperty(FIELD_NAME_END_TIMESTAMP) long endTimestamp,
		@JsonProperty(FIELD_NAME_SUBTASKS) List<SubtaskBackPressureInfo> subtasks) {
		this.status = status;
		this.backpressureLevel = backpressureLevel;
		this.endTimestamp = endTimestamp;
		this.subtasks = checkNotNull(subtasks);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobVertexBackPressureInfo that = (JobVertexBackPressureInfo) o;
		return Objects.equals(status, that.status) &&
			Objects.equals(backpressureLevel, that.backpressureLevel) &&
			Objects.equals(endTimestamp, that.endTimestamp) &&
			Objects.equals(subtasks, that.subtasks);
	}

	@Override
	public int hashCode() {
		return Objects.hash(status, backpressureLevel, endTimestamp, subtasks);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Nested class to encapsulate the sub tasks back pressure.
	 */
	public static final class SubtaskBackPressureInfo {

		public static final String FIELD_NAME_SUBTASK = "subtask";
		public static final String FIELD_NAME_BACKPRESSURE_LEVEL = "backpressure-level";
		public static final String FIELD_NAME_RATIO = "ratio";

		@JsonProperty(FIELD_NAME_SUBTASK)
		private final int subtask;

		@JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL)
		private final VertexBackPressureLevel backpressureLevel;

		@JsonProperty(FIELD_NAME_RATIO)
		private final double ratio;

		public SubtaskBackPressureInfo(
			@JsonProperty(FIELD_NAME_SUBTASK) int subtask,
			@JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL) VertexBackPressureLevel backpressureLevel,
			@JsonProperty(FIELD_NAME_RATIO) double ratio) {
			this.subtask = subtask;
			this.backpressureLevel = checkNotNull(backpressureLevel);
			this.ratio = ratio;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SubtaskBackPressureInfo that = (SubtaskBackPressureInfo) o;
			return subtask == that.subtask &&
				ratio == that.ratio &&
				Objects.equals(backpressureLevel, that.backpressureLevel);
		}

		@Override
		public int hashCode() {
			return Objects.hash(subtask, backpressureLevel, ratio);
		}
	}

	/**
	 * Status of vertex back-pressure.
	 */
	public enum VertexBackPressureStatus {
		DEPRECATED("deprecated"), OK("ok");

		private String status;

		VertexBackPressureStatus(String status) {
			this.status = status;
		}

		@Override
		public String toString() {
			return status;
		}
	}

	/**
	 * Level of vertex back-pressure.
	 */
	public enum VertexBackPressureLevel {
		OK("ok"), LOW("low"), HIGH("high");

		private String level;

		VertexBackPressureLevel(String level) {
			this.level = level;
		}

		@Override
		public String toString() {
			return level;
		}
	}
}
