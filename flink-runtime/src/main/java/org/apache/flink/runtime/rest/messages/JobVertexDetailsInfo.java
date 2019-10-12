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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
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
	private final List<SubtaskExecutionAttemptDetailsInfo> subtasks;

	@JsonCreator
	public JobVertexDetailsInfo(
			@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID id,
			@JsonProperty(FIELD_NAME_VERTEX_NAME) String name,
			@JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
			@JsonProperty(FIELD_NAME_NOW) long now,
			@JsonProperty(FIELD_NAME_SUBTASKS) List<SubtaskExecutionAttemptDetailsInfo> subtasks) {
		this.id = checkNotNull(id);
		this.name = checkNotNull(name);
		this.parallelism = parallelism;
		this.now = now;
		this.subtasks = checkNotNull(subtasks);
	}

	@JsonIgnore
	public List<SubtaskExecutionAttemptDetailsInfo> getSubtasks() {
		return Collections.unmodifiableList(subtasks);
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
}
