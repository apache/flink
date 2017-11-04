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

import org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Response type of the {@link JobAccumulatorsHandler}.
 */
public class JobAccumulatorsInfo implements ResponseBody {
	public static final String FIELD_NAME_JOB_ACCUMULATORS = "job-accumulators";
	public static final String FIELD_NAME_USER_TASK_ACCUMULATORS = "user-task-accumulators";

	@JsonProperty(FIELD_NAME_JOB_ACCUMULATORS)
	private List<JobAccumulator> jobAccumulators;

	@JsonProperty(FIELD_NAME_USER_TASK_ACCUMULATORS)
	private List<UserTaskAccumulator> userAccumulators;

	@JsonCreator
	public JobAccumulatorsInfo(
			@JsonProperty(FIELD_NAME_JOB_ACCUMULATORS) List<JobAccumulator> jobAccumulators,
			@JsonProperty(FIELD_NAME_USER_TASK_ACCUMULATORS) List<UserTaskAccumulator> userAccumulators) {
		this.jobAccumulators = Preconditions.checkNotNull(jobAccumulators);
		this.userAccumulators = Preconditions.checkNotNull(userAccumulators);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobAccumulatorsInfo that = (JobAccumulatorsInfo) o;
		return Objects.equals(userAccumulators, that.userAccumulators);
	}

	@Override
	public int hashCode() {
		return Objects.hash(userAccumulators);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Json serializer for the {@link JobAccumulatorsInfo}.
	 */
	public static final class JobAccumulator {
		// empty for now
	}

	/**
	 * Json serializer for the {@link JobAccumulatorsInfo}.
	 */
	public static final class UserTaskAccumulator {

		public static final String FIELD_NAME_ACC_NAME = "name";
		public static final String FIELD_NAME_ACC_TYPE = "type";
		public static final String FIELD_NAME_ACC_VALUE = "value";

		@JsonProperty(FIELD_NAME_ACC_NAME)
		private String name;

		@JsonProperty(FIELD_NAME_ACC_TYPE)
		private String type;

		@JsonProperty(FIELD_NAME_ACC_VALUE)
		private String value;

		@JsonCreator
		public UserTaskAccumulator(
			@JsonProperty(FIELD_NAME_ACC_NAME) String name,
			@JsonProperty(FIELD_NAME_ACC_TYPE) String type,
			@JsonProperty(FIELD_NAME_ACC_VALUE) String value) {
			this.name = Preconditions.checkNotNull(name);
			this.type = Preconditions.checkNotNull(type);
			this.value = Preconditions.checkNotNull(value);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			UserTaskAccumulator that = (UserTaskAccumulator) o;
			return Objects.equals(name, that.name) &&
				Objects.equals(type, that.type) &&
				Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, type, value);
		}
	}
}
