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

import org.apache.flink.runtime.rest.handler.job.JobVertexAccumulatorsHandler;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * Response type of the {@link JobVertexAccumulatorsHandler}.
 */
public class JobVertexAccumulatorsInfo implements ResponseBody {

	public static final String FIELD_NAME_ID = "id";
	public static final String FIELD_NAME_USER_ACCUMULATORS = "user-accumulators";

	@JsonProperty(FIELD_NAME_ID)
	private String id;

	@JsonProperty(FIELD_NAME_USER_ACCUMULATORS)
	private Collection<UserAccumulator> userAccumulatorList;

	@JsonCreator
	public JobVertexAccumulatorsInfo(
			@JsonProperty(FIELD_NAME_ID) String id,
			@JsonProperty(FIELD_NAME_USER_ACCUMULATORS) Collection<UserAccumulator> userAccumulatorList) {
		this.id = Preconditions.checkNotNull(id);
		this.userAccumulatorList = Preconditions.checkNotNull(userAccumulatorList);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobVertexAccumulatorsInfo that = (JobVertexAccumulatorsInfo) o;
		return Objects.equals(id, that.id) &&
			Objects.equals(userAccumulatorList, that.userAccumulatorList);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, userAccumulatorList);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Json serializer for the {@link JobVertexAccumulatorsInfo}.
	 */
	public static final class UserAccumulator {

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
		public UserAccumulator(
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
			UserAccumulator that = (UserAccumulator) o;
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
