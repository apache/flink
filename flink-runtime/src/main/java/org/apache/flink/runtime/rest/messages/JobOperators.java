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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rest.handler.job.JobOperatorsHandler;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexOperatorIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexOperatorIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Response type of the {@link JobOperatorsHandler}.
 */
public class JobOperators implements ResponseBody {

	private static final String FIELD_NAME_OPERATORS = "operators";

	@JsonProperty(FIELD_NAME_OPERATORS)
	private final Collection<JobOperator> jobOperators;

	@JsonCreator
	public JobOperators(
		@JsonProperty(FIELD_NAME_OPERATORS) Collection<JobOperator> jobOperators) {
		this.jobOperators = jobOperators;
	}

	@JsonIgnore
	public Collection<JobOperator> getJobOperators() {
		return jobOperators;
	}

	@Override
	public String toString() {
		return "JobOperators{" +
			"operators=" + jobOperators +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobOperators that = (JobOperators) o;
		return Objects.equals(jobOperators, that.jobOperators);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobOperators);
	}

	/**
	 * Detailed information about a job operator.
	 */
	public static final class JobOperator{

		public static final String FIELD_NAME_VERTEX_VERTEX_ID = "vertex_id";

		public static final String FIELD_NAME_VERTEX_OPERATOR_ID = "operator_id";

		public static final String FIELD_NAME_VERTEX_OPERATOR_NAME = "name";

		public static final String FIELD_NAME_VERTEX_OPERATOR_INPUTS = "inputs";

		@JsonProperty(FIELD_NAME_VERTEX_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID jobVertexID;

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_ID)
		@JsonSerialize(using = JobVertexOperatorIDSerializer.class)
		private final OperatorID jobVertexOperatorID;

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_NAME)
		private final String name;

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_INPUTS)
		private final Collection<OperatorEdgeInfo> inputs;

		@JsonCreator
		public JobOperator(
			@JsonDeserialize(using = JobVertexIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_VERTEX_ID) JobVertexID jobVertexID,
			@JsonDeserialize(using = JobVertexOperatorIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_ID) OperatorID jobVertexOperatorID,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_NAME) String name,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_INPUTS) Collection<OperatorEdgeInfo> inputs) {
			this.jobVertexOperatorID = Preconditions.checkNotNull(jobVertexOperatorID);
			this.name = Preconditions.checkNotNull(name);
			this.inputs = inputs;
			this.jobVertexID = jobVertexID;
		}

		@JsonIgnore
		public JobVertexID getJobVertexID() {
			return jobVertexID;
		}

		@JsonIgnore
		public OperatorID getJobVertexOperatorID() {
			return jobVertexOperatorID;
		}

		@JsonIgnore
		public String getName() {
			return name;
		}

		@JsonIgnore
		public Collection<OperatorEdgeInfo> getInputs() {
			return inputs;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobOperator that = (JobOperator) o;
			return Objects.equals(jobVertexID, that.jobVertexID) &&
				Objects.equals(jobVertexOperatorID, that.jobVertexOperatorID) &&
				Objects.equals(name, that.name) &&
				Objects.equals(inputs, that.inputs);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobVertexID, jobVertexOperatorID, name, inputs);
		}

		@Override
		public String toString() {
			return "JobOperator {" +
				"vertex_id=" + jobVertexID +
				", operator_id='" + jobVertexOperatorID + '\'' +
				", name=" + name +
				", inputs=" + Arrays.toString(inputs.toArray()) +
				'}';
		}
	}

	/**
	 * Detailed information about a job operator edge.
	 */
	public static final class OperatorEdgeInfo {

		public static final String FIELD_NAME_VERTEX_OPERATOR_EDG_TARGET_ID = "operator_id";

		public static final String FIELD_NAME_VERTEX_OPERATOR_EDG_PARTITIONER = "partitioner";

		public static final String FIELD_NAME_VERTEX_OPERATOR_EDG_TYPE_NUMBER = "type_number";

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_TARGET_ID)
		@JsonSerialize(using = JobVertexOperatorIDSerializer.class)
		private final OperatorID jobVertexOperatorID;

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_PARTITIONER)
		private final String partitioner;

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_TYPE_NUMBER)
		private final int typeNumber;

		@JsonCreator
		public OperatorEdgeInfo(
			@JsonDeserialize(using = JobVertexOperatorIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_TARGET_ID) OperatorID jobVertexOperatorID,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_PARTITIONER) String partitioner,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_EDG_TYPE_NUMBER) int typeNumber) {
			this.jobVertexOperatorID = Preconditions.checkNotNull(jobVertexOperatorID);
			this.partitioner = Preconditions.checkNotNull(partitioner);
			this.typeNumber = typeNumber;
		}

		@JsonIgnore
		public OperatorID getJobVertexOperatorID() {
			return jobVertexOperatorID;
		}

		@JsonIgnore
		public String getPartitioner() {
			return partitioner;
		}

		@JsonIgnore
		public int getTypeNumber() {
			return typeNumber;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			OperatorEdgeInfo that = (OperatorEdgeInfo) o;
			return Objects.equals(jobVertexOperatorID, that.jobVertexOperatorID) &&
				Objects.equals(partitioner, that.partitioner) &&
				Objects.equals(typeNumber, that.typeNumber);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobVertexOperatorID, partitioner, typeNumber);
		}
	}
}
