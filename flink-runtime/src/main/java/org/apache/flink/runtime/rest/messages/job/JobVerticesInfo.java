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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Details of all Vertices about a job.
 */
public class JobVerticesInfo implements ResponseBody {

	public static final String FIELD_NAME_JOB_VERTICES = "vertices";


	public static final String FIELD_NAME_JOB_OPERATOR = "operators";

	@JsonProperty(FIELD_NAME_JOB_VERTICES)
	private final Collection<JobVertex> jobVertices;

	@JsonProperty(FIELD_NAME_JOB_OPERATOR)
	private final Collection<JobOperator> jobOperators;

	@JsonCreator
	public JobVerticesInfo(
			@JsonProperty(FIELD_NAME_JOB_VERTICES) Collection<JobVertex> jobVertices,
			@JsonProperty(FIELD_NAME_JOB_OPERATOR) Collection<JobOperator> jobOperators) {
		this.jobVertices = Preconditions.checkNotNull(jobVertices);
		this.jobOperators = Preconditions.checkNotNull(jobOperators);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobVerticesInfo that = (JobVerticesInfo) o;
		return Objects.equals(jobVertices, that.jobVertices) &&
			Objects.equals(jobOperators, that.jobOperators);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobVertices, jobOperators);
	}

	@JsonIgnore
	public Collection<JobVertex> getJobVertices() {
		return jobVertices;
	}

	@JsonIgnore
	public Collection<JobOperator> getJobOperators() {
		return jobOperators;
	}

	// ---------------------------------------------------
	// Static inner classes
	// ---------------------------------------------------

	/**
	 * Detailed information about a job vertex.
	 */
	public static final class JobVertex {

		public static final String FIELD_NAME_JOB_VERTEX_ID = "id";

		public static final String FIELD_NAME_JOB_VERTEX_NAME = "name";

		public static final String FIELD_NAME_JOB_VERTEX_PARALLELISM = "parallelism";

		public static final String FIELD_NAME_JOB_VERTEX_SUBTASK_METRICS = "subtask_metrics";

		public static final String FIELD_NAME_JOB_VERTEX_METRICS = "metrics";

		@JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID jobVertexID;

		@JsonProperty(FIELD_NAME_JOB_VERTEX_NAME)
		private final String name;

		@JsonProperty(FIELD_NAME_JOB_VERTEX_PARALLELISM)
		private final int parallelism;

		@JsonProperty(FIELD_NAME_JOB_VERTEX_SUBTASK_METRICS)
		private final Collection<Map<String, String>> jobVertexSubTaskMetrics;

		@JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS)
		private final IOMetricsInfo jobVertexMetrics;

		@JsonCreator
		public JobVertex(
				@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_VERTEX_ID) JobVertexID jobVertexID,
				@JsonProperty(FIELD_NAME_JOB_VERTEX_NAME) String name,
				@JsonProperty(FIELD_NAME_JOB_VERTEX_PARALLELISM) int parallelism,
				@JsonProperty(FIELD_NAME_JOB_VERTEX_SUBTASK_METRICS) Collection<Map<String, String>> jobVertexSubTaskMetrics,
				@JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS) IOMetricsInfo jobVertexMetrics) {
			this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
			this.name = Preconditions.checkNotNull(name);
			this.parallelism = parallelism;
			this.jobVertexSubTaskMetrics = Preconditions.checkNotNull(jobVertexSubTaskMetrics);
			this.jobVertexMetrics = Preconditions.checkNotNull(jobVertexMetrics);
		}

		@JsonIgnore
		public JobVertexID getJobVertexID() {
			return jobVertexID;
		}

		@JsonIgnore
		public String getName() {
			return name;
		}

		@JsonIgnore
		public int getParallelism() {
			return parallelism;
		}

		@JsonIgnore
		public Collection<Map<String, String>> getJobVertexSubTaskMetrics() {
			return jobVertexSubTaskMetrics;
		}

		@JsonIgnore
		public IOMetricsInfo getJobVertexMetrics() {
			return jobVertexMetrics;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobVertex that = (JobVertex) o;
			return Objects.equals(jobVertexID, that.jobVertexID) &&
				Objects.equals(name, that.name) &&
				parallelism == that.parallelism &&
				Objects.equals(jobVertexSubTaskMetrics, that.jobVertexSubTaskMetrics) &&
				Objects.equals(jobVertexMetrics, that.jobVertexMetrics);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobVertexID, name, parallelism, jobVertexSubTaskMetrics, jobVertexMetrics);
		}
	}

	/**
	 * Detailed information about a job operator.
	 */
	public static final class JobOperator{

		public static final String FIELD_NAME_VERTEX_VERTEX_ID = "vertex_id";

		public static final String FIELD_NAME_VERTEX_OPERATOR_ID = "operator_id";

		public static final String FIELD_NAME_VERTEX_OPERATOR_NAME = "name";

		public static final String FIELD_NAME_VERTEX_OPERATOR_INPUTS = "inputs";

		public static final String FIELD_NAME_VERTEX_OPERATOR_METRIC_NAME = "metric_name";

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

		@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_METRIC_NAME)
		private final String metricName;

		@JsonCreator
		public JobOperator(
			@JsonDeserialize(using = JobVertexIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_VERTEX_ID) JobVertexID jobVertexID,
			@JsonDeserialize(using = JobVertexOperatorIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_ID) OperatorID jobVertexOperatorID,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_NAME) String name,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_INPUTS) Collection<OperatorEdgeInfo> inputs,
			@JsonProperty(FIELD_NAME_VERTEX_OPERATOR_METRIC_NAME) String metricName) {
			this.jobVertexOperatorID = Preconditions.checkNotNull(jobVertexOperatorID);
			this.name = Preconditions.checkNotNull(name);
			this.inputs = inputs;
			this.metricName = metricName;
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

		@JsonIgnore
		public String getMetricName() {
			return metricName;
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
					Objects.equals(inputs, that.inputs) &&
					Objects.equals(metricName, that.metricName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobVertexID, jobVertexOperatorID, name, inputs, metricName);
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
