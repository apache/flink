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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An overview of how many jobs are in which status.
 */
public class JobIdsWithStatusOverview implements ResponseBody, InfoMessage {

	private static final long serialVersionUID = -3699051943490133183L;

	public static final String FIELD_NAME_JOBS = "jobs";

	@JsonProperty(FIELD_NAME_JOBS)
	private final Collection<JobIdWithStatus> jobsWithStatus;

	@JsonCreator
	public JobIdsWithStatusOverview(
			@JsonProperty(FIELD_NAME_JOBS) Collection<JobIdWithStatus> jobsWithStatus) {
		this.jobsWithStatus = checkNotNull(jobsWithStatus);
	}

	public JobIdsWithStatusOverview(JobIdsWithStatusOverview first, JobIdsWithStatusOverview second) {
		this.jobsWithStatus = combine(first.getJobsWithStatus(), second.getJobsWithStatus());
	}

	public Collection<JobIdWithStatus> getJobsWithStatus() {
		return jobsWithStatus;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return jobsWithStatus.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj instanceof JobIdsWithStatusOverview) {
			JobIdsWithStatusOverview that = (JobIdsWithStatusOverview) obj;
			return jobsWithStatus.equals(that.getJobsWithStatus());
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "JobIdsWithStatusesOverview { " + jobsWithStatus + " }";
	}

	// ------------------------------------------------------------------------

	private static Collection<JobIdWithStatus> combine(
			Collection<JobIdWithStatus> first,
			Collection<JobIdWithStatus> second) {
		checkNotNull(first);
		checkNotNull(second);

		ArrayList<JobIdWithStatus> result = new ArrayList<>(first.size() + second.size());

		result.addAll(first);
		result.addAll(second);

		return result;
	}

	// -------------------------------------------------------------------------
	// Static classes
	// -------------------------------------------------------------------------

	public static final class JobIdWithStatus implements Serializable {

		private static final long serialVersionUID = -499449819268733026L;

		public static final String FIELD_NAME_JOB_ID = "id";

		public static final String FIELD_NAME_JOB_STATUS = "status";

		@JsonProperty(FIELD_NAME_JOB_ID)
		@JsonSerialize(using = JobIDSerializer.class)
		private final JobID jobId;

		@JsonProperty(FIELD_NAME_JOB_STATUS)
		private final JobStatus jobStatus;

		@JsonCreator
		public JobIdWithStatus(
			@JsonProperty(FIELD_NAME_JOB_ID) @JsonDeserialize(using = JobIDDeserializer.class) JobID jobId,
			@JsonProperty(FIELD_NAME_JOB_STATUS) JobStatus jobStatus) {
			this.jobId = Preconditions.checkNotNull(jobId);
			this.jobStatus = Preconditions.checkNotNull(jobStatus);
		}

		public JobID getJobId() {
			return jobId;
		}

		public JobStatus getJobStatus() {
			return jobStatus;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobIdWithStatus that = (JobIdWithStatus) o;
			return Objects.equals(jobId, that.jobId) &&
				jobStatus == that.jobStatus;
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobId, jobStatus);
		}
	}
}
