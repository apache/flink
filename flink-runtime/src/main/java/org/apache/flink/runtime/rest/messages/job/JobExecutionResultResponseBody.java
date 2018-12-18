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

import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobResultDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobResultSerializer;
import org.apache.flink.runtime.rest.messages.queue.AsynchronouslyCreatedResource;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * {@link ResponseBody} that carries a {@link QueueStatus} and a {@link JobResult}.
 *
 * @see org.apache.flink.runtime.rest.handler.job.JobExecutionResultHandler
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobExecutionResultResponseBody
		implements ResponseBody, AsynchronouslyCreatedResource<JobResult> {

	@JsonProperty(value = "status", required = true)
	private final QueueStatus status;

	@JsonProperty(value = "job-execution-result")
	@JsonSerialize(using = JobResultSerializer.class)
	@JsonDeserialize(using = JobResultDeserializer.class)
	@Nullable
	private final JobResult jobExecutionResult;

	@JsonCreator
	public JobExecutionResultResponseBody(
			@JsonProperty(value = "status", required = true) final QueueStatus status,
			@JsonProperty(value = "job-execution-result")
			@JsonDeserialize(using = JobResultDeserializer.class)
			@Nullable final JobResult jobExecutionResult) {
		this.status = requireNonNull(status);
		this.jobExecutionResult = jobExecutionResult;
	}

	public static JobExecutionResultResponseBody inProgress() {
		return new JobExecutionResultResponseBody(QueueStatus.inProgress(), null);
	}

	public static JobExecutionResultResponseBody created(
			final JobResult jobExecutionResult) {
		return new JobExecutionResultResponseBody(QueueStatus.completed(), jobExecutionResult);
	}

	public QueueStatus getStatus() {
		return status;
	}

	@Nullable
	public JobResult getJobExecutionResult() {
		return jobExecutionResult;
	}

	@Override
	public QueueStatus queueStatus() {
		return status;
	}

	@Override
	public JobResult resource() {
		return jobExecutionResult;
	}
}
