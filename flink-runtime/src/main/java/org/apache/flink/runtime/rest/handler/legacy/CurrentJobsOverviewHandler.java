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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns a summary of the job status.
 */
public class CurrentJobsOverviewHandler extends AbstractJsonRequestHandler {

	private static final String ALL_JOBS_REST_PATH = "/joboverview";
	private static final String RUNNING_JOBS_REST_PATH = "/joboverview/running";
	private static final String COMPLETED_JOBS_REST_PATH = "/joboverview/completed";

	private final Time timeout;

	private final boolean includeRunningJobs;
	private final boolean includeFinishedJobs;

	public CurrentJobsOverviewHandler(
			Executor executor,
			Time timeout,
			boolean includeRunningJobs,
			boolean includeFinishedJobs) {

		super(executor);
		this.timeout = checkNotNull(timeout);
		this.includeRunningJobs = includeRunningJobs;
		this.includeFinishedJobs = includeFinishedJobs;
	}

	@Override
	public String[] getPaths() {
		if (includeRunningJobs && includeFinishedJobs) {
			return new String[]{ALL_JOBS_REST_PATH};
		}
		if (includeRunningJobs) {
			return new String[]{RUNNING_JOBS_REST_PATH};
		} else {
			return new String[]{COMPLETED_JOBS_REST_PATH};
		}
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		if (jobManagerGateway != null) {
			CompletableFuture<MultipleJobsDetails> jobDetailsFuture = jobManagerGateway.requestJobDetails(includeRunningJobs, includeFinishedJobs, timeout);

			return jobDetailsFuture.thenApplyAsync(
				(MultipleJobsDetails result) -> {
					final long now = System.currentTimeMillis();

					StringWriter writer = new StringWriter();
					try {
						JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
						gen.writeStartObject();

						if (includeRunningJobs && includeFinishedJobs) {
							gen.writeArrayFieldStart("running");
							for (JobDetails detail : result.getRunningJobs()) {
								writeJobDetailOverviewAsJson(detail, gen, now);
							}
							gen.writeEndArray();

							gen.writeArrayFieldStart("finished");
							for (JobDetails detail : result.getFinishedJobs()) {
								writeJobDetailOverviewAsJson(detail, gen, now);
							}
							gen.writeEndArray();
						} else {
							gen.writeArrayFieldStart("jobs");
							for (JobDetails detail : includeRunningJobs ? result.getRunningJobs() : result.getFinishedJobs()) {
								writeJobDetailOverviewAsJson(detail, gen, now);
							}
							gen.writeEndArray();
						}

						gen.writeEndObject();
						gen.close();
						return writer.toString();
					} catch (IOException e) {
						throw new FlinkFutureException("Could not write current jobs overview json.", e);
					}
				},
				executor);
		}
		else {
			return FutureUtils.completedExceptionally(new Exception("No connection to the leading JobManager."));
		}
	}

	/**
	 * Archivist for the CurrentJobsOverviewHandler.
	 */
	public static class CurrentJobsOverviewJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			StringWriter writer = new StringWriter();
			try (JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer)) {
				gen.writeStartObject();
				gen.writeArrayFieldStart("running");
				gen.writeEndArray();
				gen.writeArrayFieldStart("finished");
				writeJobDetailOverviewAsJson(WebMonitorUtils.createDetailsForJob(graph), gen, System.currentTimeMillis());
				gen.writeEndArray();
				gen.writeEndObject();
			}
			String json = writer.toString();
			String path = ALL_JOBS_REST_PATH;
			return Collections.singleton(new ArchivedJson(path, json));
		}
	}

	public static void writeJobDetailOverviewAsJson(JobDetails details, JsonGenerator gen, long now) throws IOException {
		gen.writeStartObject();

		gen.writeStringField("jid", details.getJobId().toString());
		gen.writeStringField("name", details.getJobName());
		gen.writeStringField("state", details.getStatus().name());

		gen.writeNumberField("start-time", details.getStartTime());
		gen.writeNumberField("end-time", details.getEndTime());
		gen.writeNumberField("duration", (details.getEndTime() <= 0 ? now : details.getEndTime()) - details.getStartTime());
		gen.writeNumberField("last-modification", details.getLastUpdateTime());

		gen.writeObjectFieldStart("tasks");
		gen.writeNumberField("total", details.getNumTasks());

		final int[] perState = details.getNumVerticesPerExecutionState();
		gen.writeNumberField("pending", perState[ExecutionState.CREATED.ordinal()] +
				perState[ExecutionState.SCHEDULED.ordinal()] +
				perState[ExecutionState.DEPLOYING.ordinal()]);
		gen.writeNumberField("running", perState[ExecutionState.RUNNING.ordinal()]);
		gen.writeNumberField("finished", perState[ExecutionState.FINISHED.ordinal()]);
		gen.writeNumberField("canceling", perState[ExecutionState.CANCELING.ordinal()]);
		gen.writeNumberField("canceled", perState[ExecutionState.CANCELED.ordinal()]);
		gen.writeNumberField("failed", perState[ExecutionState.FAILED.ordinal()]);
		gen.writeEndObject();

		gen.writeEndObject();
	}
}
