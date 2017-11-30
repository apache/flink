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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns a summary of the job status.
 */
public class JobsOverviewHandler extends AbstractJsonRequestHandler {

	private static final String ALL_JOBS_REST_PATH = JobsOverviewHeaders.URL;

	private final Time timeout;

	public JobsOverviewHandler(
			Executor executor,
			Time timeout) {

		super(executor);
		this.timeout = checkNotNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{ALL_JOBS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		if (jobManagerGateway != null) {
			CompletableFuture<MultipleJobsDetails> jobDetailsFuture = jobManagerGateway.requestMultipleJobDetails(timeout);

			return jobDetailsFuture.thenApplyAsync(
				(MultipleJobsDetails result) -> {

					StringWriter writer = new StringWriter();
					try {
						JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
						final JobDetails.JobDetailsSerializer jobDetailsSerializer = new JobDetails.JobDetailsSerializer();

						gen.writeStartObject();

						gen.writeArrayFieldStart(MultipleJobsDetails.FIELD_NAME_JOBS);
						for (JobDetails detail : result.getJobs()) {
							jobDetailsSerializer.serialize(detail, gen, null);
						}
						gen.writeEndArray();

						gen.writeEndObject();
						gen.close();
						return writer.toString();
					} catch (IOException e) {
						throw new CompletionException(new FlinkException("Could not write current jobs overview json.", e));
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
				gen.writeArrayFieldStart(MultipleJobsDetails.FIELD_NAME_JOBS);

				final JobDetails.JobDetailsSerializer jobDetailsSerializer = new JobDetails.JobDetailsSerializer();
				jobDetailsSerializer.serialize(WebMonitorUtils.createDetailsForJob(graph), gen, null);

				gen.writeEndArray();
				gen.writeEndObject();
			}
			String json = writer.toString();
			String path = ALL_JOBS_REST_PATH;
			return Collections.singleton(new ArchivedJson(path, json));
		}
	}
}
