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
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Responder that returns with a list of all JobIDs of jobs found at the target actor.
 * May serve the IDs of current jobs, or past jobs, depending on whether this handler is
 * given the JobManager or Archive Actor Reference.
 */
public class CurrentJobIdsHandler extends AbstractJsonRequestHandler {

	private final Time timeout;

	public CurrentJobIdsHandler(Executor executor, Time timeout) {
		super(executor);
		this.timeout = requireNonNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JobIdsWithStatusesOverviewHeaders.CURRENT_JOB_IDS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			JobManagerGateway jobManagerGateway) {

		return CompletableFuture.supplyAsync(
			() -> {
				// we need no parameters, get all requests
				try {
					if (jobManagerGateway != null) {
						CompletableFuture<JobIdsWithStatusOverview> overviewFuture = jobManagerGateway.requestJobsOverview(timeout);
						JobIdsWithStatusOverview overview = overviewFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

						StringWriter writer = new StringWriter();
						JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
						gen.setCodec(RestMapperUtils.getStrictObjectMapper());

						gen.writeStartObject();
						gen.writeArrayFieldStart(JobIdsWithStatusOverview.FIELD_NAME_JOBS);

						for (JobIdsWithStatusOverview.JobIdWithStatus jobIdWithStatus : overview.getJobsWithStatus()) {
							gen.writeObject(jobIdWithStatus);
						}

						gen.writeEndArray();
						gen.writeEndObject();

						gen.close();

						return writer.toString();
					}
					else {
						throw new Exception("No connection to the leading JobManager.");
					}
				}
				catch (Exception e) {
					throw new CompletionException(new FlinkException("Failed to fetch list of all running jobs.", e));
				}
			},
			executor);
	}
}
