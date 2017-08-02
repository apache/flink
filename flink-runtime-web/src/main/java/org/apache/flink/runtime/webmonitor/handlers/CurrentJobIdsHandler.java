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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobsWithIDsOverview;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Responder that returns with a list of all JobIDs of jobs found at the target actor.
 * May serve the IDs of current jobs, or past jobs, depending on whether this handler is
 * given the JobManager or Archive Actor Reference.
 */
public class CurrentJobIdsHandler extends AbstractJsonRequestHandler {

	private static final String CURRENT_JOB_IDS_REST_PATH = "/jobs";

	private final Time timeout;

	public CurrentJobIdsHandler(Time timeout) {
		this.timeout = requireNonNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{CURRENT_JOB_IDS_REST_PATH};
	}

	@Override
	public String handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) throws Exception {
		// we need no parameters, get all requests
		try {
			if (jobManagerGateway != null) {
				CompletableFuture<JobsWithIDsOverview> overviewFuture = jobManagerGateway.requestJobsOverview(timeout);
				JobsWithIDsOverview overview = overviewFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

				StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

				gen.writeStartObject();

				gen.writeArrayFieldStart("jobs-running");
				for (JobID jid : overview.getJobsRunningOrPending()) {
					gen.writeString(jid.toString());
				}
				gen.writeEndArray();

				gen.writeArrayFieldStart("jobs-finished");
				for (JobID jid : overview.getJobsFinished()) {
					gen.writeString(jid.toString());
				}
				gen.writeEndArray();

				gen.writeArrayFieldStart("jobs-cancelled");
				for (JobID jid : overview.getJobsCancelled()) {
					gen.writeString(jid.toString());
				}
				gen.writeEndArray();

				gen.writeArrayFieldStart("jobs-failed");
				for (JobID jid : overview.getJobsFailed()) {
					gen.writeString(jid.toString());
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
			throw new RuntimeException("Failed to fetch list of all running jobs: " + e.getMessage(), e);
		}
	}
}
