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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Request handler that returns a summary of the job status.
 */
public class CurrentJobsOverviewHandler implements RequestHandler {

	private final FiniteDuration timeout;
	
	private final boolean includeRunningJobs;
	private final boolean includeFinishedJobs;

	
	public CurrentJobsOverviewHandler(
			FiniteDuration timeout,
			boolean includeRunningJobs,
			boolean includeFinishedJobs) {

		this.timeout = checkNotNull(timeout);
		this.includeRunningJobs = includeRunningJobs;
		this.includeFinishedJobs = includeFinishedJobs;
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			if (jobManager != null) {
				Future<Object> future = jobManager.ask(
						new RequestJobDetails(includeRunningJobs, includeFinishedJobs), timeout);
				
				MultipleJobsDetails result = (MultipleJobsDetails) Await.result(future, timeout);
	
				final long now = System.currentTimeMillis();
	
				StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
				gen.writeStartObject();
				
				
				if (includeRunningJobs && includeFinishedJobs) {
					gen.writeArrayFieldStart("running");
					for (JobDetails detail : result.getRunningJobs()) {
						generateSingleJobDetails(detail, gen, now);
					}
					gen.writeEndArray();
	
					gen.writeArrayFieldStart("finished");
					for (JobDetails detail : result.getFinishedJobs()) {
						generateSingleJobDetails(detail, gen, now);
					}
					gen.writeEndArray();
				}
				else {
					gen.writeArrayFieldStart("jobs");
					for (JobDetails detail : includeRunningJobs ? result.getRunningJobs() : result.getFinishedJobs()) {
						generateSingleJobDetails(detail, gen, now);
					}
					gen.writeEndArray();
				}
	
				gen.writeEndObject();
				gen.close();
				return writer.toString();
			}
			else {
				throw new Exception("No connection to the leading JobManager.");
			}
		}
		catch (Exception e) {
			throw new Exception("Failed to fetch the status overview: " + e.getMessage(), e);
		}
	}

	private static void generateSingleJobDetails(JobDetails details, JsonGenerator gen, long now) throws Exception {
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
