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
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.StringWriter;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Request handler that returns a summary of the job status.
 */
public class CurrentJobsOverviewHandler extends AbstractJsonRequestHandler {

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
	public String handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
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
						JsonUtils.writeJobDetailOverviewAsJson(detail, gen, now);
					}
					gen.writeEndArray();
	
					gen.writeArrayFieldStart("finished");
					for (JobDetails detail : result.getFinishedJobs()) {
						JsonUtils.writeJobDetailOverviewAsJson(detail, gen, now);
					}
					gen.writeEndArray();
				}
				else {
					gen.writeArrayFieldStart("jobs");
					for (JobDetails detail : includeRunningJobs ? result.getRunningJobs() : result.getFinishedJobs()) {
						JsonUtils.writeJobDetailOverviewAsJson(detail, gen, now);
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
}
