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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.webmonitor.JobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsWithIDsOverview;

import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.StringWriter;
import java.util.Map;

/**
 * Responder that returns with a list of all JobIDs of jobs found at the target actor.
 * May serve the IDs of current jobs, or past jobs, depending on whether this handler is
 * given the JobManager or Archive Actor Reference.
 */
public class CurrentJobIdsHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final JobManagerRetriever retriever;

	private final FiniteDuration timeout;


	public CurrentJobIdsHandler(JobManagerRetriever retriever, FiniteDuration timeout) {
		if (retriever == null || timeout == null) {
			throw new NullPointerException();
		}
		this.retriever = retriever;
		this.timeout = timeout;
	}
	
	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		// we need no parameters, get all requests
		try {
			if (jobManager != null) {
				Future<Object> future = jobManager.ask(RequestJobsWithIDsOverview.getInstance(), timeout);
				JobsWithIDsOverview overview = (JobsWithIDsOverview) Await.result(future, timeout);
	
				StringWriter writer = new StringWriter();
				JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);
	
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
