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

import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.webmonitor.JsonFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;

/**
 * Request handler that returns a summary of the job status.
 */
public class JobsOverviewHandler implements RequestHandler, RequestHandler.JsonResponse {
	
	private final ActorGateway jobManager;
	
	private final FiniteDuration timeout;
	
	private final boolean includeRunningJobs;
	private final boolean includeFinishedJobs;

	
	public JobsOverviewHandler(ActorGateway jobManager, FiniteDuration timeout,
								boolean includeRunningJobs, boolean includeFinishedJobs) {
		this.jobManager = jobManager;
		this.timeout = timeout;
		this.includeRunningJobs = includeRunningJobs;
		this.includeFinishedJobs = includeFinishedJobs;
	}

	@Override
	public String handleRequest(Map<String, String> params) throws Exception {
		try {
			Future<Object> future = jobManager.ask(
					new RequestJobDetails(includeRunningJobs, includeFinishedJobs), timeout);
			
			MultipleJobsDetails result = (MultipleJobsDetails) Await.result(future, timeout);
			
			if (includeRunningJobs && includeFinishedJobs) {
				return JsonFactory.generateRunningAndFinishedJobDetailsJSON(
						result.getRunningJobs(), result.getFinishedJobs());
			}
			else {
				return JsonFactory.generateMultipleJobsDetailsJSON(
						includeRunningJobs ? result.getRunningJobs() : result.getFinishedJobs());
			}
		}
		catch (Exception e) {
			throw new Exception("Failed to fetch the status overview: " + e.getMessage(), e);
		}
	}
}
