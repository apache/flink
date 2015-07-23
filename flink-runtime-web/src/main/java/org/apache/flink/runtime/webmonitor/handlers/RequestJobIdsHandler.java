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

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;

import org.apache.flink.runtime.messages.webmonitor.JobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsWithIDsOverview;
import org.apache.flink.runtime.webmonitor.JsonFactory;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;

/**
 * Responder that returns with a list of all JobIDs of jobs found at the target actor.
 * May serve the IDs of current jobs, or past jobs, depending on whether this handler is
 * given the JobManager or Archive Actor Reference.
 */
public class RequestJobIdsHandler implements RequestHandler, RequestHandler.JsonResponse {
	
	private final ActorRef target;
	
	private final FiniteDuration timeout;
	
	public RequestJobIdsHandler(ActorRef target) {
		this(target, WebRuntimeMonitor.DEFAULT_REQUEST_TIMEOUT);
	}
	
	public RequestJobIdsHandler(ActorRef target, FiniteDuration timeout) {
		if (target == null || timeout == null) {
			throw new NullPointerException();
		}
		this.target = target;
		this.timeout = timeout;
	}
	
	@Override
	public String handleRequest(Map<String, String> params) throws Exception {
		// we need no parameters, get all requests
		try {
			Timeout to = new Timeout(timeout); 
			Future<Object> future = Patterns.ask(target, RequestJobsWithIDsOverview.getInstance(), to);
			JobsWithIDsOverview result = (JobsWithIDsOverview) Await.result(future, timeout);
			return JsonFactory.generateJobsOverviewJSON(result);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to fetch list of all running jobs: " + e.getMessage(), e);
		}
	}
}
