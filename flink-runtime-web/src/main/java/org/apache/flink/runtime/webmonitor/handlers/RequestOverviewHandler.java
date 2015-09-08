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
import org.apache.flink.runtime.messages.webmonitor.RequestStatusWithJobIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.StatusWithJobIDsOverview;
import org.apache.flink.runtime.webmonitor.JobManagerArchiveRetriever;
import org.apache.flink.runtime.webmonitor.JsonFactory;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;

/**
 * Responder that returns the status of the Flink cluster, such as how many
 * TaskManagers are currently connected, and how many jobs are running.
 */
public class RequestOverviewHandler implements  RequestHandler, RequestHandler.JsonResponse {
	
	private final JobManagerArchiveRetriever retriever;
	
	private final FiniteDuration timeout;
	
	
	public RequestOverviewHandler(JobManagerArchiveRetriever retriever) {
		this(retriever, WebRuntimeMonitor.DEFAULT_REQUEST_TIMEOUT);
	}
	
	public RequestOverviewHandler(JobManagerArchiveRetriever retriever, FiniteDuration timeout) {
		if (retriever == null || timeout == null) {
			throw new NullPointerException();
		}
		this.retriever = retriever;
		this.timeout = timeout;
	}
	
	@Override
	public String handleRequest(Map<String, String> params) throws Exception {
		try {
			ActorGateway jobManager = retriever.getJobManagerGateway();

			if (jobManager != null) {
				Future<Object> future = jobManager.ask(RequestStatusWithJobIDsOverview.getInstance(), timeout);
				StatusWithJobIDsOverview result = (StatusWithJobIDsOverview) Await.result(future, timeout);
				return JsonFactory.generateOverviewWithJobIDsJSON(result);
			} else {
				throw new Exception("No connection to the leading job manager.");
			}
		}
		catch (Exception e) {
			throw new Exception("Failed to fetch the status overview: " + e.getMessage(), e);
		}
	}
}
