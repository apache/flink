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

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler that returns the JSON program plan of a job graph.
 */
public class JobPlanHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_PLAN_REST_PATH = "/jobs/:jobid/plan";

	public JobPlanHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_PLAN_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.completedFuture(graph.getJsonPlan());
	}

	/**
	 * Archivist for the JobPlanHandler.
	 */
	public static class JobPlanJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String path = JOB_PLAN_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			String json = graph.getJsonPlan();
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}
}
