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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for request handlers whose response depends on a specific job vertex (defined
 * via the "vertexid" parameter) in a specific job, defined via (defined via the "jobid" parameter).
 */
public abstract class AbstractJobVertexRequestHandler extends AbstractExecutionGraphRequestHandler {

	public AbstractJobVertexRequestHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public final CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		final JobVertexID vid = parseJobVertexId(params);

		final AccessExecutionJobVertex jobVertex = graph.getJobVertex(vid);
		if (jobVertex == null) {
			throw new IllegalArgumentException("No vertex with ID '" + vid + "' exists.");
		}

		return handleRequest(jobVertex, params);
	}

	/**
	 * Returns the job vertex ID parsed from the provided parameters.
	 *
	 * @param params Path parameters
	 * @return Parsed job vertex ID or <code>null</code> if not available.
	 */
	public static JobVertexID parseJobVertexId(Map<String, String> params) {
		String jobVertexIdParam = params.get("vertexid");
		if (jobVertexIdParam == null) {
			return null;
		}

		try {
			return JobVertexID.fromHexString(jobVertexIdParam);
		} catch (RuntimeException ignored) {
			return null;
		}
	}

	public abstract CompletableFuture<String> handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params);
}
