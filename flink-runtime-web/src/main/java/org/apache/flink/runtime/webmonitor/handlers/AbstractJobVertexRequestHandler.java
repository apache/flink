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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.util.Map;

/**
 * Base class for request handlers whose response depends on a specific job vertex (defined
 * via the "vertexid" parameter) in a specific job, defined via (defined voa the "jobid" parameter).  
 */
public abstract class AbstractJobVertexRequestHandler extends AbstractExecutionGraphRequestHandler {
	
	public AbstractJobVertexRequestHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public final String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {
		final String vidString = params.get("vertexid");
		if (vidString == null) {
			throw new IllegalArgumentException("vertexId parameter missing");
		}

		final JobVertexID vid;
		try {
			vid = JobVertexID.fromHexString(vidString);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Invalid JobVertexID string '" + vidString + "': " + e.getMessage());
		}

		final ExecutionJobVertex jobVertex = graph.getJobVertex(vid);
		if (jobVertex == null) {
			throw new IllegalArgumentException("No vertex with ID '" + vidString + "' exists.");
		}

		return handleRequest(jobVertex, params);
	}
	
	public abstract String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception;
}
