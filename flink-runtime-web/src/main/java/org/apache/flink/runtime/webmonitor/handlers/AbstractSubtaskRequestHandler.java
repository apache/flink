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

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.util.Map;

/**
 * Base class for request handlers whose response depends on a specific subtask (defined via the
 * "subtasknum" parameter) in a specific job vertex (defined via the "vertexid" parameter) in a
 * specific job, defined via (defined voa the "jobid" parameter).  
 */
public abstract class AbstractSubtaskRequestHandler extends AbstractJobVertexRequestHandler {
	
	public AbstractSubtaskRequestHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public final String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		final String subtaskNumberString = params.get("subtasknum");
		if (subtaskNumberString == null) {
			throw new RuntimeException("Subtask number parameter missing");
		}

		final int subtask;
		try {
			subtask = Integer.parseInt(subtaskNumberString);
		}
		catch (NumberFormatException e) {
			throw new RuntimeException("Invalid subtask number parameter");
		}
		
		if (subtask < 0 || subtask >= jobVertex.getParallelism()) {
			throw new RuntimeException("subtask does not exist: " + subtask); 
		}
		
		final ExecutionVertex vertex = jobVertex.getTaskVertices()[subtask];
		return handleRequest(vertex, params);
	}

	public abstract String handleRequest(ExecutionVertex vertex, Map<String, String> params) throws Exception;
}
