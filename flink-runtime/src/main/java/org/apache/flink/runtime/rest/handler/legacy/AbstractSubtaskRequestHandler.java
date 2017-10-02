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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.util.FlinkException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for request handlers whose response depends on a specific subtask (defined via the
 * "subtasknum" parameter) in a specific job vertex (defined via the "vertexid" parameter) in a
 * specific job, defined via (defined voa the "jobid" parameter).
 */
public abstract class AbstractSubtaskRequestHandler extends AbstractJobVertexRequestHandler {

	public AbstractSubtaskRequestHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public final CompletableFuture<String> handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) {
		final String subtaskNumberString = params.get("subtasknum");
		if (subtaskNumberString == null) {
			return FutureUtils.completedExceptionally(new FlinkException("Subtask number parameter missing"));
		}

		final int subtask;
		try {
			subtask = Integer.parseInt(subtaskNumberString);
		}
		catch (NumberFormatException e) {
			return FutureUtils.completedExceptionally(new FlinkException("Invalid subtask number parameter", e));
		}

		if (subtask < 0 || subtask >= jobVertex.getParallelism()) {
			return FutureUtils.completedExceptionally(new FlinkException("subtask does not exist: " + subtask));
		}

		final AccessExecutionVertex vertex = jobVertex.getTaskVertices()[subtask];
		return handleRequest(vertex, params);
	}

	public abstract CompletableFuture<String> handleRequest(AccessExecutionVertex vertex, Map<String, String> params);
}
