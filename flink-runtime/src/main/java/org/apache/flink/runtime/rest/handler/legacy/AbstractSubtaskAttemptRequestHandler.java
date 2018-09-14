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
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.util.FlinkException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for request handlers whose response depends on a specific subtask execution attempt
 * (defined via the "attempt" parameter) of a specific subtask (defined via the
 * "subtasknum" parameter) in a specific job vertex (defined via the "vertexid" parameter) in a
 * specific job, defined via (defined voa the "jobid" parameter).
 */
public abstract class AbstractSubtaskAttemptRequestHandler extends AbstractSubtaskRequestHandler {

	public AbstractSubtaskAttemptRequestHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionVertex vertex, Map<String, String> params) {
		final String attemptNumberString = params.get("attempt");
		if (attemptNumberString == null) {
			return FutureUtils.completedExceptionally(new FlinkException("Attempt number parameter missing"));
		}

		final int attempt;
		try {
			attempt = Integer.parseInt(attemptNumberString);
		}
		catch (NumberFormatException e) {
			return FutureUtils.completedExceptionally(new FlinkException("Invalid attempt number parameter"));
		}

		final AccessExecution currentAttempt = vertex.getCurrentExecutionAttempt();
		if (attempt == currentAttempt.getAttemptNumber()) {
			return handleRequest(currentAttempt, params);
		}
		else if (attempt >= 0 && attempt < currentAttempt.getAttemptNumber()) {
			AccessExecution exec = vertex.getPriorExecutionAttempt(attempt);

			if (exec != null) {
				return handleRequest(exec, params);
			} else {
				return FutureUtils.completedExceptionally(new RequestHandlerException("Execution for attempt " + attempt +
					" has already been deleted."));
			}
		}
		else {
			return FutureUtils.completedExceptionally(new FlinkException("Attempt does not exist: " + attempt));
		}
	}

	public abstract CompletableFuture<String> handleRequest(AccessExecution execAttempt, Map<String, String> params);
}
