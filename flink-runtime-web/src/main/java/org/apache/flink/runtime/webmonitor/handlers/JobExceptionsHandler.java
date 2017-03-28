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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Request handler that returns the configuration of a job.
 */
public class JobExceptionsHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_EXCEPTIONS_REST_PATH = "/jobs/:jobid/exceptions";
	private static final String JOB_EXCEPTIONS_PRIOR_REST_PATH = "/jobs/:jobid/exceptions/:attempt";

	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;
	
	public JobExceptionsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_EXCEPTIONS_REST_PATH, JOB_EXCEPTIONS_PRIOR_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		String attemptString = params.get("attempt");
		if (attemptString == null) {
			return createJobExceptionsJson(graph, -1);
		} else {
			int attempt = Integer.valueOf(params.get("attempt"));
			return createJobExceptionsJson(graph, attempt);
		}
	}

	public static class JobExceptionsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createJobExceptionsJson(graph, -1);
			String path = JOB_EXCEPTIONS_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	public static String createJobExceptionsJson(AccessExecutionGraph graph, int attempt) throws IOException {
		EvictingBoundedList<ErrorInfo> priorFailureCauses = graph.getPriorFailureCauses();
		ErrorInfo rootError = attempt == -1
			? graph.getFailureCause()
			: graph.getPriorFailureCauses().get(attempt);

		if (rootError == null) {
			return "{}";
		}

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		
		// most important is the root failure cause
		ErrorInfo rootException = graph.getFailureCause();
		if (rootException != null && !rootException.getExceptionAsString().equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
			gen.writeStringField("root-exception", rootException.getExceptionAsString());
			gen.writeNumberField("timestamp", rootException.getTimestamp());

			TaskManagerLocation location = rootError.getLocation();
			if (location != null) {
				String locationString = location.getFQDNHostname() + ':' + location.dataPort();
				gen.writeStringField("location", locationString);
			}
			String task = rootError.getTaskName();
			if (task != null) {
				gen.writeStringField("task", task);
			}
		}

		if (attempt == -1) {
			gen.writeArrayFieldStart("availableExceptionIds");
			for (int x = Math.max(0, priorFailureCauses.size() - priorFailureCauses.getSizeLimit()); x < priorFailureCauses.size(); x++) {
				gen.writeNumber(priorFailureCauses.get(x).getAttemptNumber());
			}
			gen.writeEndArray();
		}

		// we additionally collect all exceptions (up to a limit) that occurred in the individual tasks
		gen.writeArrayFieldStart("all-exceptions");

		int numExceptionsSoFar = 0;
		boolean truncated = false;
		
		for (AccessExecutionVertex task : graph.getAllExecutionVertices()) {
			AccessExecution execution = attempt == -1
				? task.getCurrentExecutionAttempt()
				: task.getPriorExecutionAttempt(rootError.getAttemptNumber());

			String t = execution.getFailureCauseAsString();
			if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				if (numExceptionsSoFar >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				TaskManagerLocation location = execution.getAssignedResourceLocation();
				String locationString = location != null ?
						location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";

				gen.writeStartObject();
				gen.writeStringField("exception", t);
				gen.writeStringField("task", task.getTaskNameWithSubtaskIndex());
				gen.writeStringField("location", locationString);
				long timestamp = execution.getStateTimestamp(ExecutionState.FAILED);
				gen.writeNumberField("timestamp", timestamp == 0 ? -1 : timestamp);
				gen.writeEndObject();
				numExceptionsSoFar++;
			}
		}
		gen.writeEndArray();

		gen.writeBooleanField("truncated", truncated);
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
