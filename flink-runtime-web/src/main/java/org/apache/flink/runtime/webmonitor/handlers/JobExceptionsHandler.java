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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
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

	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;
	
	public JobExceptionsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_EXCEPTIONS_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		return createJobExceptionsJson(graph);
	}

	public static class JobExceptionsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createJobExceptionsJson(graph);
			String path = JOB_EXCEPTIONS_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	public static String createJobExceptionsJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		
		// most important is the root failure cause
		String rootException = graph.getFailureCauseAsString();
		if (rootException != null && !rootException.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
			gen.writeStringField("root-exception", rootException);
		}

		// we additionally collect all exceptions (up to a limit) that occurred in the individual tasks
		gen.writeArrayFieldStart("all-exceptions");

		int numExceptionsSoFar = 0;
		boolean truncated = false;
		
		for (AccessExecutionVertex task : graph.getAllExecutionVertices()) {
			String t = task.getFailureCauseAsString();
			if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				if (numExceptionsSoFar >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				TaskManagerLocation location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
						location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";

				gen.writeStartObject();
				gen.writeStringField("exception", t);
				gen.writeStringField("task", task.getTaskNameWithSubtaskIndex());
				gen.writeStringField("location", locationString);
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
