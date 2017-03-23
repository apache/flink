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
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the configuration of a job.
 */
public class JobExceptionsHistoryHandler extends AbstractExecutionGraphRequestHandler {
	private static final String JOB_EXCEPTIONS_PRIOR_REST_PATH = "/jobs/:jobid/exceptions/history";

	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;

	public JobExceptionsHistoryHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_EXCEPTIONS_PRIOR_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		return createPriorJobExceptionsJson(graph);
	}

	public static String createPriorJobExceptionsJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();

		gen.writeArrayFieldStart("all-root-exceptions");

		EvictingBoundedList<ErrorInfo> priorFailureCauses = graph.getPriorFailureCauses();
		// reverse order so that the oldest exception is at the bottom
		for (int x = priorFailureCauses.size() - 1; x >= Math.max(0, priorFailureCauses.size() - priorFailureCauses.getSizeLimit()); x--) {
			ErrorInfo exception = priorFailureCauses.get(x);
			gen.writeStartObject();

			gen.writeStringField("root-exception", ExceptionUtils.stringifyException(exception.getException()));
			gen.writeNumberField("timestamp", exception.getTimestamp());

			TaskManagerLocation location = exception.getLocation();
			if (location != null) {
				String locationString = location.getFQDNHostname() + ':' + location.dataPort();
				gen.writeStringField("location", locationString);
			}
			String task = exception.getTaskName();
			if (task != null) {
				gen.writeStringField("task", task);
			}

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
