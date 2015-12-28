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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.util.ExceptionUtils;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the configuration of a job.
 */
public class JobExceptionsHandler extends AbstractExecutionGraphRequestHandler {

	private static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;
	
	public JobExceptionsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		
		// most important is the root failure cause
		Throwable rootException = graph.getFailureCause();
		if (rootException != null) {
			gen.writeStringField("root-exception", ExceptionUtils.stringifyException(rootException));
		}

		// we additionally collect all exceptions (up to a limit) that occurred in the individual tasks
		gen.writeArrayFieldStart("all-exceptions");

		int numExceptionsSoFar = 0;
		boolean truncated = false;
		
		for (ExecutionVertex task : graph.getAllExecutionVertices()) {
			Throwable t = task.getFailureCause();
			if (t != null) {
				if (numExceptionsSoFar >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				InstanceConnectionInfo location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
						location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";

				gen.writeStartObject();
				gen.writeStringField("exception", ExceptionUtils.stringifyException(t));
				gen.writeStringField("task", task.getSimpleName());
				gen.writeStringField("location", locationString);
				gen.writeEndObject();
			}
		}
		gen.writeEndArray();

		gen.writeBooleanField("truncated", truncated);
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
