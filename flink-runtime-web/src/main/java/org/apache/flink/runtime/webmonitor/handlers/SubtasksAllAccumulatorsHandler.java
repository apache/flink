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

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the accumulators for all subtasks of job vertex.
 */
public class SubtasksAllAccumulatorsHandler extends AbstractJobVertexRequestHandler {

	private static final String SUBTASKS_ALL_ACCUMULATORS_REST_PATH = 	"/jobs/:jobid/vertices/:vertexid/subtasks/accumulators";
	
	public SubtasksAllAccumulatorsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String[] getPaths() {
		return new String[]{SUBTASKS_ALL_ACCUMULATORS_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		return createSubtasksAccumulatorsJson(jobVertex);
	}

	public static String createSubtasksAccumulatorsJson(AccessExecutionJobVertex jobVertex) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeNumberField("parallelism", jobVertex.getParallelism());

		gen.writeArrayFieldStart("subtasks");
		
		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();
			
			gen.writeStartObject();
			
			gen.writeNumberField("subtask", num++);
			gen.writeNumberField("attempt", vertex.getCurrentExecutionAttempt().getAttemptNumber());
			gen.writeStringField("host", locationString);

			StringifiedAccumulatorResult[] accs = vertex.getCurrentExecutionAttempt().getUserAccumulatorsStringified();
			gen.writeArrayFieldStart("user-accumulators");
			for (StringifiedAccumulatorResult acc : accs) {
				gen.writeStartObject();
				gen.writeStringField("name", acc.getName());
				gen.writeStringField("type", acc.getType());
				gen.writeStringField("value", acc.getValue());
				gen.writeEndObject();
			}
			gen.writeEndArray();
			
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
		gen.close();
		return writer.toString();
	}
}
