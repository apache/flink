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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the JSON program plan of a job graph.
 */
public class SubtasksTimesHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {

	
	public SubtasksTimesHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {
		String vidString = params.get("vertexid");
		if (vidString == null) {
			throw new IllegalArgumentException("vertexId parameter missing");
		}

		JobVertexID vid;
		try {
			vid = JobVertexID.fromHexString(vidString);
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Invalid JobVertexID string '" + vidString + "': " + e.getMessage());
		}

		ExecutionJobVertex jobVertex = graph.getJobVertex(vid);
		if (jobVertex == null) {
			throw new IllegalArgumentException("No vertex with ID '" + vidString + "' exists.");
		}


		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getJobVertex().getName());

		gen.writeArrayFieldStart("subtasks");

		int num = 0;
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			gen.writeStartObject();
			gen.writeNumberField("subtask", num);

			InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();
			gen.writeStringField("host", locationString);

			gen.writeObjectFieldStart("timestamps");
			long[] timestamps = vertex.getCurrentExecutionAttempt().getStateTimestamps();
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), timestamps[state.ordinal()]);
			}
			gen.writeEndObject();
			gen.writeEndObject();
		}

		gen.writeEndArray();
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
