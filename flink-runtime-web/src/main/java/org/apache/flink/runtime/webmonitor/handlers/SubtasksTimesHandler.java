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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the state transition timestamps for all subtasks, plus their
 * location and duration.
 */
public class SubtasksTimesHandler extends AbstractJobVertexRequestHandler {

	
	public SubtasksTimesHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		final long now = System.currentTimeMillis();

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getJobVertex().getName());
		gen.writeNumberField("now", now);
		
		gen.writeArrayFieldStart("subtasks");

		int num = 0;
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			
			long[] timestamps = vertex.getCurrentExecutionAttempt().getStateTimestamps();
			ExecutionState status = vertex.getExecutionState();

			long scheduledTime = timestamps[ExecutionState.SCHEDULED.ordinal()];
			
			long start = scheduledTime > 0 ? scheduledTime : -1;
			long end = status.isTerminal() ? timestamps[status.ordinal()] : now;
			long duration = start >= 0 ? end - start : -1L;
			
			gen.writeStartObject();
			gen.writeNumberField("subtask", num++);

			InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();
			gen.writeStringField("host", locationString);

			gen.writeNumberField("duration", duration);
			
			gen.writeObjectFieldStart("timestamps");
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
