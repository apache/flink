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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A request handler that provides the details of a job vertex, including id, name, and the
 * runtime and metrics of all its subtasks aggregated by TaskManager.
 */
public class JobVertexTaskManagersHandler extends AbstractJobVertexRequestHandler {

	public JobVertexTaskManagersHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		// Build a map that groups tasks by TaskManager
		Map<String, List<ExecutionVertex>> taskManagerVertices = new HashMap<>();

		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
			String taskManager = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			List<ExecutionVertex> vertices = taskManagerVertices.get(taskManager);

			if (vertices == null) {
				vertices = new ArrayList<ExecutionVertex>();
				taskManagerVertices.put(taskManager, vertices);
			}

			vertices.add(vertex);
		}

		// Build JSON response
		final long now = System.currentTimeMillis();

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getJobVertex().getName());
		gen.writeNumberField("now", now);

		gen.writeArrayFieldStart("taskmanagers");
		for (Entry<String, List<ExecutionVertex>> entry : taskManagerVertices.entrySet()) {
			String host = entry.getKey();
			List<ExecutionVertex> taskVertices = entry.getValue();

			int[] tasksPerState = new int[ExecutionState.values().length];

			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			LongCounter tmReadBytes = new LongCounter();
			LongCounter tmWriteBytes = new LongCounter();
			LongCounter tmReadRecords = new LongCounter();
			LongCounter tmWriteRecords = new LongCounter();

			for (ExecutionVertex vertex : taskVertices) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}

				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));

				Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> metrics = vertex.getCurrentExecutionAttempt().getFlinkAccumulators();

				if (metrics != null) {
					LongCounter readBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_IN);
					tmReadBytes.merge(readBytes);

					LongCounter writeBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_OUT);
					tmWriteBytes.merge(writeBytes);

					LongCounter readRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_IN);
					tmReadRecords.merge(readRecords);

					LongCounter writeRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_OUT);
					tmWriteRecords.merge(writeRecords);
				}
			}

			long duration;
			if (startTime < Long.MAX_VALUE) {
				if (allFinished) {
					duration = endTime - startTime;
				}
				else {
					endTime = -1L;
					duration = now - startTime;
				}
			}
			else {
				startTime = -1L;
				endTime = -1L;
				duration = -1L;
			}

			ExecutionState jobVertexState =
				ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, taskVertices.size());

			gen.writeStartObject();

			gen.writeStringField("host", host);
			gen.writeStringField("status", jobVertexState.name());

			gen.writeNumberField("start-time", startTime);
			gen.writeNumberField("end-time", endTime);
			gen.writeNumberField("duration", duration);

			gen.writeObjectFieldStart("metrics");
			gen.writeNumberField("read-bytes", tmReadBytes.getLocalValuePrimitive());
			gen.writeNumberField("write-bytes", tmWriteBytes.getLocalValuePrimitive());
			gen.writeNumberField("read-records", tmReadRecords.getLocalValuePrimitive());
			gen.writeNumberField("write-records", tmWriteRecords.getLocalValuePrimitive());
			gen.writeEndObject();

			gen.writeObjectFieldStart("status-counts");
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), tasksPerState[state.ordinal()]);
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
