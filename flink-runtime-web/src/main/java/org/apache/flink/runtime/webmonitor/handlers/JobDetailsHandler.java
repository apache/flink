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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns the JSON program plan of a job graph.
 */
public class JobDetailsHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {
	
	public JobDetailsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {
		final StringWriter writer = new StringWriter();
		final JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

		final long now = System.currentTimeMillis();
		
		gen.writeStartObject();
		
		// basic info
		gen.writeStringField("jid", graph.getJobID().toString());
		gen.writeStringField("name", graph.getJobName());
		gen.writeStringField("state", graph.getState().name());
		
		// times and duration
		final long jobStartTime = graph.getStatusTimestamp(JobStatus.CREATED);
		final long jobEndTime = graph.getState().isTerminalState() ?
				graph.getStatusTimestamp(graph.getState()) : -1L;
		gen.writeNumberField("start-time", jobStartTime);
		gen.writeNumberField("end-time", jobEndTime);
		gen.writeNumberField("duration", (jobEndTime > 0 ? jobEndTime : now) - jobStartTime);
		
		// timestamps
		gen.writeObjectFieldStart("timestamps");
		for (JobStatus status : JobStatus.values()) {
			gen.writeNumberField(status.name(), graph.getStatusTimestamp(status));
		}
		gen.writeEndObject();
		
		final int[] tasksPerStatusTotal = new int[ExecutionState.values().length];
		
		// job vertices
		gen.writeArrayFieldStart("vertices");

		for (ExecutionJobVertex ejv : graph.getVerticesTopologically()) {
			int[] tasksPerState = new int[ExecutionState.values().length];
			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;
			
			for (ExecutionVertex vertex : ejv.getTaskVertices()) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;
				tasksPerStatusTotal[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}
				
				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));
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

			Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> metrics = ejv.getAggregatedMetricAccumulators();

			LongCounter readBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_IN);
			LongCounter writeBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_OUT);
			LongCounter readRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_IN);
			LongCounter writeRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_OUT);

			gen.writeStartObject();
			gen.writeStringField("id", ejv.getJobVertexId().toString());
			gen.writeStringField("name", ejv.getJobVertex().getName());
			gen.writeNumberField("parallelism", ejv.getParallelism());
			
			gen.writeNumberField("start-time", startTime);
			gen.writeNumberField("end-time", endTime);
			gen.writeNumberField("duration", duration);
			
			gen.writeObjectFieldStart("tasks");
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), tasksPerState[state.ordinal()]);
			}
			gen.writeEndObject();
			
			gen.writeObjectFieldStart("metrics");
			gen.writeNumberField("read-bytes", readBytes != null ? readBytes.getLocalValuePrimitive() : -1L);
			gen.writeNumberField("write-bytes", writeBytes != null ? writeBytes.getLocalValuePrimitive() : -1L);
			gen.writeNumberField("read-records", readRecords != null ? readRecords.getLocalValuePrimitive() : -1L);
			gen.writeNumberField("write-records",writeRecords != null ? writeRecords.getLocalValuePrimitive() : -1L);
			gen.writeEndObject();

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
