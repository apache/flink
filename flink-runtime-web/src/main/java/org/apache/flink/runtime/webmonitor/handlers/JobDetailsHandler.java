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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns details about a job, including:
 * <ul>
 *     <li>Dataflow plan</li>
 *     <li>id, name, and current status</li>
 *     <li>start time, end time, duration</li>
 *     <li>number of job vertices in each state (pending, running, finished, failed)</li>
 *     <li>info about job vertices, including runtime, status, I/O bytes and records, subtasks in each status</li>
 * </ul>
 */
public class JobDetailsHandler extends AbstractExecutionGraphRequestHandler {

	private final MetricFetcher fetcher;

	public JobDetailsHandler(ExecutionGraphHolder executionGraphHolder, MetricFetcher fetcher) {
		super(executionGraphHolder);
		this.fetcher = fetcher;
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		final StringWriter writer = new StringWriter();
		final JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		final long now = System.currentTimeMillis();
		
		gen.writeStartObject();
		
		// basic info
		gen.writeStringField("jid", graph.getJobID().toString());
		gen.writeStringField("name", graph.getJobName());
		gen.writeBooleanField("isStoppable", graph.isStoppable());
		gen.writeStringField("state", graph.getState().name());
		
		// times and duration
		final long jobStartTime = graph.getStatusTimestamp(JobStatus.CREATED);
		final long jobEndTime = graph.getState().isGloballyTerminalState() ?
				graph.getStatusTimestamp(graph.getState()) : -1L;
		gen.writeNumberField("start-time", jobStartTime);
		gen.writeNumberField("end-time", jobEndTime);
		gen.writeNumberField("duration", (jobEndTime > 0 ? jobEndTime : now) - jobStartTime);
		gen.writeNumberField("now", now);
		
		// timestamps
		gen.writeObjectFieldStart("timestamps");
		for (JobStatus status : JobStatus.values()) {
			gen.writeNumberField(status.name(), graph.getStatusTimestamp(status));
		}
		gen.writeEndObject();
		
		// job vertices
		int[] jobVerticesPerState = new int[ExecutionState.values().length];
		gen.writeArrayFieldStart("vertices");

		for (AccessExecutionJobVertex ejv : graph.getVerticesTopologically()) {
			int[] tasksPerState = new int[ExecutionState.values().length];
			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;
			
			for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

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
			
			ExecutionState jobVertexState = 
					ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, ejv.getParallelism());
			jobVerticesPerState[jobVertexState.ordinal()]++;

			gen.writeStartObject();
			gen.writeStringField("id", ejv.getJobVertexId().toString());
			gen.writeStringField("name", ejv.getName());
			gen.writeNumberField("parallelism", ejv.getParallelism());
			gen.writeStringField("status", jobVertexState.name());

			gen.writeNumberField("start-time", startTime);
			gen.writeNumberField("end-time", endTime);
			gen.writeNumberField("duration", duration);
			
			gen.writeObjectFieldStart("tasks");
			for (ExecutionState state : ExecutionState.values()) {
				gen.writeNumberField(state.name(), tasksPerState[state.ordinal()]);
			}
			gen.writeEndObject();
			
			long numBytesIn = 0;
			long numBytesOut = 0;
			long numRecordsIn = 0;
			long numRecordsOut = 0;

			for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
				IOMetrics ioMetrics = vertex.getCurrentExecutionAttempt().getIOMetrics();

				if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
					numBytesIn += ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
					numBytesOut += ioMetrics.getNumBytesOut();
					numRecordsIn += ioMetrics.getNumRecordsIn();
					numRecordsOut += ioMetrics.getNumRecordsOut();
				} else { // execAttempt is still running, use MetricQueryService instead
					fetcher.update();
					MetricStore.SubtaskMetricStore metrics = fetcher.getMetricStore().getSubtaskMetricStore(graph.getJobID().toString(), ejv.getJobVertexId().toString(), vertex.getParallelSubtaskIndex());
					if (metrics != null) {
						numBytesIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL, "0")) + Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE, "0"));
						numBytesOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT, "0"));
						numRecordsIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0"));
						numRecordsOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT, "0"));
					}
				}
			}

			gen.writeObjectFieldStart("metrics");
			gen.writeNumberField("read-bytes", numBytesIn);
			gen.writeNumberField("write-bytes", numBytesOut);
			gen.writeNumberField("read-records", numRecordsIn);
			gen.writeNumberField("write-records", numRecordsOut);
			gen.writeEndObject();
			
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeObjectFieldStart("status-counts");
		for (ExecutionState state : ExecutionState.values()) {
			gen.writeNumberField(state.name(), jobVerticesPerState[state.ordinal()]);
		}
		gen.writeEndObject();

		gen.writeFieldName("plan");
		gen.writeRawValue(graph.getJsonPlan());

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
