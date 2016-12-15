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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;

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

	private final MetricFetcher fetcher;

	public JobVertexTaskManagersHandler(ExecutionGraphHolder executionGraphHolder, MetricFetcher fetcher) {
		super(executionGraphHolder);
		this.fetcher = fetcher;
	}

	@Override
	public String handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		// Build a map that groups tasks by TaskManager
		Map<String, List<AccessExecutionVertex>> taskManagerVertices = new HashMap<>();

		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String taskManager = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			List<AccessExecutionVertex> vertices = taskManagerVertices.get(taskManager);

			if (vertices == null) {
				vertices = new ArrayList<>();
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
		gen.writeStringField("name", jobVertex.getName());
		gen.writeNumberField("now", now);

		gen.writeArrayFieldStart("taskmanagers");
		for (Entry<String, List<AccessExecutionVertex>> entry : taskManagerVertices.entrySet()) {
			String host = entry.getKey();
			List<AccessExecutionVertex> taskVertices = entry.getValue();

			int[] tasksPerState = new int[ExecutionState.values().length];

			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			long numBytesIn = 0;
			long numBytesOut = 0;
			long numRecordsIn = 0;
			long numRecordsOut = 0;

			for (AccessExecutionVertex vertex : taskVertices) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}

				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));

				IOMetrics ioMetrics = vertex.getCurrentExecutionAttempt().getIOMetrics();

				if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
					numBytesIn += ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
					numBytesOut += ioMetrics.getNumBytesOut();
					numRecordsIn += ioMetrics.getNumRecordsIn();
					numRecordsOut += ioMetrics.getNumRecordsOut();
				} else { // execAttempt is still running, use MetricQueryService instead
					fetcher.update();
					MetricStore.SubtaskMetricStore metrics = fetcher.getMetricStore().getSubtaskMetricStore(params.get("jobid"), params.get("vertexid"), vertex.getParallelSubtaskIndex());
					if (metrics != null) {
						numBytesIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL, "0")) + Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE, "0"));
						numBytesOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT, "0"));
						numRecordsIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0"));
						numRecordsOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT, "0"));
					}
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
			gen.writeNumberField("read-bytes", numBytesIn);
			gen.writeNumberField("write-bytes", numBytesOut);
			gen.writeNumberField("read-records", numRecordsIn);
			gen.writeNumberField("write-records", numRecordsOut);
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
