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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * A request handler that provides the details of a job vertex, including id, name, and the
 * runtime and metrics of all its subtasks aggregated by TaskManager.
 */
public class JobVertexTaskManagersHandler extends AbstractJobVertexRequestHandler {

	private static final String JOB_VERTEX_TASKMANAGERS_REST_PATH = "/jobs/:jobid/vertices/:vertexid/taskmanagers";

	private final MetricFetcher fetcher;

	public JobVertexTaskManagersHandler(ExecutionGraphCache executionGraphHolder, Executor executor, MetricFetcher fetcher) {
		super(executionGraphHolder, executor);
		this.fetcher = fetcher;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_VERTEX_TASKMANAGERS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createVertexDetailsByTaskManagerJson(jobVertex, params.get("jobid"), fetcher);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create TaskManager json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for JobVertexTaskManagersHandler.
	 */
	public static class JobVertexTaskManagersJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			List<ArchivedJson> archive = new ArrayList<>();
			for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
				String json = createVertexDetailsByTaskManagerJson(task, graph.getJobID().toString(), null);
				String path = JOB_VERTEX_TASKMANAGERS_REST_PATH
					.replace(":jobid", graph.getJobID().toString())
					.replace(":vertexid", task.getJobVertexId().toString());
				archive.add(new ArchivedJson(path, json));
			}
			return archive;
		}
	}

	public static String createVertexDetailsByTaskManagerJson(
			AccessExecutionJobVertex jobVertex,
			String jobID,
			@Nullable MetricFetcher fetcher) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

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

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getName());
		gen.writeNumberField("now", now);

		gen.writeArrayFieldStart("taskmanagers");
		for (Map.Entry<String, List<AccessExecutionVertex>> entry : taskManagerVertices.entrySet()) {
			String host = entry.getKey();
			List<AccessExecutionVertex> taskVertices = entry.getValue();

			int[] tasksPerState = new int[ExecutionState.values().length];

			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			MutableIOMetrics counts = new MutableIOMetrics();

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

				counts.addIOMetrics(
					vertex.getCurrentExecutionAttempt(),
					fetcher,
					jobID,
					jobVertex.getJobVertexId().toString());
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

			counts.writeIOMetricsAsJson(gen);

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
