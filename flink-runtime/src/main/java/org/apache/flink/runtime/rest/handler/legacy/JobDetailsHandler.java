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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Request handler that returns details about a job. This includes:
 * <ul>
 *     <li>Dataflow plan</li>
 *     <li>id, name, and current status</li>
 *     <li>start time, end time, duration</li>
 *     <li>number of job vertices in each state (pending, running, finished, failed)</li>
 *     <li>info about job vertices, including runtime, status, I/O bytes and records, subtasks in each status</li>
 * </ul>
 */
public class JobDetailsHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_DETAILS_REST_PATH = "/jobs/:jobid";
	private static final String JOB_DETAILS_VERTICES_REST_PATH = "/jobs/:jobid/vertices";

	private final MetricFetcher fetcher;

	public JobDetailsHandler(ExecutionGraphCache executionGraphHolder, Executor executor, MetricFetcher fetcher) {
		super(executionGraphHolder, executor);
		this.fetcher = fetcher;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_DETAILS_REST_PATH, JOB_DETAILS_VERTICES_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createJobDetailsJson(graph, fetcher);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create job details json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the JobDetailsHandler.
	 */
	public static class JobDetailsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createJobDetailsJson(graph, null);
			String path1 = JOB_DETAILS_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			String path2 = JOB_DETAILS_VERTICES_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			Collection<ArchivedJson> archives = new ArrayList<>();
			archives.add(new ArchivedJson(path1, json));
			archives.add(new ArchivedJson(path2, json));
			return archives;
		}
	}

	public static String createJobDetailsJson(AccessExecutionGraph graph, @Nullable MetricFetcher fetcher) throws IOException {
		final StringWriter writer = new StringWriter();
		final JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

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

			MutableIOMetrics counts = new MutableIOMetrics();

			for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
				counts.addIOMetrics(
					vertex.getCurrentExecutionAttempt(),
					fetcher,
					graph.getJobID().toString(),
					ejv.getJobVertexId().toString());
			}

			counts.writeIOMetricsAsJson(gen);

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
