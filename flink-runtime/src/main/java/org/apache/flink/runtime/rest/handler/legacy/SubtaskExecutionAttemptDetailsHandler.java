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
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.rest.handler.legacy.SubtaskCurrentAttemptDetailsHandler.SUBTASK_CURRENT_ATTEMPT_DETAILS_REST_PATH;

/**
 * Request handler providing details about a single task execution attempt.
 */
public class SubtaskExecutionAttemptDetailsHandler extends AbstractSubtaskAttemptRequestHandler {

	public static final String PARAMETER_SUBTASK_INDEX = "subtasknum";
	private static final String SUBTASK_ATTEMPT_DETAILS_REST_PATH = "/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt";

	private final MetricFetcher fetcher;

	public SubtaskExecutionAttemptDetailsHandler(ExecutionGraphCache executionGraphHolder, Executor executor, MetricFetcher fetcher) {
		super(executionGraphHolder, executor);
		this.fetcher = fetcher;
	}

	@Override
	public String[] getPaths() {
		return new String[]{SUBTASK_ATTEMPT_DETAILS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecution execAttempt, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createAttemptDetailsJson(execAttempt, params.get("jobid"), params.get("vertexid"), fetcher);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create attempt details json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the SubtaskExecutionAttemptDetailsHandler.
	 */
	public static class SubtaskExecutionAttemptDetailsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			List<ArchivedJson> archive = new ArrayList<>();
			for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
				for (AccessExecutionVertex subtask : task.getTaskVertices()) {
					String curAttemptJson = createAttemptDetailsJson(subtask.getCurrentExecutionAttempt(), graph.getJobID().toString(), task.getJobVertexId().toString(), null);
					String curAttemptPath1 = SUBTASK_CURRENT_ATTEMPT_DETAILS_REST_PATH
						.replace(":jobid", graph.getJobID().toString())
						.replace(":vertexid", task.getJobVertexId().toString())
						.replace(":subtasknum", String.valueOf(subtask.getParallelSubtaskIndex()));
					String curAttemptPath2 = SUBTASK_ATTEMPT_DETAILS_REST_PATH
						.replace(":jobid", graph.getJobID().toString())
						.replace(":vertexid", task.getJobVertexId().toString())
						.replace(":subtasknum", String.valueOf(subtask.getParallelSubtaskIndex()))
						.replace(":attempt", String.valueOf(subtask.getCurrentExecutionAttempt().getAttemptNumber()));

					archive.add(new ArchivedJson(curAttemptPath1, curAttemptJson));
					archive.add(new ArchivedJson(curAttemptPath2, curAttemptJson));

					for (int x = 0; x < subtask.getCurrentExecutionAttempt().getAttemptNumber(); x++) {
						AccessExecution attempt = subtask.getPriorExecutionAttempt(x);
						String json = createAttemptDetailsJson(attempt, graph.getJobID().toString(), task.getJobVertexId().toString(), null);
						String path = SUBTASK_ATTEMPT_DETAILS_REST_PATH
							.replace(":jobid", graph.getJobID().toString())
							.replace(":vertexid", task.getJobVertexId().toString())
							.replace(":subtasknum", String.valueOf(subtask.getParallelSubtaskIndex()))
							.replace(":attempt", String.valueOf(attempt.getAttemptNumber()));
						archive.add(new ArchivedJson(path, json));
					}
				}
			}
			return archive;
		}
	}

	public static String createAttemptDetailsJson(
			AccessExecution execAttempt,
			String jobID,
			String vertexID,
			@Nullable MetricFetcher fetcher) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		final ExecutionState status = execAttempt.getState();
		final long now = System.currentTimeMillis();

		TaskManagerLocation location = execAttempt.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname();

		long startTime = execAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		long endTime = status.isTerminal() ? execAttempt.getStateTimestamp(status) : -1;
		long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		gen.writeStartObject();
		gen.writeNumberField("subtask", execAttempt.getParallelSubtaskIndex());
		gen.writeStringField("status", status.name());
		gen.writeNumberField("attempt", execAttempt.getAttemptNumber());
		gen.writeStringField("host", locationString);
		gen.writeNumberField("start-time", startTime);
		gen.writeNumberField("end-time", endTime);
		gen.writeNumberField("duration", duration);

		MutableIOMetrics counts = new MutableIOMetrics();

		counts.addIOMetrics(
			execAttempt,
			fetcher,
			jobID,
			vertexID
		);

		counts.writeIOMetricsAsJson(gen);

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
