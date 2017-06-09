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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.utils.MutableIOMetrics;

import com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A request handler that provides the details of a job vertex, including id, name, parallelism,
 * and the runtime and metrics of all its subtasks.
 */
public class JobVertexDetailsHandler extends AbstractJobVertexRequestHandler {

	private static final String JOB_VERTEX_DETAILS_REST_PATH = "/jobs/:jobid/vertices/:vertexid";

	private final MetricFetcher fetcher;

	public JobVertexDetailsHandler(ExecutionGraphHolder executionGraphHolder, MetricFetcher fetcher) {
		super(executionGraphHolder);
		this.fetcher = fetcher;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_VERTEX_DETAILS_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		return createVertexDetailsJson(jobVertex, params.get("jobid"), fetcher);
	}

	/**
	 * Archivist for the JobVertexDetailsHandler.
	 */
	public static class JobVertexDetailsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			List<ArchivedJson> archive = new ArrayList<>();
			for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
				String json = createVertexDetailsJson(task, graph.getJobID().toString(), null);
				String path = JOB_VERTEX_DETAILS_REST_PATH
					.replace(":jobid", graph.getJobID().toString())
					.replace(":vertexid", task.getJobVertexId().toString());
				archive.add(new ArchivedJson(path, json));
			}
			return archive;
		}
	}

	public static String createVertexDetailsJson(
			AccessExecutionJobVertex jobVertex,
			String jobID,
			@Nullable MetricFetcher fetcher) throws IOException {
		final long now = System.currentTimeMillis();

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getName());
		gen.writeNumberField("parallelism", jobVertex.getParallelism());
		gen.writeNumberField("now", now);

		gen.writeArrayFieldStart("subtasks");
		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			final ExecutionState status = vertex.getExecutionState();

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			long startTime = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
			if (startTime == 0) {
				startTime = -1;
			}
			long endTime = status.isTerminal() ? vertex.getStateTimestamp(status) : -1;
			long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

			gen.writeStartObject();
			gen.writeNumberField("subtask", num);
			gen.writeStringField("status", status.name());
			gen.writeNumberField("attempt", vertex.getCurrentExecutionAttempt().getAttemptNumber());
			gen.writeStringField("host", locationString);
			gen.writeNumberField("start-time", startTime);
			gen.writeNumberField("end-time", endTime);
			gen.writeNumberField("duration", duration);

			MutableIOMetrics counts = new MutableIOMetrics();

			counts.addIOMetrics(
				vertex.getCurrentExecutionAttempt(),
				fetcher,
				jobID,
				jobVertex.getJobVertexId().toString()
			);

			counts.writeIOMetricsAsJson(gen);

			gen.writeEndObject();

			num++;
		}
		gen.writeEndArray();

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
