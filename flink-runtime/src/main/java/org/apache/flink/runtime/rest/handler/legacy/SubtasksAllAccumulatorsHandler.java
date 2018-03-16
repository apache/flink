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

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Request handler that returns the accumulators for all subtasks of job vertex.
 */
public class SubtasksAllAccumulatorsHandler extends AbstractJobVertexRequestHandler {

	private static final String SUBTASKS_ALL_ACCUMULATORS_REST_PATH = 	"/jobs/:jobid/vertices/:vertexid/subtasks/accumulators";

	public SubtasksAllAccumulatorsHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{SUBTASKS_ALL_ACCUMULATORS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createSubtasksAccumulatorsJson(jobVertex);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create subtasks accumulator json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the SubtasksAllAccumulatorsHandler.
	 */
	public static class SubtasksAllAccumulatorsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			List<ArchivedJson> archive = new ArrayList<>();
			for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
				String json = createSubtasksAccumulatorsJson(task);
				String path = SUBTASKS_ALL_ACCUMULATORS_REST_PATH
					.replace(":jobid", graph.getJobID().toString())
					.replace(":vertexid", task.getJobVertexId().toString());
				archive.add(new ArchivedJson(path, json));
			}
			return archive;
		}
	}

	public static String createSubtasksAccumulatorsJson(AccessExecutionJobVertex jobVertex) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeNumberField("parallelism", jobVertex.getParallelism());

		gen.writeArrayFieldStart("subtasks");

		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();

			gen.writeStartObject();

			gen.writeNumberField("subtask", num++);
			gen.writeNumberField("attempt", vertex.getCurrentExecutionAttempt().getAttemptNumber());
			gen.writeStringField("host", locationString);

			StringifiedAccumulatorResult[] accs = vertex.getCurrentExecutionAttempt().getUserAccumulatorsStringified();
			gen.writeArrayFieldStart("user-accumulators");
			for (StringifiedAccumulatorResult acc : accs) {
				gen.writeStartObject();
				gen.writeStringField("name", acc.getName());
				gen.writeStringField("type", acc.getType());
				gen.writeStringField("value", acc.getValue());
				gen.writeEndObject();
			}
			gen.writeEndArray();

			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.writeEndObject();
		gen.close();
		return writer.toString();
	}
}
