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
 * Request handler that returns the accummulators for a given vertex.
 */
public class JobVertexAccumulatorsHandler extends AbstractJobVertexRequestHandler {

	private static final String JOB_VERTEX_ACCUMULATORS_REST_PATH = "/jobs/:jobid/vertices/:vertexid/accumulators";

	public JobVertexAccumulatorsHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_VERTEX_ACCUMULATORS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionJobVertex jobVertex, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createVertexAccumulatorsJson(jobVertex);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create job vertex accumulators json.", e));
				}
			},
			executor);

	}

	/**
	 * Archivist for JobVertexAccumulatorsHandler.
	 */
	public static class JobVertexAccumulatorsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			List<ArchivedJson> archive = new ArrayList<>();
			for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
				String json = createVertexAccumulatorsJson(task);
				String path = JOB_VERTEX_ACCUMULATORS_REST_PATH
					.replace(":jobid", graph.getJobID().toString())
					.replace(":vertexid", task.getJobVertexId().toString());
				archive.add(new ArchivedJson(path, json));
			}
			return archive;
		}
	}

	public static String createVertexAccumulatorsJson(AccessExecutionJobVertex jobVertex) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		StringifiedAccumulatorResult[] accs = jobVertex.getAggregatedUserAccumulatorsStringified();

		gen.writeStartObject();
		gen.writeStringField("id", jobVertex.getJobVertexId().toString());

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

		gen.close();
		return writer.toString();
	}
}
