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
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Request handler that returns the aggregated user accumulators of a job.
 */
public class JobAccumulatorsHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_ACCUMULATORS_REST_PATH = "/jobs/:jobid/accumulators";

	public JobAccumulatorsHandler(ExecutionGraphCache executionGraphHolder, Executor executor) {
		super(executionGraphHolder, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_ACCUMULATORS_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createJobAccumulatorsJson(graph);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not create job accumulators json.", e));
				}
			},
			executor);
	}

	/**
	 * Archivist for the JobAccumulatorsHandler.
	 */
	public static class JobAccumulatorsJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createJobAccumulatorsJson(graph);
			String path = JOB_ACCUMULATORS_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	public static String createJobAccumulatorsJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		StringifiedAccumulatorResult[] allAccumulators = graph.getAccumulatorResultsStringified();

		gen.writeStartObject();

		gen.writeArrayFieldStart("job-accumulators");
		// empty for now
		gen.writeEndArray();

		gen.writeArrayFieldStart("user-task-accumulators");
		for (StringifiedAccumulatorResult acc : allAccumulators) {
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
