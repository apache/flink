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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.messages.JobConfigInfo;
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
 * Request handler that returns the execution config of a job.
 */
public class JobConfigHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_CONFIG_REST_PATH = "/jobs/:jobid/config";

	public JobConfigHandler(ExecutionGraphCache executionGraphCache, Executor executor) {
		super(executionGraphCache, executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_CONFIG_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleRequest(AccessExecutionGraph graph, Map<String, String> params) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return createJobConfigJson(graph);
				} catch (IOException e) {
					throw new CompletionException(new FlinkException("Could not write job config json.", e));
				}
			},
			executor);

	}

	/**
	 * Archivist for the JobConfigHandler.
	 */
	public static class JobConfigJsonArchivist implements JsonArchivist {

		@Override
		public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
			String json = createJobConfigJson(graph);
			String path = JOB_CONFIG_REST_PATH
				.replace(":jobid", graph.getJobID().toString());
			return Collections.singletonList(new ArchivedJson(path, json));
		}
	}

	public static String createJobConfigJson(AccessExecutionGraph graph) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField(JobConfigInfo.FIELD_NAME_JOB_ID, graph.getJobID().toString());
		gen.writeStringField(JobConfigInfo.FIELD_NAME_JOB_NAME, graph.getJobName());

		final ArchivedExecutionConfig summary = graph.getArchivedExecutionConfig();

		if (summary != null) {
			gen.writeObjectFieldStart(JobConfigInfo.FIELD_NAME_EXECUTION_CONFIG);

			gen.writeStringField(JobConfigInfo.ExecutionConfigInfo.FIELD_NAME_EXECUTION_MODE, summary.getExecutionMode());

			gen.writeStringField(JobConfigInfo.ExecutionConfigInfo.FIELD_NAME_RESTART_STRATEGY, summary.getRestartStrategyDescription());
			gen.writeNumberField(JobConfigInfo.ExecutionConfigInfo.FIELD_NAME_PARALLELISM, summary.getParallelism());
			gen.writeBooleanField(JobConfigInfo.ExecutionConfigInfo.FIELD_NAME_OBJECT_REUSE_MODE, summary.getObjectReuseEnabled());

			Map<String, String> ucVals = summary.getGlobalJobParameters();
			if (ucVals != null) {
				gen.writeObjectFieldStart(JobConfigInfo.ExecutionConfigInfo.FIELD_NAME_GLOBAL_JOB_PARAMETERS);

				for (Map.Entry<String, String> ucVal : ucVals.entrySet()) {
					gen.writeStringField(ucVal.getKey(), ucVal.getValue());
				}

				gen.writeEndObject();
			}

			gen.writeEndObject();
		}
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
