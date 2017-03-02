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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import java.util.Collection;
import java.util.Collections;

/**
 * Request handler that returns the execution config of a job.
 */
public class JobConfigHandler extends AbstractExecutionGraphRequestHandler {

	private static final String JOB_CONFIG_REST_PATH = "/jobs/:jobid/config";

	public JobConfigHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_CONFIG_REST_PATH};
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		return createJobConfigJson(graph);
	}

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
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField("jid", graph.getJobID().toString());
		gen.writeStringField("name", graph.getJobName());

		final ArchivedExecutionConfig summary = graph.getArchivedExecutionConfig();

		if (summary != null) {
			gen.writeObjectFieldStart("execution-config");

			gen.writeStringField("execution-mode", summary.getExecutionMode());

			gen.writeStringField("restart-strategy", summary.getRestartStrategyDescription());
			gen.writeNumberField("job-parallelism", summary.getParallelism());
			gen.writeBooleanField("object-reuse-mode", summary.getObjectReuseEnabled());

			Map<String, String> ucVals = summary.getGlobalJobParameters();
			if (ucVals != null) {
				gen.writeObjectFieldStart("user-config");
				
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
