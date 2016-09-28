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

import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.archive.ExecutionConfigSummary;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

/**
 * Request handler that returns the execution config of a job.
 */
public class JobConfigHandler extends AbstractExecutionGraphRequestHandler {

	public JobConfigHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField("jid", graph.getJobID().toString());
		gen.writeStringField("name", graph.getJobName());

		final ExecutionConfigSummary summary = graph.getExecutionConfigSummary();

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
