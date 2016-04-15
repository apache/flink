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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
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

		ExecutionConfig ec = graph.getExecutionConfig();
		if (ec != null) {
			gen.writeObjectFieldStart("execution-config");
			
			gen.writeStringField("execution-mode", ec.getExecutionMode().name());

			final String restartStrategyDescription = ec.getRestartStrategy() != null ? ec.getRestartStrategy().getDescription() : "default";
			gen.writeStringField("restart-strategy", restartStrategyDescription);
			gen.writeNumberField("job-parallelism", ec.getParallelism());
			gen.writeBooleanField("object-reuse-mode", ec.isObjectReuseEnabled());

			ExecutionConfig.GlobalJobParameters uc = ec.getGlobalJobParameters();
			if (uc != null) {
				Map<String, String> ucVals = uc.toMap();
				if (ucVals != null) {
					gen.writeObjectFieldStart("user-config");
					
					for (Map.Entry<String, String> ucVal : ucVals.entrySet()) {
						gen.writeStringField(ucVal.getKey(), ucVal.getValue());
					}

					gen.writeEndObject();
				}
			}
			
			gen.writeEndObject();
		}
		gen.writeEndObject();
		
		gen.close();
		return writer.toString();
	}
}
