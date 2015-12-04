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

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * Base class for request handlers whose response depends on a specific job vertex (defined
 * via the "vertexid" parameter) in a specific job, defined via (defined voa the "jobid" parameter).  
 */
public class SubtaskExecutionAttemptAccumulatorsHandler extends AbstractSubtaskAttemptRequestHandler implements RequestHandler.JsonResponse {
	
	public SubtaskExecutionAttemptAccumulatorsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(Execution execAttempt, Map<String, String> params) throws Exception {
		final StringifiedAccumulatorResult[] accs = execAttempt.getUserAccumulatorsStringified();
		
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

		gen.writeStartObject();

		gen.writeNumberField("subtask", execAttempt.getVertex().getParallelSubtaskIndex());
		gen.writeNumberField("attempt", execAttempt.getAttemptNumber());
		gen.writeStringField("id", execAttempt.getAttemptId().toShortString());
		
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
