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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler providing details about a single task execution attempt.
 */
public class SubtaskExecutionAttemptDetailsHandler extends AbstractSubtaskAttemptRequestHandler implements RequestHandler.JsonResponse {
	
	public SubtaskExecutionAttemptDetailsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(Execution execAttempt, Map<String, String> params) throws Exception {
		final ExecutionState status = execAttempt.getState();
		final long now = System.currentTimeMillis();

		InstanceConnectionInfo location = execAttempt.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname();

		long startTime = execAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		long endTime = status.isTerminal() ? execAttempt.getStateTimestamp(status) : -1;
		long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> metrics = execAttempt.getFlinkAccumulators();
		LongCounter readBytes;
		LongCounter writeBytes;
		LongCounter readRecords;
		LongCounter writeRecords;

		if (metrics != null) {
			readBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_IN);
			writeBytes = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_BYTES_OUT);
			readRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_IN);
			writeRecords = (LongCounter) metrics.get(AccumulatorRegistry.Metric.NUM_RECORDS_OUT);
		}
		else {
			readBytes = null;
			writeBytes = null;
			readRecords = null;
			writeRecords = null;
		}

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

		gen.writeStartObject();
		gen.writeNumberField("subtask", execAttempt.getVertex().getParallelSubtaskIndex());
		gen.writeStringField("status", status.name());
		gen.writeNumberField("attempt", execAttempt.getAttemptNumber());
		gen.writeStringField("host", locationString);
		gen.writeNumberField("start-time", startTime);
		gen.writeNumberField("end-time", endTime);
		gen.writeNumberField("duration", duration);

		gen.writeObjectFieldStart("metrics");
		gen.writeNumberField("read-bytes", readBytes != null ? readBytes.getLocalValuePrimitive() : -1L);
		gen.writeNumberField("write-bytes", writeBytes != null ? writeBytes.getLocalValuePrimitive() : -1L);
		gen.writeNumberField("read-records", readRecords != null ? readRecords.getLocalValuePrimitive() : -1L);
		gen.writeNumberField("write-records",writeRecords != null ? writeRecords.getLocalValuePrimitive() : -1L);
		gen.writeEndObject();

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
