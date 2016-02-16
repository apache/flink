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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;

import java.io.StringWriter;
import java.util.Map;

/**
 * A request handler that provides the details of a job vertex, including id, name, parallelism,
 * and the runtime and metrics of all its subtasks.
 */
public class JobVertexDetailsHandler extends AbstractJobVertexRequestHandler {
	
	public JobVertexDetailsHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionJobVertex jobVertex, Map<String, String> params) throws Exception {
		final long now = System.currentTimeMillis();
		
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();

		gen.writeStringField("id", jobVertex.getJobVertexId().toString());
		gen.writeStringField("name", jobVertex.getJobVertex().getName());
		gen.writeNumberField("parallelism", jobVertex.getParallelism());
		gen.writeNumberField("now", now);

		gen.writeArrayFieldStart("subtasks");
		int num = 0;
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			final ExecutionState status = vertex.getExecutionState();
			
			InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			long startTime = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
			if (startTime == 0) {
				startTime = -1;
			}
			long endTime = status.isTerminal() ? vertex.getStateTimestamp(status) : -1;
			long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;
			
			Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> metrics = vertex.getCurrentExecutionAttempt().getFlinkAccumulators();
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
			
			gen.writeStartObject();
			gen.writeNumberField("subtask", num);
			gen.writeStringField("status", status.name());
			gen.writeNumberField("attempt", vertex.getCurrentExecutionAttempt().getAttemptNumber());
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
			
			num++;
		}
		gen.writeEndArray();
		
		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
