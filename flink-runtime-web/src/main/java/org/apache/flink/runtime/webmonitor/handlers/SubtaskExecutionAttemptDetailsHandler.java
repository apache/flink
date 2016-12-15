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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler providing details about a single task execution attempt.
 */
public class SubtaskExecutionAttemptDetailsHandler extends AbstractSubtaskAttemptRequestHandler {

	private final MetricFetcher fetcher;

	public SubtaskExecutionAttemptDetailsHandler(ExecutionGraphHolder executionGraphHolder, MetricFetcher fetcher) {
		super(executionGraphHolder);
		this.fetcher = fetcher;
	}

	@Override
	public String handleRequest(AccessExecution execAttempt, Map<String, String> params) throws Exception {
		final ExecutionState status = execAttempt.getState();
		final long now = System.currentTimeMillis();

		TaskManagerLocation location = execAttempt.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname();

		long startTime = execAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		long endTime = status.isTerminal() ? execAttempt.getStateTimestamp(status) : -1;
		long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartObject();
		gen.writeNumberField("subtask", execAttempt.getParallelSubtaskIndex());
		gen.writeStringField("status", status.name());
		gen.writeNumberField("attempt", execAttempt.getAttemptNumber());
		gen.writeStringField("host", locationString);
		gen.writeNumberField("start-time", startTime);
		gen.writeNumberField("end-time", endTime);
		gen.writeNumberField("duration", duration);

		IOMetrics ioMetrics = execAttempt.getIOMetrics();

		long numBytesIn = 0;
		long numBytesOut = 0;
		long numRecordsIn = 0;
		long numRecordsOut = 0;
		
		if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
			numBytesIn = ioMetrics.getNumBytesInLocal() + ioMetrics.getNumBytesInRemote();
			numBytesOut = ioMetrics.getNumBytesOut();
			numRecordsIn = ioMetrics.getNumRecordsIn();
			numRecordsOut = ioMetrics.getNumRecordsOut();
		} else { // execAttempt is still running, use MetricQueryService instead
			fetcher.update();
			MetricStore.SubtaskMetricStore metrics = fetcher.getMetricStore().getSubtaskMetricStore(params.get("jobid"), params.get("vertexid"), execAttempt.getParallelSubtaskIndex());
			if (metrics != null) {
				numBytesIn = Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL, "0")) + Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE, "0"));
				numBytesOut = Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT, "0"));
				numRecordsIn = Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0"));
				numRecordsOut = Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT, "0"));
			}
		}
		
		gen.writeObjectFieldStart("metrics");
		gen.writeNumberField("read-bytes", numBytesIn);
		gen.writeNumberField("write-bytes", numBytesOut);
		gen.writeNumberField("read-records", numRecordsIn);
		gen.writeNumberField("write-records", numRecordsOut);
		gen.writeEndObject();

		gen.writeEndObject();

		gen.close();
		return writer.toString();
	}
}
