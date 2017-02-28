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
package org.apache.flink.runtime.webmonitor.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;

import javax.annotation.Nullable;
import java.io.IOException;

public class MutableIOMetrics extends IOMetrics {

	private static final long serialVersionUID = -5460777634971381737L;

	public MutableIOMetrics() {
		super(0, 0, 0, 0, 0, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D);
	}

	public void addNumBytesInLocal(long toAdd) {
		this.numBytesInLocal += toAdd;
	}

	public void addNumBytesInRemote(long toAdd) {
		this.numBytesInRemote += toAdd;
	}

	public void addNumBytesOut(long toAdd) {
		this.numBytesOut += toAdd;
	}

	public void addNumRecordsIn(long toAdd) {
		this.numRecordsIn += toAdd;
	}

	public void addNumRecordsOut(long toAdd) {
		this.numRecordsOut += toAdd;
	}

	public static void addIOMetrics(MutableIOMetrics summedMetrics, ExecutionState state, @Nullable IOMetrics ioMetrics, @Nullable MetricFetcher fetcher, String jobID, String taskID, int subtaskIndex) {
		if (state.isTerminal()) {
			if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
				summedMetrics.addNumBytesInLocal(ioMetrics.getNumBytesInLocal());
				summedMetrics.addNumBytesInRemote(ioMetrics.getNumBytesInRemote());
				summedMetrics.addNumBytesOut(ioMetrics.getNumBytesOut());
				summedMetrics.addNumRecordsIn(ioMetrics.getNumRecordsIn());
				summedMetrics.addNumRecordsOut(ioMetrics.getNumRecordsOut());
			}
		} else { // execAttempt is still running, use MetricQueryService instead
			if (fetcher != null) {
				fetcher.update();
				MetricStore.SubtaskMetricStore metrics = fetcher.getMetricStore().getSubtaskMetricStore(jobID, taskID, subtaskIndex);
				if (metrics != null) {
					summedMetrics.addNumBytesInLocal(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL, "0")));
					summedMetrics.addNumBytesInRemote(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE, "0")));
					summedMetrics.addNumBytesOut(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT, "0")));
					summedMetrics.addNumRecordsIn(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0")));
					summedMetrics.addNumRecordsOut(Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT, "0")));
				}
			}
		}
	}

	public static void writeIOMetrics(JsonGenerator gen, IOMetrics metrics) throws IOException {
		gen.writeObjectFieldStart("metrics");
		gen.writeNumberField("read-bytes", metrics.getNumBytesInLocal() + metrics.getNumBytesInRemote());
		gen.writeNumberField("write-bytes", metrics.getNumBytesOut());
		gen.writeNumberField("read-records", metrics.getNumRecordsIn());
		gen.writeNumberField("write-records", metrics.getNumRecordsOut());
		gen.writeEndObject();
	}
}
