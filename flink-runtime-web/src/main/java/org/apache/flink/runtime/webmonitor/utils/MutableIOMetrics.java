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

import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexDetailsHandler;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.MetricStore;

import com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class is a mutable version of the {@link IOMetrics} class that allows adding up IO-related metrics.
 *
 * <p>For finished jobs these metrics are stored in the {@link ExecutionGraph} as another {@link IOMetrics}.
 * For running jobs these metrics are retrieved using the {@link MetricFetcher}.
 *
 * <p>This class provides a common interface to handle both cases, reducing complexity in various handlers (like
 * the {@link JobVertexDetailsHandler}).
 */
public class MutableIOMetrics extends IOMetrics {

	private static final long serialVersionUID = -5460777634971381737L;

	public MutableIOMetrics() {
		super(0, 0, 0, 0, 0, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D);
	}

	/**
	 * Adds the IO metrics for the given attempt to this object. If the {@link AccessExecution} is in
	 * a terminal state the contained {@link IOMetrics} object is added. Otherwise the given {@link MetricFetcher} is
	 * used to retrieve the required metrics.
	 *
	 * @param attempt Attempt whose IO metrics should be added
	 * @param fetcher MetricFetcher to retrieve metrics for running jobs
	 * @param jobID JobID to which the attempt belongs
	 * @param taskID TaskID to which the attempt belongs
	 */
	public void addIOMetrics(AccessExecution attempt, @Nullable MetricFetcher fetcher, String jobID, String taskID) {
		if (attempt.getState().isTerminal()) {
			IOMetrics ioMetrics = attempt.getIOMetrics();
			if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
				this.numBytesInLocal += ioMetrics.getNumBytesInLocal();
				this.numBytesInRemote += ioMetrics.getNumBytesInRemote();
				this.numBytesOut += ioMetrics.getNumBytesOut();
				this.numRecordsIn += ioMetrics.getNumRecordsIn();
				this.numRecordsOut += ioMetrics.getNumRecordsOut();
			}
		} else { // execAttempt is still running, use MetricQueryService instead
			if (fetcher != null) {
				fetcher.update();
				MetricStore metricStore = fetcher.getMetricStore();
				synchronized (metricStore) {
					MetricStore.SubtaskMetricStore metrics = metricStore.getSubtaskMetricStore(jobID, taskID, attempt.getParallelSubtaskIndex());
					if (metrics != null) {
						/**
						 * We want to keep track of missing metrics to be able to make a difference between 0 as a value
						 * and a missing value.
						 * In case a metric is missing for a parallel instance of a task, we initialize it with -1 and
						 * consider it as incomplete.
						 */
						if (this.numBytesInLocal >= 0 && metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL) != null){
							this.numBytesInLocal += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL));
						}
						else {
							this.numBytesInLocal = -1;
						}
						if (this.numBytesInRemote >= 0 && metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE) != null){
							this.numBytesInRemote += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE));
						}
						else {
							this.numBytesInRemote = -1;
						}
						if (this.numBytesOut >= 0 && metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT) != null){
							this.numBytesOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT));
						}
						else {
							this.numBytesOut = -1;
						}
						if (this.numRecordsIn >= 0 && metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN) != null){
							this.numRecordsIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN));
						}
						else {
							this.numRecordsIn = -1;
						}
						if (this.numRecordsOut >= 0 && metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT) != null){
							this.numRecordsOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT));
						}
						else {
							this.numRecordsOut = -1;
						}
					}
					else {
						this.numBytesInLocal = -1;
						this.numBytesInRemote = -1;
						this.numBytesOut = -1;
						this.numRecordsIn = -1;
						this.numRecordsOut = -1;
					}
				}
			}
		}
	}

	/**
	 * Writes the IO metrics contained in this object to the given {@link JsonGenerator}.
	 *
	 * <p>The JSON structure written is as follows:
	 * "metrics": {
	 *     "read-bytes": 1,
	 *     "write-bytes": 2,
	 *     "read-records": 3,
	 *     "write-records": 4
	 * }
	 *
	 * @param gen JsonGenerator to which the metrics should be written
	 * @throws IOException
	 */
	public void writeIOMetricsAsJson(JsonGenerator gen) throws IOException {
		/**
		 * As described in {@link addIOMetrics}, we want to distinguish incomplete values from 0.
		 * However, for API backward compatibility, incomplete metrics will still be represented by the 0 value and
		 * a boolean will indicate the completeness.
		 */

		gen.writeObjectFieldStart("metrics");

		Long numBytesIn = (this.numBytesInLocal >= 0 && this.numBytesInRemote >= 0) ? (this.numBytesInLocal + this.numBytesInRemote) : -1;

		writeIOMetricWithCompleteness(gen, "read-bytes", numBytesIn);
		writeIOMetricWithCompleteness(gen, "write-bytes", this.numBytesOut);
		writeIOMetricWithCompleteness(gen, "read-records", this.numRecordsIn);
		writeIOMetricWithCompleteness(gen, "write-records", this.numRecordsOut);

		gen.writeEndObject();
	}

	/**
	 * Write the IO Metric to the JSON with its completeness attribute.
	 *
	 * @param gen JsonGenerator to which the metrics should be written
	 * @param fieldName Name of the added JSON attribute
	 * @param fieldValue Value of the added JSON attribute
	 * @throws IOException
	 */
	private void writeIOMetricWithCompleteness(JsonGenerator gen, String fieldName, Long fieldValue) throws IOException{
		boolean isComplete = fieldValue >= 0;
		gen.writeNumberField(fieldName, isComplete ? fieldValue : 0);
		gen.writeBooleanField(fieldName + "-complete", isComplete);
	}
}
