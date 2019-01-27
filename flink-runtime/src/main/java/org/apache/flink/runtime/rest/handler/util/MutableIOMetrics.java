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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.legacy.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

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
	private boolean numBytesInLocalComplete = true;
	private boolean numBytesInRemoteComplete = true;
	private boolean numBytesOutComplete = true;
	private boolean numRecordsInComplete = true;
	private boolean numRecordsOutComplete = true;
	private float bufferInPoolUsageMax = 0.0f;
	private float bufferOutPoolUsageMax = 0.0f;
	private boolean bufferInPoolUsageMaxComplete = true;
	private boolean bufferOutPoolUsageMaxComplete = true;
	private double tps = -1D;
	private boolean tpsComplete = true;
	private long delay = -1L;
	private boolean delayComplete = true;

	public MutableIOMetrics() {
		super(0, 0, 0, 0, 0, 0.0D, 0.0D, 0.0D, 0.0D, 0.0D);
	}

	public boolean isNumBytesInLocalComplete() {
		return numBytesInLocalComplete;
	}

	public boolean isNumBytesInRemoteComplete() {
		return numBytesInRemoteComplete;
	}

	public boolean isNumBytesOutComplete() {
		return numBytesOutComplete;
	}

	public boolean isNumRecordsInComplete() {
		return numRecordsInComplete;
	}

	public boolean isNumRecordsOutComplete() {
		return numRecordsOutComplete;
	}

	public boolean isBufferInPoolUsageMaxComplete() {
		return bufferInPoolUsageMaxComplete;
	}

	public boolean isBufferOutPoolUsageMaxComplete() {
		return bufferOutPoolUsageMaxComplete;
	}

	public float getBufferInPoolUsageMax() {
		return bufferInPoolUsageMax;
	}

	public float getBufferOutPoolUsageMax() {
		return bufferOutPoolUsageMax;
	}

	public double getTps() {
		return tps;
	}

	public boolean isTpsComplete() {
		return tpsComplete;
	}

	public long getDelay() {
		return delay;
	}

	public boolean isDelayComplete() {
		return delayComplete;
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
				MetricStore.ComponentMetricStore metrics = fetcher.getMetricStore()
					.getSubtaskMetricStore(jobID, taskID, attempt.getParallelSubtaskIndex());
				if (metrics != null) {
					/**
					 * We want to keep track of missing metrics to be able to make a difference between 0 as a value
					 * and a missing value.
					 * In case a metric is missing for a parallel instance of a task, we set the complete flag as
					 * false.
					 */
					boolean findTps = false;
					double tps = 0.0D;
					for (Map.Entry<String, String> entry : metrics.getMetrics().entrySet()) {
						if (entry.getKey().endsWith("." + MetricNames.IO_NUM_TPS) &&
							StringUtils.isNotBlank(entry.getValue())) {
							tps = Double.valueOf(entry.getValue());
							findTps = true;
						} else if (entry.getKey().endsWith("." + MetricNames.IO_NUM_DELAY) &&
							StringUtils.isNotBlank(entry.getValue())) {
							long delay = Long.valueOf(entry.getValue());
							this.delay = Math.max(delay, this.delay);
						}
					}
					if (metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL) == null){
						this.numBytesInLocalComplete = false;
					}
					else {
						this.numBytesInLocal += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_LOCAL));
					}

					if (metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE) == null){
						this.numBytesInRemoteComplete = false;
					}
					else {
						this.numBytesInRemote += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_IN_REMOTE));
					}

					if (metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT) == null){
						this.numBytesOutComplete = false;
					}
					else {
						this.numBytesOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_BYTES_OUT));
					}

					if (metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN) == null){
						this.numRecordsInComplete = false;
					}
					else {
						this.numRecordsIn += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN));
					}

					if (metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT) == null){
						this.numRecordsOutComplete = false;
					}
					else {
						this.numRecordsOut += Long.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_OUT));
					}

					if (metrics.getMetric(MetricNames.BUFFERS_IN_POOL_USAGE_NAME) == null) {
						this.bufferInPoolUsageMaxComplete = false;
					} else {
						float bufferInQueue = Float.valueOf(metrics.getMetric(MetricNames.BUFFERS_IN_POOL_USAGE_NAME));
						this.bufferInPoolUsageMax = Math.max(bufferInQueue, this.bufferInPoolUsageMax);
					}

					if (metrics.getMetric(MetricNames.BUFFERS_OUT_POOL_USAGE_NAME) == null) {
						this.bufferOutPoolUsageMaxComplete = false;
					} else {
						float bufferOutQueue = Float.valueOf(metrics.getMetric(MetricNames.BUFFERS_OUT_POOL_USAGE_NAME));
						this.bufferOutPoolUsageMax = Math.max(bufferOutQueue, this.bufferOutPoolUsageMax);
					}

					if (!findTps && metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN_RATE) != null) {
						tps = Double.valueOf(metrics.getMetric(MetricNames.IO_NUM_RECORDS_IN_RATE));
						findTps = true;
					}
					if (findTps) {
						if (this.tps == -1D) {
							this.tps = tps;
						} else {
							this.tps += tps;
						}
					}
				}
				else {
					this.numBytesInLocalComplete = false;
					this.numBytesInRemoteComplete = false;
					this.numBytesOutComplete = false;
					this.numRecordsInComplete = false;
					this.numRecordsOutComplete = false;
					this.bufferInPoolUsageMaxComplete = false;
					this.bufferOutPoolUsageMaxComplete = false;
					this.tpsComplete = false;
					this.delayComplete = false;
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
	 *     "read-bytes-complete": true,
	 *     "write-bytes": 2,
	 *     "write-bytes-complete": true,
	 *     "read-records": 3,
	 *     "read-records-complete": true,
	 *     "write-records": 4,
	 *     "write-records-complete": true
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

		Long numBytesIn = this.numBytesInLocal + this.numBytesInRemote;
		gen.writeNumberField("read-bytes", numBytesIn);
		gen.writeBooleanField("read-bytes-complete", (this.numBytesInLocalComplete && this.numBytesInRemoteComplete));
		gen.writeNumberField("write-bytes", this.numBytesOut);
		gen.writeBooleanField("write-bytes-complete", this.numBytesOutComplete);
		gen.writeNumberField("read-records", this.numRecordsIn);
		gen.writeBooleanField("read-records-complete", this.numRecordsInComplete);
		gen.writeNumberField("write-records", this.numRecordsOut);
		gen.writeBooleanField("write-records-complete", this.numRecordsOutComplete);
		gen.writeNumberField("buffers-in-pool-usage-max", this.bufferInPoolUsageMax);
		gen.writeBooleanField("buffers-in-pool-usage_max-complete", this.bufferInPoolUsageMaxComplete);
		gen.writeNumberField("buffers-out-pool-usage-max", this.bufferOutPoolUsageMax);
		gen.writeBooleanField("buffers-out-pool-usage-max-complete", this.bufferOutPoolUsageMaxComplete);

		gen.writeEndObject();
	}
}
