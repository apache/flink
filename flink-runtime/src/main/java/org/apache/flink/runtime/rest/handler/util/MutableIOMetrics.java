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
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;

import javax.annotation.Nullable;

import java.util.function.Consumer;

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
	private boolean numBytesInComplete = true;
	private boolean numBytesOutComplete = true;
	private boolean numRecordsInComplete = true;
	private boolean numRecordsOutComplete = true;
	private boolean usageInputFloatingBuffersComplete = true;
	private boolean usageInputExclusiveBuffersComplete = true;
	private boolean usageOutPoolComplete = true;
	private boolean isBackPressuredComplete = true;

	public MutableIOMetrics() {
		super(0, 0, 0, 0, 0.0f, 0.0f, 0.0f, false);
	}

	public boolean isNumBytesInComplete() {
		return numBytesInComplete;
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

	public boolean isUsageInputFloatingBuffersComplete() {
		return usageInputFloatingBuffersComplete;
	}

	public boolean isUsageInputExclusiveBuffersComplete() {
		return usageInputExclusiveBuffersComplete;
	}

	public boolean isUsageOutPoolComplete() {
		return usageOutPoolComplete;
	}

	public boolean isBackPressuredComplete() {
		return isBackPressuredComplete;
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
				this.numBytesIn += ioMetrics.getNumBytesIn();
				this.numBytesOut += ioMetrics.getNumBytesOut();
				this.numRecordsIn += ioMetrics.getNumRecordsIn();
				this.numRecordsOut += ioMetrics.getNumRecordsOut();
				this.usageInputExclusiveBuffers += ioMetrics.getUsageInputExclusiveBuffers();
				this.usageInputFloatingBuffers += ioMetrics.getUsageInputFloatingBuffers();
				this.usageOutPool += ioMetrics.getUsageOutPool();
				this.isBackPressured |= ioMetrics.isBackPressured();
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
					update(metrics, MetricNames.IO_NUM_BYTES_IN,
						(String value) -> this.numBytesInComplete = false,
						(String value) -> this.numBytesIn += Long.valueOf(value)
					);

					update(metrics, MetricNames.IO_NUM_BYTES_OUT,
						(String value) -> this.numBytesOutComplete = false,
						(String value) -> this.numBytesOut += Long.valueOf(value)
					);

					update(metrics, MetricNames.IO_NUM_RECORDS_IN,
						(String value) -> this.numRecordsInComplete = false,
						(String value) -> this.numRecordsIn += Long.valueOf(value)
					);

					update(metrics, MetricNames.IO_NUM_RECORDS_OUT,
						(String value) -> this.numRecordsOutComplete = false,
						(String value) -> this.numRecordsOut += Long.valueOf(value)
					);

					update(metrics, MetricNames.USAGE_SHUFFLE_NETTY_INPUT_FLOATING_BUFFERS,
						(String value) -> this.usageInputFloatingBuffersComplete = false,
						(String value) -> this.usageInputFloatingBuffers += Float.valueOf(value)
					);

					update(metrics, MetricNames.USAGE_SHUFFLE_NETTY_INPUT_EXCLUSIVE_BUFFERS,
						(String value) -> this.usageInputExclusiveBuffersComplete = false,
						(String value) -> this.usageInputExclusiveBuffers += Float.valueOf(value)
					);

					update(metrics, MetricNames.USAGE_SHUFFLE_NETTY_OUTPUT_POOL_USAGE,
						(String value) -> this.usageOutPoolComplete = false,
						(String value) -> this.usageOutPool += Float.valueOf(value)
					);

					update(metrics, MetricNames.IS_BACKPRESSURED,
						(String value) -> this.isBackPressuredComplete = false,
						(String value) -> this.isBackPressured |= Boolean.valueOf(value)
					);
				}
				else {
					this.numBytesInComplete = false;
					this.numBytesOutComplete = false;
					this.numRecordsInComplete = false;
					this.numRecordsOutComplete = false;
					this.usageInputFloatingBuffersComplete = false;
					this.usageInputExclusiveBuffersComplete = false;
					this.usageOutPoolComplete = false;
					this.isBackPressuredComplete = false;
				}
			}
		}
	}

	private void update(MetricStore.ComponentMetricStore metrics, String metricKey, Consumer<String> emptyFunction, Consumer<String> noEmptyFunction) {
		String value = metrics.getMetric(metricKey);
		if (value == null){
			emptyFunction.accept(value);
		}
		else {
			noEmptyFunction.accept(value);
		}
	}
}
