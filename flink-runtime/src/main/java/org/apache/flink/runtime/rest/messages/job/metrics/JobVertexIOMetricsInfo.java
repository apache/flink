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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * JobVertex IO metrics information.
 */
public final class JobVertexIOMetricsInfo extends IOMetricsInfo {

	private static final String FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG = "input-exclusive-buffers-usage-avg";

	private static final String FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG_COMPLETE = "input-exclusive-buffers-usage-avg-complete";

	private static final String FIELD_NAME_INPUT_FLOATING_BUFFERS_AVG_USAGE = "input-floating-buffers-usage-avg";

	private static final String FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_AVG_COMPLETE = "input-floating-buffers-usage-avg-complete";

	private static final String FIELD_NAME_OUT_POOL_USAGE_AVG = "out-pool-usage-avg";

	private static final String FIELD_NAME_OUT_POOL_USAGE_AVG_COMPLETE = "out-pool-usage-avg-complete";

	@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG)
	private final float inputExclusiveBuffersUsageAvg;

	@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG_COMPLETE)
	private final boolean inputExclusiveBuffersUsageAvgComplete;

	@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_AVG_USAGE)
	private final float inputFloatingBuffersUsageAvg;

	@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_AVG_COMPLETE)
	private final boolean inputFloatingBuffersUsageAvgComplete;

	@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_AVG)
	private final float outPoolUsageAvg;

	@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_AVG_COMPLETE)
	private final boolean outPoolUsageAvgComplete;

	@JsonCreator
	public JobVertexIOMetricsInfo(
		@JsonProperty(FIELD_NAME_BYTES_READ) long bytesRead,
		@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE) boolean bytesReadComplete,
		@JsonProperty(FIELD_NAME_BYTES_WRITTEN) long bytesWritten,
		@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE) boolean bytesWrittenComplete,
		@JsonProperty(FIELD_NAME_RECORDS_READ) long recordsRead,
		@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE) boolean recordsReadComplete,
		@JsonProperty(FIELD_NAME_RECORDS_WRITTEN) long recordsWritten,
		@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE) boolean recordsWrittenComplete,
		@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG) float inputExclusiveBuffersUsageAvg,
		@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_AVG_COMPLETE) boolean inputExclusiveBuffersUsageAvgComplete,
		@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_AVG_USAGE) float inputFloatingBuffersUsageAvg,
		@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_AVG_COMPLETE) boolean inputFloatingBuffersUsageAvgComplete,
		@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_AVG) float outPoolUsageAvg,
		@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_AVG_COMPLETE) boolean outPoolUsageAvgComplete,
		@JsonProperty(FIELD_NAME_IS_BACKPRESSED) boolean isBackPressured,
		@JsonProperty(FIELD_NAME_IS_BACKPRESSED_COMPLETE) boolean isBackPressuredComplete) {
		super(bytesRead, bytesReadComplete, bytesWritten, bytesWrittenComplete, recordsRead, recordsReadComplete,
			recordsWritten, recordsWrittenComplete, isBackPressured, isBackPressuredComplete);
		this.outPoolUsageAvg = outPoolUsageAvg;
		this.outPoolUsageAvgComplete = outPoolUsageAvgComplete;
		this.inputExclusiveBuffersUsageAvg = inputExclusiveBuffersUsageAvg;
		this.inputExclusiveBuffersUsageAvgComplete = inputExclusiveBuffersUsageAvgComplete;
		this.inputFloatingBuffersUsageAvg = inputFloatingBuffersUsageAvg;
		this.inputFloatingBuffersUsageAvgComplete = inputFloatingBuffersUsageAvgComplete;
	}

	@JsonIgnore
	public float getInputExclusiveBuffersUsageAvg() {
		return inputExclusiveBuffersUsageAvg;
	}

	@JsonIgnore
	public boolean isInputExclusiveBuffersUsageAvgComplete() {
		return inputExclusiveBuffersUsageAvgComplete;
	}

	@JsonIgnore
	public float getInputFloatingBuffersUsageAvg() {
		return inputFloatingBuffersUsageAvg;
	}

	@JsonIgnore
	public boolean isInputFloatingBuffersUsageAvgComplete() {
		return inputFloatingBuffersUsageAvgComplete;
	}

	@JsonIgnore
	public float getOutPoolUsageAvg() {
		return outPoolUsageAvg;
	}

	@JsonIgnore
	public boolean isOutPoolUsageAvgComplete() {
		return outPoolUsageAvgComplete;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobVertexIOMetricsInfo that = (JobVertexIOMetricsInfo) o;
		return this.getBytesRead() == that.getBytesRead() &&
			this.isBytesReadComplete() == that.isBytesReadComplete() &&
			this.getBytesWritten() == that.getBytesWritten() &&
			this.isBytesWrittenComplete() == that.isBytesWrittenComplete() &&
			this.getRecordsRead() == that.getRecordsRead() &&
			this.isRecordsReadComplete() == that.isRecordsReadComplete() &&
			this.getRecordsWritten() == that.getRecordsWritten() &&
			this.isRecordsWrittenComplete() == that.isRecordsWrittenComplete() &&
			this.getInputExclusiveBuffersUsageAvg() == that.getInputExclusiveBuffersUsageAvg() &&
			this.isInputExclusiveBuffersUsageAvgComplete() == that.isInputExclusiveBuffersUsageAvgComplete() &&
			this.getInputFloatingBuffersUsageAvg() == that.getInputFloatingBuffersUsageAvg() &&
			this.isInputFloatingBuffersUsageAvgComplete() == that.isInputFloatingBuffersUsageAvgComplete() &&
			this.getOutPoolUsageAvg() == that.getOutPoolUsageAvg() &&
			this.isOutPoolUsageAvgComplete() == that.isOutPoolUsageAvgComplete() &&
			this.isBackPressured() == that.isBackPressured() &&
			this.isBackPressuredComplete() == that.isBackPressuredComplete();
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getBytesRead(), this.isBytesReadComplete(), this.getBytesWritten(), this.isBytesWrittenComplete(),
			this.getRecordsRead(), this.isRecordsReadComplete(), this.getRecordsWritten(), this.isRecordsWrittenComplete(),
			this.getInputExclusiveBuffersUsageAvg(), this.isInputExclusiveBuffersUsageAvgComplete(), this.getInputFloatingBuffersUsageAvg(),
			this.isInputFloatingBuffersUsageAvgComplete(), this.getOutPoolUsageAvg(), this.isOutPoolUsageAvgComplete(),
			this.isBackPressured(), this.isBackPressuredComplete());
	}

}
