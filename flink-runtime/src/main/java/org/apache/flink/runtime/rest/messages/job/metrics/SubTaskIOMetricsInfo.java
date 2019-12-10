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
 * SubTask IO metrics information.
 */
public final class SubTaskIOMetricsInfo extends IOMetricsInfo {

	private static final String FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE = "input-exclusive-buffers-usage";

	private static final String FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_COMPLETE = "input-exclusive-buffers-usage-complete";

	private static final String FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE = "input-floating-buffers-usage";

	private static final String FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_COMPLETE = "input-floating-buffers-usage-complete";

	private static final String FIELD_NAME_OUT_POOL_USAGE = "out-pool-usage";

	private static final String FIELD_NAME_OUT_POOL_USAGE_COMPLETE = "out-pool-usage-complete";

	@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE)
	private final float inputExclusiveBuffersUsage;

	@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_COMPLETE)
	private final boolean inputExclusiveBuffersUsageComplete;

	@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE)
	private final float inputFloatingBuffersUsage;

	@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_COMPLETE)
	private final boolean inputFloatingBuffersUsageComplete;

	@JsonProperty(FIELD_NAME_OUT_POOL_USAGE)
	private final float outPoolUsage;

	@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_COMPLETE)
	private final boolean outPoolUsageComplete;

	@JsonCreator
	public SubTaskIOMetricsInfo(
		@JsonProperty(FIELD_NAME_BYTES_READ) long bytesRead,
		@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE) boolean bytesReadComplete,
		@JsonProperty(FIELD_NAME_BYTES_WRITTEN) long bytesWritten,
		@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE) boolean bytesWrittenComplete,
		@JsonProperty(FIELD_NAME_RECORDS_READ) long recordsRead,
		@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE) boolean recordsReadComplete,
		@JsonProperty(FIELD_NAME_RECORDS_WRITTEN) long recordsWritten,
		@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE) boolean recordsWrittenComplete,
		@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE) float inputExclusiveBuffersUsage,
		@JsonProperty(FIELD_NAME_INPUT_EXCLUSIVE_BUFFERS_USAGE_COMPLETE) boolean inputExclusiveBuffersUsageComplete,
		@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE) float inputFloatingBuffersUsage,
		@JsonProperty(FIELD_NAME_INPUT_FLOATING_BUFFERS_USAGE_COMPLETE) boolean inputFloatingBuffersUsageComplete,
		@JsonProperty(FIELD_NAME_OUT_POOL_USAGE) float outPoolUsage,
		@JsonProperty(FIELD_NAME_OUT_POOL_USAGE_COMPLETE) boolean outPoolUsageComplete,
		@JsonProperty(FIELD_NAME_IS_BACKPRESSED) boolean isBackPressured,
		@JsonProperty(FIELD_NAME_IS_BACKPRESSED_COMPLETE) boolean isBackPressuredComplete) {
		super(bytesRead, bytesReadComplete, bytesWritten, bytesWrittenComplete, recordsRead, recordsReadComplete,
			recordsWritten, recordsWrittenComplete, isBackPressured, isBackPressuredComplete);
		this.outPoolUsage = outPoolUsage;
		this.outPoolUsageComplete = outPoolUsageComplete;
		this.inputExclusiveBuffersUsage = inputExclusiveBuffersUsage;
		this.inputExclusiveBuffersUsageComplete = inputExclusiveBuffersUsageComplete;
		this.inputFloatingBuffersUsage = inputFloatingBuffersUsage;
		this.inputFloatingBuffersUsageComplete = inputFloatingBuffersUsageComplete;
	}

	@JsonIgnore
	public float getInputExclusiveBuffersUsage() {
		return inputExclusiveBuffersUsage;
	}

	@JsonIgnore
	public boolean isInputExclusiveBuffersUsageComplete() {
		return inputExclusiveBuffersUsageComplete;
	}

	@JsonIgnore
	public float getInputFloatingBuffersUsage() {
		return inputFloatingBuffersUsage;
	}

	@JsonIgnore
	public boolean isInputFloatingBuffersUsageComplete() {
		return inputFloatingBuffersUsageComplete;
	}

	@JsonIgnore
	public float getOutPoolUsage() {
		return outPoolUsage;
	}

	@JsonIgnore
	public boolean isOutPoolUsageComplete() {
		return outPoolUsageComplete;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SubTaskIOMetricsInfo that = (SubTaskIOMetricsInfo) o;
		return this.getBytesRead() == that.getBytesRead() &&
			this.isBytesReadComplete() == that.isBytesReadComplete() &&
			this.getBytesWritten() == that.getBytesWritten() &&
			this.isBytesWrittenComplete() == that.isBytesWrittenComplete() &&
			this.getRecordsRead() == that.getRecordsRead() &&
			this.isRecordsReadComplete() == that.isRecordsReadComplete() &&
			this.getRecordsWritten() == that.getRecordsWritten() &&
			this.isRecordsWrittenComplete() == that.isRecordsWrittenComplete() &&
			this.getInputExclusiveBuffersUsage() == that.getInputExclusiveBuffersUsage() &&
			this.isInputExclusiveBuffersUsageComplete() == that.isInputExclusiveBuffersUsageComplete() &&
			this.getInputFloatingBuffersUsage() == that.getInputFloatingBuffersUsage() &&
			this.isInputFloatingBuffersUsageComplete() == that.isInputFloatingBuffersUsageComplete() &&
			this.isBackPressured() == that.isBackPressured() &&
			that.isBackPressuredComplete() == that.isBackPressuredComplete() &&
			this.getOutPoolUsage() == that.getOutPoolUsage() &&
			this.isOutPoolUsageComplete() == that.isOutPoolUsageComplete();
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.getBytesRead(), this.isBytesReadComplete(), this.getBytesWritten(), this.isBytesWrittenComplete(),
			this.getRecordsRead(), this.isRecordsReadComplete(), this.getRecordsWritten(), this.isRecordsWrittenComplete(),
			this.getInputExclusiveBuffersUsage(), this.isInputExclusiveBuffersUsageComplete(), this.getInputFloatingBuffersUsage(),
			this.isInputFloatingBuffersUsageComplete(), this.getOutPoolUsage(), this.isOutPoolUsageComplete(),
			this.isBackPressured(), this.isBackPressuredComplete());
	}

}
