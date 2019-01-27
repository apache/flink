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

import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * IO metrics information.
 */
public final class IOMetricsInfo {

	private static final String FIELD_NAME_BYTES_READ = "read-bytes";

	private static final String FIELD_NAME_BYTES_READ_COMPLETE = "read-bytes-complete";

	private static final String FIELD_NAME_BYTES_WRITTEN = "write-bytes";

	private static final String FIELD_NAME_BYTES_WRITTEN_COMPLETE = "write-bytes-complete";

	private static final String FIELD_NAME_RECORDS_READ = "read-records";

	private static final String FIELD_NAME_RECORDS_READ_COMPLETE = "read-records-complete";

	private static final String FIELD_NAME_RECORDS_WRITTEN = "write-records";

	private static final String FIELD_NAME_RECORDS_WRITTEN_COMPLETE = "write-records-complete";

	private static final String FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX = "buffers-in-pool-usage-max";

	private static final String FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX_COMPLETE = "buffers-in-pool-usage-max-complete";

	private static final String FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX = "buffers-out-pool-usage-max";

	private static final String FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX_COMPLETE = "buffers-out-pool-usage-max-complete";

	private static final String FIELD_NAME_TPS = "tps";

	private static final String FIELD_NAME_TPS_COMPLETE = "tps-complete";

	private static final String FIELD_NAME_DELAY = "delay";

	private static final String FIELD_NAME_DELAY_COMPLETE = "delay-complete";

	@JsonProperty(FIELD_NAME_BYTES_READ)
	private final long bytesRead;

	@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE)
	private final boolean bytesReadComplete;

	@JsonProperty(FIELD_NAME_BYTES_WRITTEN)
	private final long bytesWritten;

	@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE)
	private final boolean bytesWrittenComplete;

	@JsonProperty(FIELD_NAME_RECORDS_READ)
	private final long recordsRead;

	@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE)
	private final boolean recordsReadComplete;

	@JsonProperty(FIELD_NAME_RECORDS_WRITTEN)
	private final long recordsWritten;

	@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE)
	private final boolean recordsWrittenComplete;

	@JsonProperty(FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX)
	private final float bufferInPoolUsageMax;

	@JsonProperty(FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX_COMPLETE)
	private final boolean bufferInPoolUsageMaxComplete;

	@JsonProperty(FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX)
	private final float bufferOutPoolUsageMax;

	@JsonProperty(FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX_COMPLETE)
	private final boolean bufferOutPoolUsageMaxComplete;

	@JsonProperty(FIELD_NAME_TPS)
	private final double tps;

	@JsonProperty(FIELD_NAME_TPS_COMPLETE)
	private final boolean tpsComplete;

	@JsonProperty(FIELD_NAME_DELAY)
	private final long delay;

	@JsonProperty(FIELD_NAME_DELAY_COMPLETE)
	private final boolean delayComplete;

	@JsonCreator
	public IOMetricsInfo(
			@JsonProperty(FIELD_NAME_BYTES_READ) long bytesRead,
			@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE) boolean bytesReadComplete,
			@JsonProperty(FIELD_NAME_BYTES_WRITTEN) long bytesWritten,
			@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE) boolean bytesWrittenComplete,
			@JsonProperty(FIELD_NAME_RECORDS_READ) long recordsRead,
			@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE) boolean recordsReadComplete,
			@JsonProperty(FIELD_NAME_RECORDS_WRITTEN) long recordsWritten,
			@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE) boolean recordsWrittenComplete,
			@JsonProperty(FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX) float bufferInPoolUsageMax,
			@JsonProperty(FIELD_NAME_BUFFERS_IN_POOL_USAGE_MAX_COMPLETE) boolean bufferInPoolUsageMaxComplete,
			@JsonProperty(FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX) float bufferOutPoolUsageMax,
			@JsonProperty(FIELD_NAME_BUFFERS_OUT_POOL_USAGE_MAX_COMPLETE) boolean bufferOutPoolUsageMaxComplete,
			@JsonProperty(FIELD_NAME_TPS) double tps,
			@JsonProperty(FIELD_NAME_TPS_COMPLETE) boolean tpsComplete,
			@JsonProperty(FIELD_NAME_DELAY) long delay,
			@JsonProperty(FIELD_NAME_DELAY_COMPLETE) boolean delayComplete) {
		this.bytesRead = bytesRead;
		this.bytesReadComplete = bytesReadComplete;
		this.bytesWritten = bytesWritten;
		this.bytesWrittenComplete = bytesWrittenComplete;
		this.recordsRead = recordsRead;
		this.recordsReadComplete = recordsReadComplete;
		this.recordsWritten = recordsWritten;
		this.recordsWrittenComplete = recordsWrittenComplete;
		this.bufferInPoolUsageMax = bufferInPoolUsageMax;
		this.bufferInPoolUsageMaxComplete = bufferInPoolUsageMaxComplete;
		this.bufferOutPoolUsageMax = bufferOutPoolUsageMax;
		this.bufferOutPoolUsageMaxComplete = bufferOutPoolUsageMaxComplete;
		this.tps = tps;
		this.tpsComplete = tpsComplete;
		this.delay = delay;
		this.delayComplete = delayComplete;
	}

	public IOMetricsInfo(MutableIOMetrics counts) {
		this.bytesRead = counts.getNumBytesInLocal() + counts.getNumBytesInRemote();
		this.bytesReadComplete = counts.isNumBytesInLocalComplete() && counts.isNumBytesInRemoteComplete();
		this.bytesWritten = counts.getNumBytesOut();
		this.bytesWrittenComplete = counts.isNumBytesOutComplete();
		this.recordsRead = counts.getNumRecordsIn();
		this.recordsReadComplete = counts.isNumRecordsInComplete();
		this.recordsWritten = counts.getNumRecordsOut();
		this.recordsWrittenComplete = counts.isNumRecordsOutComplete();
		this.bufferInPoolUsageMax = counts.getBufferInPoolUsageMax();
		this.bufferInPoolUsageMaxComplete = counts.isBufferInPoolUsageMaxComplete();
		this.bufferOutPoolUsageMax = counts.getBufferOutPoolUsageMax();
		this.bufferOutPoolUsageMaxComplete = counts.isBufferOutPoolUsageMaxComplete();
		this.tps = counts.getTps();
		this.tpsComplete = counts.isTpsComplete();
		this.delay = counts.getDelay();
		this.delayComplete = counts.isDelayComplete();
	}

	public long getBytesRead() {
		return bytesRead;
	}

	public boolean isBytesReadComplete() {
		return bytesReadComplete;
	}

	public long getBytesWritten() {
		return bytesWritten;
	}

	public boolean isBytesWrittenComplete() {
		return bytesWrittenComplete;
	}

	public long getRecordsRead() {
		return recordsRead;
	}

	public boolean isRecordsReadComplete() {
		return recordsReadComplete;
	}

	public long getRecordsWritten() {
		return recordsWritten;
	}

	public boolean isRecordsWrittenComplete() {
		return recordsWrittenComplete;
	}

	public float getBufferInPoolUsageMax() {
		return bufferInPoolUsageMax;
	}

	public boolean isBufferInPoolUsageMaxComplete() {
		return bufferInPoolUsageMaxComplete;
	}

	public float getBufferOutPoolUsageMax() {
		return bufferOutPoolUsageMax;
	}

	public boolean isBufferOutPoolUsageMaxComplete() {
		return bufferOutPoolUsageMaxComplete;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		IOMetricsInfo that = (IOMetricsInfo) o;
		return bytesRead == that.bytesRead &&
			bytesReadComplete == that.bytesReadComplete &&
			bytesWritten == that.bytesWritten &&
			bytesWrittenComplete == that.bytesWrittenComplete &&
			recordsRead == that.recordsRead &&
			recordsReadComplete == that.recordsReadComplete &&
			recordsWritten == that.recordsWritten &&
			recordsWrittenComplete == that.recordsWrittenComplete &&
			bufferInPoolUsageMax == that.bufferInPoolUsageMax &&
			bufferInPoolUsageMaxComplete == that.bufferInPoolUsageMaxComplete &&
			bufferOutPoolUsageMax == that.bufferOutPoolUsageMax &&
			bufferOutPoolUsageMaxComplete == that.bufferOutPoolUsageMaxComplete &&
			tps == that.tps &&
			tpsComplete == that.tpsComplete &&
			delay == that.delay &&
			delayComplete == that.delayComplete;
	}

	@Override
	public int hashCode() {
		return Objects.hash(bytesRead, bytesReadComplete, bytesWritten, bytesWrittenComplete, recordsRead,
			recordsReadComplete, recordsWritten, recordsWrittenComplete, bufferInPoolUsageMax,
			bufferInPoolUsageMaxComplete, bufferOutPoolUsageMax, bufferOutPoolUsageMaxComplete,
			tps, tpsComplete, delay, delayComplete);
	}
}
