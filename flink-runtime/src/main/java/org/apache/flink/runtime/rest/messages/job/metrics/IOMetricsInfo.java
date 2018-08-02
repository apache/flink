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

	private static final String FIELD_NAME_BUFFERS_READ = "read-buffers";

	private static final String FIELD_NAME_BUFFERS_READ_COMPLETE = "read-buffers-complete";

	private static final String FIELD_NAME_BUFFERS_WRITTEN = "write-buffers";

	private static final String FIELD_NAME_BUFFERS_WRITTEN_COMPLETE = "write-buffers-complete";

	private static final String FIELD_NAME_RECORDS_READ = "read-records";

	private static final String FIELD_NAME_RECORDS_READ_COMPLETE = "read-records-complete";

	private static final String FIELD_NAME_RECORDS_WRITTEN = "write-records";

	private static final String FIELD_NAME_RECORDS_WRITTEN_COMPLETE = "write-records-complete";

	@JsonProperty(FIELD_NAME_BYTES_READ)
	private final long bytesRead;

	@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE)
	private final boolean bytesReadComplete;

	@JsonProperty(FIELD_NAME_BYTES_WRITTEN)
	private final long bytesWritten;

	@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE)
	private final boolean bytesWrittenComplete;

	@JsonProperty(FIELD_NAME_BUFFERS_READ)
	private final long buffersRead;

	@JsonProperty(FIELD_NAME_BUFFERS_READ_COMPLETE)
	private final boolean buffersReadComplete;

	@JsonProperty(FIELD_NAME_BUFFERS_WRITTEN)
	private final long buffersWritten;

	@JsonProperty(FIELD_NAME_BUFFERS_WRITTEN_COMPLETE)
	private final boolean buffersWrittenComplete;

	@JsonProperty(FIELD_NAME_RECORDS_READ)
	private final long recordsRead;

	@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE)
	private final boolean recordsReadComplete;

	@JsonProperty(FIELD_NAME_RECORDS_WRITTEN)
	private final long recordsWritten;

	@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE)
	private final boolean recordsWrittenComplete;

	@JsonCreator
	public IOMetricsInfo(
			@JsonProperty(FIELD_NAME_BYTES_READ) long bytesRead,
			@JsonProperty(FIELD_NAME_BYTES_READ_COMPLETE) boolean bytesReadComplete,
			@JsonProperty(FIELD_NAME_BYTES_WRITTEN) long bytesWritten,
			@JsonProperty(FIELD_NAME_BYTES_WRITTEN_COMPLETE) boolean bytesWrittenComplete,
			@JsonProperty(FIELD_NAME_BUFFERS_READ) long buffersRead,
			@JsonProperty(FIELD_NAME_BUFFERS_READ_COMPLETE) boolean buffersReadComplete,
			@JsonProperty(FIELD_NAME_BUFFERS_WRITTEN) long buffersWritten,
			@JsonProperty(FIELD_NAME_BUFFERS_WRITTEN_COMPLETE) boolean buffersWrittenComplete,
			@JsonProperty(FIELD_NAME_RECORDS_READ) long recordsRead,
			@JsonProperty(FIELD_NAME_RECORDS_READ_COMPLETE) boolean recordsReadComplete,
			@JsonProperty(FIELD_NAME_RECORDS_WRITTEN) long recordsWritten,
			@JsonProperty(FIELD_NAME_RECORDS_WRITTEN_COMPLETE) boolean recordsWrittenComplete) {
		this.bytesRead = bytesRead;
		this.bytesReadComplete = bytesReadComplete;
		this.bytesWritten = bytesWritten;
		this.bytesWrittenComplete = bytesWrittenComplete;

		this.buffersRead = buffersRead;
		this.buffersReadComplete = buffersReadComplete;
		this.buffersWritten = buffersWritten;
		this.buffersWrittenComplete = buffersWrittenComplete;

		this.recordsRead = recordsRead;
		this.recordsReadComplete = recordsReadComplete;
		this.recordsWritten = recordsWritten;
		this.recordsWrittenComplete = recordsWrittenComplete;
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

	public long getBuffersRead() {
		return buffersRead;
	}

	public boolean isBuffersReadComplete() {
		return buffersReadComplete;
	}

	public long getBuffersWritten() {
		return buffersWritten;
	}

	public boolean isBuffersWrittenComplete() {
		return buffersWrittenComplete;
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
			buffersRead == that.buffersRead &&
			buffersReadComplete == that.buffersReadComplete &&
			buffersWritten == that.buffersWritten &&
			buffersWrittenComplete == that.buffersWrittenComplete &&
			recordsRead == that.recordsRead &&
			recordsReadComplete == that.recordsReadComplete &&
			recordsWritten == that.recordsWritten &&
			recordsWrittenComplete == that.recordsWrittenComplete;
	}

	@Override
	public int hashCode() {
		return Objects.hash(bytesRead, bytesReadComplete, bytesWritten, bytesWrittenComplete, buffersRead, buffersReadComplete, buffersWritten, buffersWrittenComplete, recordsRead, recordsReadComplete, recordsWritten, recordsWrittenComplete);
	}
}
