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
 * IO metrics information.
 */
public class IOMetricsInfo {

	protected static final String FIELD_NAME_BYTES_READ = "read-bytes";

	protected static final String FIELD_NAME_BYTES_READ_COMPLETE = "read-bytes-complete";

	protected static final String FIELD_NAME_BYTES_WRITTEN = "write-bytes";

	protected static final String FIELD_NAME_BYTES_WRITTEN_COMPLETE = "write-bytes-complete";

	protected static final String FIELD_NAME_RECORDS_READ = "read-records";

	protected static final String FIELD_NAME_RECORDS_READ_COMPLETE = "read-records-complete";

	protected static final String FIELD_NAME_RECORDS_WRITTEN = "write-records";

	protected static final String FIELD_NAME_RECORDS_WRITTEN_COMPLETE = "write-records-complete";

	protected static final String FIELD_NAME_IS_BACKPRESSED = "is-backpressed";

	protected static final String FIELD_NAME_IS_BACKPRESSED_COMPLETE = "is-backpressed-complete";

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

	@JsonProperty(FIELD_NAME_IS_BACKPRESSED)
	private final boolean isBackPressured;

	@JsonProperty(FIELD_NAME_IS_BACKPRESSED_COMPLETE)
	private final boolean isBackPressuredComplete;

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
			@JsonProperty(FIELD_NAME_IS_BACKPRESSED) boolean isBackPressured,
			@JsonProperty(FIELD_NAME_IS_BACKPRESSED_COMPLETE) boolean isBackPressuredComplete) {
		this.bytesRead = bytesRead;
		this.bytesReadComplete = bytesReadComplete;
		this.bytesWritten = bytesWritten;
		this.bytesWrittenComplete = bytesWrittenComplete;
		this.recordsRead = recordsRead;
		this.recordsReadComplete = recordsReadComplete;
		this.recordsWritten = recordsWritten;
		this.recordsWrittenComplete = recordsWrittenComplete;
		this.isBackPressured = isBackPressured;
		this.isBackPressuredComplete = isBackPressuredComplete;
	}

	@JsonIgnore
	public long getBytesRead() {
		return bytesRead;
	}

	@JsonIgnore
	public boolean isBytesReadComplete() {
		return bytesReadComplete;
	}

	@JsonIgnore
	public long getBytesWritten() {
		return bytesWritten;
	}

	@JsonIgnore
	public boolean isBytesWrittenComplete() {
		return bytesWrittenComplete;
	}

	@JsonIgnore
	public long getRecordsRead() {
		return recordsRead;
	}

	@JsonIgnore
	public boolean isRecordsReadComplete() {
		return recordsReadComplete;
	}

	@JsonIgnore
	public long getRecordsWritten() {
		return recordsWritten;
	}

	@JsonIgnore
	public boolean isRecordsWrittenComplete() {
		return recordsWrittenComplete;
	}

	@JsonIgnore
	public boolean isBackPressured() {
		return isBackPressured;
	}

	@JsonIgnore
	public boolean isBackPressuredComplete() {
		return isBackPressuredComplete;
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
			isBackPressured == that.isBackPressured &&
			isBackPressuredComplete == that.isBackPressuredComplete;
	}

	@Override
	public int hashCode() {
		return Objects.hash(bytesRead, bytesReadComplete, bytesWritten, bytesWrittenComplete, recordsRead,
			recordsReadComplete, recordsWritten, recordsWrittenComplete, isBackPressured, isBackPressuredComplete);
	}
}
