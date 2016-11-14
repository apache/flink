/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Encapsulates all the meta data for a checkpoint.
 */
public class CheckpointMetaData implements Serializable {

	private static final long serialVersionUID = -2387652345781312442L;

	/** The ID of the checkpoint */
	private final long checkpointId;

	/** The timestamp of the checkpoint */
	private final long timestamp;

	private final CheckpointMetrics metrics;

	public CheckpointMetaData(long checkpointId, long timestamp) {
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
		this.metrics = new CheckpointMetrics();
	}

	public CheckpointMetaData(
			long checkpointId,
			long timestamp,
			long synchronousDurationMillis,
			long asynchronousDurationMillis,
			long bytesBufferedInAlignment,
			long alignmentDurationNanos) {
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
		this.metrics = new CheckpointMetrics(
				bytesBufferedInAlignment,
				alignmentDurationNanos,
				synchronousDurationMillis,
				asynchronousDurationMillis);
	}

	public CheckpointMetaData(
			long checkpointId,
			long timestamp,
			CheckpointMetrics metrics) {
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
		this.metrics = Preconditions.checkNotNull(metrics);
	}

	public CheckpointMetrics getMetrics() {
		return metrics;
	}

	public CheckpointMetaData setBytesBufferedInAlignment(long bytesBufferedInAlignment) {
		Preconditions.checkArgument(bytesBufferedInAlignment >= 0);
		this.metrics.setBytesBufferedInAlignment(bytesBufferedInAlignment);
		return this;
	}

	public CheckpointMetaData setAlignmentDurationNanos(long alignmentDurationNanos) {
		Preconditions.checkArgument(alignmentDurationNanos >= 0);
		this.metrics.setAlignmentDurationNanos(alignmentDurationNanos);
		return this;
	}

	public CheckpointMetaData setSyncDurationMillis(long syncDurationMillis) {
		Preconditions.checkArgument(syncDurationMillis >= 0);
		this.metrics.setSyncDurationMillis(syncDurationMillis);
		return this;
	}

	public CheckpointMetaData setAsyncDurationMillis(long asyncDurationMillis) {
		Preconditions.checkArgument(asyncDurationMillis >= 0);
		this.metrics.setAsyncDurationMillis(asyncDurationMillis);
		return this;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getBytesBufferedInAlignment() {
		return metrics.getBytesBufferedInAlignment();
	}

	public long getAlignmentDurationNanos() {
		return metrics.getAlignmentDurationNanos();
	}

	public long getSyncDurationMillis() {
		return metrics.getSyncDurationMillis();
	}

	public long getAsyncDurationMillis() {
		return metrics.getAsyncDurationMillis();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointMetaData that = (CheckpointMetaData) o;

		return (checkpointId == that.checkpointId)
				&& (timestamp == that.timestamp)
				&& (metrics.equals(that.metrics));
	}

	@Override
	public int hashCode() {
		int result = (int) (checkpointId ^ (checkpointId >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + metrics.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "CheckpointMetaData{" +
				"checkpointId=" + checkpointId +
				", timestamp=" + timestamp +
				", metrics=" + metrics +
				'}';
	}
}