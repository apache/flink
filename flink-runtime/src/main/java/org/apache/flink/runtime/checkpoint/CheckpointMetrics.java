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

import java.io.Serializable;

/**
 * A collection of simple metrics, around the triggering of a checkpoint.
 */
public class CheckpointMetrics implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The number of bytes that were buffered during the checkpoint alignment phase */
	private long bytesBufferedInAlignment;

	/** The duration (in nanoseconds) that the stream alignment for the checkpoint took */
	private long alignmentDurationNanos;

	/* The duration (in milliseconds) of the synchronous part of the operator checkpoint */
	private long syncDurationMillis;

	/* The duration (in milliseconds) of the asynchronous part of the operator checkpoint  */
	private long asyncDurationMillis;

	public CheckpointMetrics() {
		this(-1L, -1L, -1L, -1L);
	}

	public CheckpointMetrics(
			long bytesBufferedInAlignment,
			long alignmentDurationNanos,
			long syncDurationMillis,
			long asyncDurationMillis) {

		this.bytesBufferedInAlignment = bytesBufferedInAlignment;
		this.alignmentDurationNanos = alignmentDurationNanos;
		this.syncDurationMillis = syncDurationMillis;
		this.asyncDurationMillis = asyncDurationMillis;
	}

	public long getBytesBufferedInAlignment() {
		return bytesBufferedInAlignment;
	}

	public void setBytesBufferedInAlignment(long bytesBufferedInAlignment) {
		this.bytesBufferedInAlignment = bytesBufferedInAlignment;
	}

	public long getAlignmentDurationNanos() {
		return alignmentDurationNanos;
	}

	public void setAlignmentDurationNanos(long alignmentDurationNanos) {
		this.alignmentDurationNanos = alignmentDurationNanos;
	}

	public long getSyncDurationMillis() {
		return syncDurationMillis;
	}

	public void setSyncDurationMillis(long syncDurationMillis) {
		this.syncDurationMillis = syncDurationMillis;
	}

	public long getAsyncDurationMillis() {
		return asyncDurationMillis;
	}

	public void setAsyncDurationMillis(long asyncDurationMillis) {
		this.asyncDurationMillis = asyncDurationMillis;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointMetrics that = (CheckpointMetrics) o;

		if (bytesBufferedInAlignment != that.bytesBufferedInAlignment) {
			return false;
		}
		if (alignmentDurationNanos != that.alignmentDurationNanos) {
			return false;
		}
		if (syncDurationMillis != that.syncDurationMillis) {
			return false;
		}
		return asyncDurationMillis == that.asyncDurationMillis;

	}

	@Override
	public int hashCode() {
		int result = (int) (bytesBufferedInAlignment ^ (bytesBufferedInAlignment >>> 32));
		result = 31 * result + (int) (alignmentDurationNanos ^ (alignmentDurationNanos >>> 32));
		result = 31 * result + (int) (syncDurationMillis ^ (syncDurationMillis >>> 32));
		result = 31 * result + (int) (asyncDurationMillis ^ (asyncDurationMillis >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "CheckpointMetrics{" +
				"bytesBufferedInAlignment=" + bytesBufferedInAlignment +
				", alignmentDurationNanos=" + alignmentDurationNanos +
				", syncDurationMillis=" + syncDurationMillis +
				", asyncDurationMillis=" + asyncDurationMillis +
				'}';
	}
}