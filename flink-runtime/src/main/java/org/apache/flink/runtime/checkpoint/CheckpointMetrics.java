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

import static org.apache.flink.util.Preconditions.checkArgument;

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

		// these may be "-1", in case the values are unknown or not set
		checkArgument(syncDurationMillis >= -1);
		checkArgument(asyncDurationMillis >= -1);
		checkArgument(bytesBufferedInAlignment >= -1);
		checkArgument(alignmentDurationNanos >= -1);

		this.bytesBufferedInAlignment = bytesBufferedInAlignment;
		this.alignmentDurationNanos = alignmentDurationNanos;
		this.syncDurationMillis = syncDurationMillis;
		this.asyncDurationMillis = asyncDurationMillis;
	}

	public long getBytesBufferedInAlignment() {
		return bytesBufferedInAlignment;
	}

	public CheckpointMetrics setBytesBufferedInAlignment(long bytesBufferedInAlignment) {
		this.bytesBufferedInAlignment = bytesBufferedInAlignment;
		return this;
	}

	public long getAlignmentDurationNanos() {
		return alignmentDurationNanos;
	}

	public CheckpointMetrics setAlignmentDurationNanos(long alignmentDurationNanos) {
		this.alignmentDurationNanos = alignmentDurationNanos;
		return this;
	}

	public long getSyncDurationMillis() {
		return syncDurationMillis;
	}

	public CheckpointMetrics setSyncDurationMillis(long syncDurationMillis) {
		this.syncDurationMillis = syncDurationMillis;
		return this;
	}

	public long getAsyncDurationMillis() {
		return asyncDurationMillis;
	}

	public CheckpointMetrics setAsyncDurationMillis(long asyncDurationMillis) {
		this.asyncDurationMillis = asyncDurationMillis;
		return this;
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

		return bytesBufferedInAlignment == that.bytesBufferedInAlignment && 
				alignmentDurationNanos == that.alignmentDurationNanos && 
				syncDurationMillis == that.syncDurationMillis && 
				asyncDurationMillis == that.asyncDurationMillis;

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
