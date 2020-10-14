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
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A collection of simple metrics, around the triggering of a checkpoint.
 */
public class CheckpointMetrics implements Serializable {

	private static final long serialVersionUID = 1L;

	private final long bytesProcessedDuringAlignment;

	private final long bytesPersistedDuringAlignment;

	/** The duration (in nanoseconds) that the stream alignment for the checkpoint took. */
	private final long alignmentDurationNanos;

	/** The duration (in milliseconds) of the synchronous part of the operator checkpoint. */
	private final long syncDurationMillis;

	/** The duration (in milliseconds) of the asynchronous part of the operator checkpoint.  */
	private final long asyncDurationMillis;
	private final long checkpointStartDelayNanos;

	public CheckpointMetrics() {
		this(-1L, -1L, -1L, -1L, -1L, -1L);
	}

	public CheckpointMetrics(
			long bytesProcessedDuringAlignment,
			long bytesPersistedDuringAlignment,
			long alignmentDurationNanos,
			long syncDurationMillis,
			long asyncDurationMillis,
			long checkpointStartDelayNanos) {

		// these may be "-1", in case the values are unknown or not set
		checkArgument(bytesProcessedDuringAlignment >= -1);
		checkArgument(bytesPersistedDuringAlignment >= -1);
		checkArgument(syncDurationMillis >= -1);
		checkArgument(asyncDurationMillis >= -1);
		checkArgument(alignmentDurationNanos >= -1);
		checkArgument(checkpointStartDelayNanos >= -1);

		this.bytesProcessedDuringAlignment = bytesProcessedDuringAlignment;
		this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
		this.alignmentDurationNanos = alignmentDurationNanos;
		this.syncDurationMillis = syncDurationMillis;
		this.asyncDurationMillis = asyncDurationMillis;
		this.checkpointStartDelayNanos = checkpointStartDelayNanos;
	}

	public long getBytesProcessedDuringAlignment() {
		return bytesProcessedDuringAlignment;
	}

	public long getBytesPersistedDuringAlignment() {
		return bytesPersistedDuringAlignment;
	}

	public long getAlignmentDurationNanos() {
		return alignmentDurationNanos;
	}

	public long getSyncDurationMillis() {
		return syncDurationMillis;
	}

	public long getAsyncDurationMillis() {
		return asyncDurationMillis;
	}

	public long getCheckpointStartDelayNanos() {
		return checkpointStartDelayNanos;
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

		return bytesProcessedDuringAlignment == that.bytesProcessedDuringAlignment &&
			bytesPersistedDuringAlignment == that.bytesPersistedDuringAlignment &&
			alignmentDurationNanos == that.alignmentDurationNanos &&
			syncDurationMillis == that.syncDurationMillis &&
			asyncDurationMillis == that.asyncDurationMillis &&
			checkpointStartDelayNanos == that.checkpointStartDelayNanos;

	}

	@Override
	public int hashCode() {
		return Objects.hash(
			bytesProcessedDuringAlignment,
			bytesPersistedDuringAlignment,
			alignmentDurationNanos,
			syncDurationMillis,
			asyncDurationMillis,
			checkpointStartDelayNanos);
	}

	@Override
	public String toString() {
		return "CheckpointMetrics{" +
			"bytesProcessedDuringAlignment=" + bytesProcessedDuringAlignment +
			", bytesPersistedDuringAlignment=" + bytesPersistedDuringAlignment +
			", alignmentDurationNanos=" + alignmentDurationNanos +
			", syncDurationMillis=" + syncDurationMillis +
			", asyncDurationMillis=" + asyncDurationMillis +
			", checkpointStartDelayNanos=" + checkpointStartDelayNanos +
			'}';
	}
}
