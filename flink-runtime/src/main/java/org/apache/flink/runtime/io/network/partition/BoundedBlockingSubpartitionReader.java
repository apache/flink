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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.MemoryMappedBuffers.BufferSlicer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition.
 */
final class BoundedBlockingSubpartitionReader implements ResultSubpartitionView {

	/** The result subpartition that we read. */
	private final BoundedBlockingSubpartition parent;

	/** The next buffer (look ahead). Null once the data is depleted or reader is disposed. */
	@Nullable
	private Buffer nextBuffer;

	/** The reader/decoder to the memory mapped region with the data we currently read from.
	 * Null once the reader empty or disposed.*/
	@Nullable
	private BufferSlicer memory;

	/** The remaining number of data buffers (not events) in the result. */
	private int dataBufferBacklog;

	/** Flag whether this reader is released. Atomic, to avoid double release. */
	private boolean isReleased;

	/**
	 * Convenience constructor that takes a single buffer.
	 */
	BoundedBlockingSubpartitionReader(
			BoundedBlockingSubpartition parent,
			BufferSlicer memory,
			int numDataBuffers) {

		checkArgument(numDataBuffers >= 0);

		this.parent = checkNotNull(parent);
		this.memory = checkNotNull(memory);
		this.dataBufferBacklog = numDataBuffers;

		this.nextBuffer = memory.sliceNextBuffer();
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException {
		final Buffer current = nextBuffer; // copy reference to stack

		if (current == null) {
			// as per contract, we must return null when the reader is empty,
			// but also in case the reader is disposed (rather than throwing an exception)
			return null;
		}
		if (current.isBuffer()) {
			dataBufferBacklog--;
		}

		assert memory != null;
		nextBuffer = memory.sliceNextBuffer();

		return BufferAndBacklog.fromBufferAndLookahead(current, nextBuffer, dataBufferBacklog);
	}

	@Override
	public void notifyDataAvailable() {
		throw new IllegalStateException("No data should become available on a blocking partition during consumption.");
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		parent.onConsumedSubpartition();
	}

	@Override
	public void releaseAllResources() throws IOException {
		// it is not a problem if this method executes multiple times
		isReleased = true;

		// nulling these fields means thet read method and will fail fast
		nextBuffer = null;
		memory = null;

		// Notify the parent that this one is released. This allows the parent to
		// eventually release all resources (when all readers are done and the
		// parent is disposed).
		parent.releaseReaderReference(this);
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public boolean nextBufferIsEvent() {
		return nextBuffer != null && !nextBuffer.isBuffer();
	}

	@Override
	public boolean isAvailable() {
		return nextBuffer != null;
	}

	@Override
	public Throwable getFailureCause() {
		// we can never throw an error after this was created
		return null;
	}

	@Override
	public String toString() {
		return String.format("Blocking Subpartition Reader: ID=%s, index=%d",
				parent.parent.getPartitionId(),
				parent.index);
	}
}
