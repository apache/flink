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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition.
 */
class BoundedBlockingSubpartitionReader implements ResultSubpartitionView {

	/** The result subpartition that we read. */
	private final BoundedBlockingSubpartition parent;

	/** Further byte buffers, to handle cases where there is more data than fits into
	 * one mapped byte buffer (2GB = Integer.MAX_VALUE). */
	private final Iterator<ByteBuffer> furtherData;

	/** The reader/decoder to the memory mapped region with the data we currently read from.
	 * Max 2GB large. Further regions may be in the {@link #furtherData} field.
	 * Null once the reader empty or disposed.*/
	@Nullable
	private BoundedBlockingSubpartitionMemory.Reader data;

	/** The next buffer (look ahead). Null once the data is depleted or reader is disposed. */
	@Nullable
	private Buffer nextBuffer;

	/** The remaining number of data buffers (not events) in the result. */
	private int bufferBacklog;

	/** Flag whether this reader is released. Atomic, to avoid double release. */
	private boolean isReleased;

	/**
	 * Convenience constructor that takes a single buffer.
	 */
	BoundedBlockingSubpartitionReader(
			BoundedBlockingSubpartition parent,
			ByteBuffer data,
			int numBuffers) throws IOException {
		this(parent, Collections.singleton(data), numBuffers);
	}

	/**
	 * The general constructor that takes a queue of buffers containing the data.
	 */
	BoundedBlockingSubpartitionReader(
			BoundedBlockingSubpartition parent,
			Iterable<ByteBuffer> data,
			int numBuffers) throws IOException {

		checkArgument(numBuffers >= 0);

		this.parent = checkNotNull(parent);
		this.bufferBacklog = numBuffers;

		this.furtherData = data.iterator();
		checkArgument(furtherData.hasNext());

		this.data = new BoundedBlockingSubpartitionMemory.Reader(furtherData.next());
		this.nextBuffer = fillNextBuffer();
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
			bufferBacklog--;
		}

		Buffer next = fillNextBuffer();
		nextBuffer = next;

		return new BufferAndBacklog(
				current,
				next != null,
				bufferBacklog,
				next != null && !next.isBuffer());
	}

	@Nullable
	private Buffer fillNextBuffer() throws IOException {
		// should only be null once empty or disposed, in which case this method
		// should not be called any more
		assert data != null;

		final Buffer next = data.sliceNextBuffer();
		if (next != null) {
			return next;
		}

		if (!furtherData.hasNext()) {
			data = null;
			return null;
		}

		data = new BoundedBlockingSubpartitionMemory.Reader(furtherData.next());
		return fillNextBuffer();
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
		// mark released and prevent double release
		if (isReleased) {
			return;
		}
		isReleased = true;

		// nulling these fields means thet read method and will fail fast
		nextBuffer = null;
		data = null;

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
