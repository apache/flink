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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Subpartition data reader for {@link SortMergeResultPartition}.
 */
public class SortMergeSubpartitionReader implements ResultSubpartitionView, BufferRecycler {

	private static final int NUM_READ_BUFFERS = 2;

	/** Target {@link SortMergeResultPartition} to read data from. */
	private final SortMergeResultPartition partition;

	/** Listener to notify when data is available. */
	private final BufferAvailabilityListener availabilityListener;

	/** Unmanaged memory used as read buffers. */
	private final Queue<MemorySegment> readBuffers = new ArrayDeque<>();

	/** Buffers read by the file reader. */
	private final Queue<Buffer> buffersRead = new ArrayDeque<>();

	/** File reader used to read buffer from. */
	private final PartitionedFileReader fileReader;

	/** Number of remaining non-event buffers to read. */
	private int dataBufferBacklog;

	/** Whether this reader is released or not. */
	private boolean isReleased;

	/** Sequence number of the next buffer to be sent to the consumer. */
	private int sequenceNumber;

	public SortMergeSubpartitionReader(
			int subpartitionIndex,
			int dataBufferBacklog,
			int bufferSize,
			SortMergeResultPartition partition,
			BufferAvailabilityListener listener,
			PartitionedFile partitionedFile) throws IOException {
		this.partition = checkNotNull(partition);
		this.availabilityListener = checkNotNull(listener);
		this.dataBufferBacklog = dataBufferBacklog;

		// allocate two pieces of unmanaged segments for data reading
		for (int i = 0; i < NUM_READ_BUFFERS; i++) {
			this.readBuffers.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize, null));
		}

		this.fileReader = new PartitionedFileReader(partitionedFile, subpartitionIndex);
		try {
			readBuffers();
		} catch (Throwable throwable) {
			// ensure that the file reader is closed when any exception occurs
			IOUtils.closeQuietly(fileReader);
			throw throwable;
		}
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() {
		checkState(!isReleased, "Reader is already released.");

		Buffer buffer = buffersRead.poll();
		if (buffer == null) {
			return null;
		}

		if (buffer.isBuffer()) {
			--dataBufferBacklog;
		}

		final Buffer lookAhead = buffersRead.peek();

		return BufferAndBacklog.fromBufferAndLookahead(
				buffer,
				lookAhead == null ? Buffer.DataType.NONE : lookAhead.getDataType(),
				dataBufferBacklog,
				sequenceNumber++);
	}

	void readBuffers() throws IOException {
		// we do not need to recycle the allocated segment here if any exception occurs
		// for this subpartition reader will be released so no resource will be leaked
		MemorySegment segment;
		while ((segment = readBuffers.poll()) != null) {
			Buffer buffer = fileReader.readBuffer(segment, this);
			if (buffer == null) {
				readBuffers.add(segment);
				break;
			}
			buffersRead.add(buffer);
		}
	}

	@Override
	public void notifyDataAvailable() {
		if (!buffersRead.isEmpty()) {
			availabilityListener.notifyDataAvailable();
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		if (!isReleased) {
			readBuffers.add(segment);
		}

		// notify data available if the reader is unavailable currently
		if (!isReleased && readBuffers.size() == NUM_READ_BUFFERS) {
			try {
				readBuffers();
			} catch (IOException exception) {
				ExceptionUtils.rethrow(exception, "Failed to read next buffer.");
			}
			notifyDataAvailable();
		}
	}

	@Override
	public void releaseAllResources() {
		isReleased = true;

		buffersRead.clear();
		readBuffers.clear();

		IOUtils.closeQuietly(fileReader);
		partition.releaseReader(this);
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public void resumeConsumption() {
		throw new UnsupportedOperationException("Method should never be called.");
	}

	@Override
	public Throwable getFailureCause() {
		// we can never throw an error after this was created
		return null;
	}

	@Override
	public boolean isAvailable(int numCreditsAvailable) {
		if (numCreditsAvailable > 0) {
			return !buffersRead.isEmpty();
		}

		return !buffersRead.isEmpty() && !buffersRead.peek().isBuffer();
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}
}
