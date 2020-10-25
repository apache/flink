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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the ResultSubpartition for a bounded result transferred
 * in a blocking manner: The result is first produced, then consumed.
 * The result can be consumed possibly multiple times.
 *
 * <p>Depending on the supplied implementation of {@link BoundedData}, the actual data is stored
 * for example in a file, or in a temporary memory mapped file.
 *
 * <h2>Important Notes on Thread Safety</h2>
 *
 * <p>This class does not synchronize every buffer access. It assumes the threading model of the
 * Flink network stack and is not thread-safe beyond that.
 *
 * <p>This class assumes a single writer thread that adds buffers, flushes, and finishes the write
 * phase. That same thread is also assumed to perform the partition release, if the release happens
 * during the write phase.
 *
 * <p>The implementation supports multiple concurrent readers, but assumes a single
 * thread per reader. That same thread must also release the reader. In particular, after the reader
 * was released, no buffers obtained from this reader may be accessed any more, or segmentation
 * faults might occur in some implementations.
 *
 * <p>The method calls to create readers, dispose readers, and dispose the partition are
 * thread-safe vis-a-vis each other.
 */
final class BoundedBlockingSubpartition extends ResultSubpartition {

	/** This lock guards the creation of readers and disposal of the memory mapped file. */
	private final Object lock = new Object();

	/** The current buffer, may be filled further over time. */
	@Nullable
	private BufferConsumer currentBuffer;

	/** The bounded data store that we store the data in. */
	private final BoundedData data;

	/** All created and not yet released readers. */
	@GuardedBy("lock")
	private final Set<BoundedBlockingSubpartitionReader> readers;

	/** Counter for the number of data buffers (not events!) written. */
	private int numDataBuffersWritten;

	/** The counter for the number of data buffers and events. */
	private int numBuffersAndEventsWritten;

	/** Flag indicating whether the writing has finished and this is now available for read. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private boolean isReleased;

	public BoundedBlockingSubpartition(
			int index,
			ResultPartition parent,
			BoundedData data) {

		super(index, parent);

		this.data = checkNotNull(data);
		this.readers = new HashSet<>();
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks if writing is finished.
	 * Readers cannot be created until writing is finished, and no further writes can happen after that.
	 */
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public boolean add(BufferConsumer bufferConsumer) throws IOException {
		if (isFinished()) {
			bufferConsumer.close();
			return false;
		}

		flushCurrentBuffer();
		currentBuffer = bufferConsumer;
		return true;
	}

	@Override
	public void flush() {
		// unfortunately, the signature of flush does not allow for any exceptions, so we
		// need to do this discouraged pattern of runtime exception wrapping
		try {
			flushCurrentBuffer();
		}
		catch (IOException e) {
			throw new FlinkRuntimeException(e.getMessage(), e);
		}
	}

	private void flushCurrentBuffer() throws IOException {
		if (currentBuffer != null) {
			writeAndCloseBufferConsumer(currentBuffer);
			currentBuffer = null;
		}
	}

	private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
		try {
			final Buffer buffer = bufferConsumer.build();
			try {
				if (canBeCompressed(buffer)) {
					final Buffer compressedBuffer = parent.bufferCompressor.compressToIntermediateBuffer(buffer);
					data.writeBuffer(compressedBuffer);
					if (compressedBuffer != buffer) {
						compressedBuffer.recycleBuffer();
					}
				} else {
					data.writeBuffer(buffer);
				}

				numBuffersAndEventsWritten++;
				if (buffer.isBuffer()) {
					numDataBuffersWritten++;
				}
			}
			finally {
				buffer.recycleBuffer();
			}
		}
		finally {
			bufferConsumer.close();
		}
	}

	@Override
	public void finish() throws IOException {
		checkState(!isReleased, "data partition already released");
		checkState(!isFinished, "data partition already finished");

		isFinished = true;
		flushCurrentBuffer();
		writeAndCloseBufferConsumer(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false));
		data.finishWrite();
	}

	@Override
	public void release() throws IOException {
		synchronized (lock) {
			if (isReleased) {
				return;
			}

			isReleased = true;
			isFinished = true; // for fail fast writes

			checkReaderReferencesAndDispose();
		}
	}

	@Override
	public ResultSubpartitionView createReadView(BufferAvailabilityListener availability) throws IOException {
		synchronized (lock) {
			checkState(!isReleased, "data partition already released");
			checkState(isFinished, "writing of blocking partition not yet finished");

			final BoundedBlockingSubpartitionReader reader = new BoundedBlockingSubpartitionReader(
					this, data, numDataBuffersWritten, availability);
			readers.add(reader);
			return reader;
		}
	}

	void releaseReaderReference(BoundedBlockingSubpartitionReader reader) throws IOException {
		onConsumedSubpartition();

		synchronized (lock) {
			if (readers.remove(reader) && isReleased) {
				checkReaderReferencesAndDispose();
			}
		}
	}

	@GuardedBy("lock")
	private void checkReaderReferencesAndDispose() throws IOException {
		assert Thread.holdsLock(lock);

		// To avoid lingering memory mapped files (large resource footprint), we don't
		// wait for GC to unmap the files, but use a Netty utility to directly unmap the file.
		// To avoid segmentation faults, we need to wait until all readers have been released.

		if (readers.isEmpty()) {
			data.close();
		}
	}

	// ---------------------------- statistics --------------------------------

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return 0;
	}

	@Override
	protected long getTotalNumberOfBuffers() {
		return numBuffersAndEventsWritten;
	}

	@Override
	protected long getTotalNumberOfBytes() {
		return data.getSize();
	}

	int getBuffersInBacklog() {
		return numDataBuffersWritten;
	}

	// ---------------------------- factories --------------------------------

	/**
	 * Creates a BoundedBlockingSubpartition that simply stores the partition data in a file.
	 * Data is eagerly spilled (written to disk) and readers directly read from the file.
	 */
	public static BoundedBlockingSubpartition createWithFileChannel(
			int index, ResultPartition parent, File tempFile, int readBufferSize) throws IOException {

		final FileChannelBoundedData bd = FileChannelBoundedData.create(tempFile.toPath(), readBufferSize);
		return new BoundedBlockingSubpartition(index, parent, bd);
	}

	/**
	 * Creates a BoundedBlockingSubpartition that stores the partition data in memory mapped file.
	 * Data is written to and read from the mapped memory region. Disk spilling happens lazily, when the
	 * OS swaps out the pages from the memory mapped file.
	 */
	public static BoundedBlockingSubpartition createWithMemoryMappedFile(
			int index, ResultPartition parent, File tempFile) throws IOException {

		final MemoryMappedBoundedData bd = MemoryMappedBoundedData.create(tempFile.toPath());
		return new BoundedBlockingSubpartition(index, parent, bd);

	}

	/**
	 * Creates a BoundedBlockingSubpartition that stores the partition data in a file and
	 * memory maps that file for reading.
	 * Data is eagerly spilled (written to disk) and then mapped into memory. The main
	 * difference to the {@link #createWithMemoryMappedFile(int, ResultPartition, File)} variant
	 * is that no I/O is necessary when pages from the memory mapped file are evicted.
	 */
	public static BoundedBlockingSubpartition createWithFileAndMemoryMappedReader(
			int index, ResultPartition parent, File tempFile) throws IOException {

		final FileChannelMemoryMappedBoundedData bd = FileChannelMemoryMappedBoundedData.create(tempFile.toPath());
		return new BoundedBlockingSubpartition(index, parent, bd);
	}
}
