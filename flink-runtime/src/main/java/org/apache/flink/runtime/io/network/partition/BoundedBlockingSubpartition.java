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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionMemory.Writer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.netty4.io.netty.util.internal.PlatformDependent;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the ResultSubpartition for a bounded result transferred
 * in a blocking manner: The result is first produced, then consumed.
 * The result can be consumed possibly multiple times.
 *
 * <p>The implementation creates a temporary memory mapped file and writes all buffers to that
 * memory and serves the result from that memory. The kernel backs the mapped memory region
 * with physical memory and file space incrementally as new pages are filled.
 */
class BoundedBlockingSubpartition extends ResultSubpartition {

	/** This lock guards the creation of readers and disposal of the memory mapped file. */
	private final Object lock = new Object();

	/** The current buffer, may be filled further over time. */
	@Nullable
	private BufferConsumer currentBuffer;

	/** The memory that we store the data in, via a memory mapped file. */
	private final MemoryMappedBuffers memory;

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

	/**
	 * Common constructor.
	 */
	public BoundedBlockingSubpartition(
			int index,
			ResultPartition parent,
			Path filePath) throws IOException {
		this(index, parent, filePath, Integer.MAX_VALUE);
	}

	/**
	 * Constructor for testing. By default regions are rolled over at 2GB (max size of direct buffers
	 * in Java). This constructor allows tests to pass in a smaller threshold to test rolling over
	 * without having to actually produce more than 2GB during testing.
	 */
	@VisibleForTesting
	BoundedBlockingSubpartition(
			int index,
			ResultPartition parent,
			Path filePath,
			int maxMMapRegionSize) throws IOException {

		super(index, parent);

		final FileChannel fc = FileChannel.open(filePath,
				StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

		this.memory = new MemoryMappedBuffers(filePath, fc, maxMMapRegionSize);
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
				memory.writeBuffer(buffer);

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
		synchronized (lock) {
			checkState(!isReleased, "data partition already released");
			checkState(!isFinished, "data partition already finished");

			isFinished = true;
			flushCurrentBuffer();
			writeAndCloseBufferConsumer(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE));
			memory.finishWrite();
		}
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

			availability.notifyDataAvailable();

			final List<ByteBuffer> allBuffers = memory.getFullBuffers();
			final BoundedBlockingSubpartitionReader reader = new BoundedBlockingSubpartitionReader(
					this, allBuffers, numDataBuffersWritten);
			readers.add(reader);
			return reader;
		}
	}

	void releaseReaderReference(BoundedBlockingSubpartitionReader reader) throws IOException {
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
			memory.unmapAndDispose();
		}
	}

	// ------------------------------ legacy ----------------------------------

	@Override
	public int releaseMemory() throws IOException {
		return 0;
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
		return memory.getSize();
	}

	int getBuffersInBacklog() {
		return numDataBuffersWritten;
	}

	// ------------------------------------------------------------------------

	/**
	 * This class is largely a workaround for the fact that a memory mapped region in Java can cannot
	 * be larger than 2GB (== signed 32 bit int max value).
	 *
	 * <p>The class takes {@link Buffer}s and writes them to several memory mapped region, using the
	 * {@link BoundedBlockingSubpartitionMemory} class.
	 *
	 * <p>This class performs absolutely no synchronization and relies on single threaded
	 * or externally synchronized access.
	 */
	private static final class MemoryMappedBuffers {

		/** Memory mappings should be at the granularity of page sizes, for efficiency. */
		private static final int PAGE_SIZE = PageSizeUtil.getSystemPageSizeOrConservativeMultiple();

		/** The encoder to the current memory mapped region we are writing to. */
		private BoundedBlockingSubpartitionMemory.Writer currentBuffer;

		/** All memory mapped regions that are already full (completed). */
		private final ArrayList<ByteBuffer> fullBuffers;

		/** The file channel backing the memory mapped file. */
		private final FileChannel file;

		/** The path of the memory mapped file. */
		private final Path filePath;

		/** The offset where the next mapped region should start. */
		private long nextMappingOffset;

		/** The size of each mapped region. */
		private final long mappingSize;

		MemoryMappedBuffers(Path filePath, FileChannel file, int maxSizePerByteBuffer) throws IOException {
			this.filePath = filePath;
			this.file = file;
			this.mappingSize = alignSize(maxSizePerByteBuffer);
			this.fullBuffers = new ArrayList<>(4);

			rollOverToNextBuffer();
		}

		void writeBuffer(Buffer buffer) throws IOException {
			if (currentBuffer.writeBuffer(buffer)) {
				return;
			}

			rollOverToNextBuffer();

			if (!currentBuffer.writeBuffer(buffer)) {
				throwTooLargeBuffer(buffer);
			}
		}

		List<ByteBuffer> getFullBuffers() {
			return fullBuffers.stream()
					.map(ByteBuffer::slice)
					.collect(Collectors.toList());
		}

		/**
		 * Finishes the current region and prevents further writes.
		 */
		void finishWrite() throws IOException {
			fullBuffers.add(currentBuffer.complete());
			currentBuffer = null; // fail further writes fast
			file.close(); // won't map further regions from now on
		}

		/**
		 * Unmaps the file from memory and deletes the file.
		 * After calling this method, access to any ByteBuffer obtained from this instance
		 * will cause a segmentation fault.
		 */
		void unmapAndDispose() throws IOException {
			IOUtils.closeQuietly(file); // in case we dispose before finishing writes

			for (ByteBuffer bb : fullBuffers) {
				PlatformDependent.freeDirectBuffer(bb);
			}
			fullBuffers.clear();

			if (currentBuffer != null) {
				PlatformDependent.freeDirectBuffer(currentBuffer.complete());
				currentBuffer = null;
			}

			// To make this compatible with all versions of Windows, we must wait with
			// deleting the file until it is unmapped.
			// See also https://stackoverflow.com/questions/11099295/file-flag-delete-on-close-and-memory-mapped-files/51649618#51649618

			Files.delete(filePath);
		}

		/**
		 * Gets the number of bytes of all written data (including the metadata in the buffer headers).
		 */
		long getSize() {
			long size = 0L;
			for (ByteBuffer bb : fullBuffers) {
				size += bb.capacity();
			}
			if (currentBuffer != null) {
				size += currentBuffer.getNumBytes();
			}
			return size;
		}

		private void rollOverToNextBuffer() throws IOException {
			if (currentBuffer != null) {
				// we need to remember the original buffers, not any slices.
				// slices have no cleaner, which we need to trigger explicit unmapping
				fullBuffers.add(currentBuffer.complete());
			}

			final ByteBuffer mapped = file.map(MapMode.READ_WRITE, nextMappingOffset, mappingSize);
			currentBuffer = new Writer(mapped);
			nextMappingOffset += mappingSize;
		}

		private void throwTooLargeBuffer(Buffer buffer) throws IOException {
			throw new IOException(String.format(
					"The buffer (%d bytes) is larger than the maximum size of a memory buffer (%d bytes)",
					buffer.getSize(), mappingSize));
		}

		/**
		 * Rounds the size down to the next multiple of the {@link #PAGE_SIZE}.
		 * We need to round down here to not exceed the original maximum size value.
		 * Otherwise, values like INT_MAX would round up to overflow the valid maximum
		 * size of a memory mapping region in Java.
		 */
		private static int alignSize(int maxRegionSize) {
			checkArgument(maxRegionSize >= PAGE_SIZE);
			return maxRegionSize - (maxRegionSize % PAGE_SIZE);
		}
	}
}
