/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.AsynchronousBufferOrEventFileReader;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.disk.iomanager.BufferOrEventFileChannelReader.writeBufferOrEventMeta;

/**
 * The buffer spiller takes the buffers and events from a data stream and adds them to a spill file.
 * After a number of elements have been spilled, the spiller can "roll over": It presents the spilled
 * elements as a readable sequence, and opens a new spill file.
 *
 * <p>This implementation buffers data effectively in the OS cache, which gracefully extends to the
 * disk. Most data is written and re-read milliseconds later. The file is deleted after the read.
 * Consequently, in most cases, the data will never actually hit the physical disks.
 *
 * <p>IMPORTANT: The SpilledBufferOrEventSequences created by this spiller all reuse the same
 * reading memory (to reduce overhead) and can consequently not be read concurrently.
 */
@Internal
@Deprecated
public class BufferSpiller implements BufferBlocker {

	/** Size of header in bytes. Header is "channel index" (4 bytes) + length (4 bytes) + buffer/event (1 byte). */
	static final int HEADER_SIZE = 9;

	/** The counter that selects the next directory to spill into. */
	private static final AtomicInteger DIRECTORY_INDEX = new AtomicInteger(0);

	/** The size of the buffer with which data is read back in. */
	private static final int READ_BUFFER_SIZE = 1024 * 1024;

	/** The directories to spill to. */
	private final File tempDir;

	/** The name prefix for spill files. */
	private final String spillFilePrefix;

	/** The buffer pool used for bulk reading data (used in the SpilledBufferOrEventSequence). */
	private final ByteBuffer[] readBufferPool;

	/** The buffer that encodes the spilled header. */
	private final ByteBuffer headBuffer;

	/** The file that we currently spill to. */
	private File currentSpillFile;

	/** The channel of the file we currently spill to. */
	private FileChannel currentChannel;

	/** The page size, to let this reader instantiate properly sized memory segments. */
	private final int pageSize;

	/** The segment count will be used to fetch data from spilled file asynchrouns. */
	private final int asyncLoadBufferCount;

	/** A counter, to created numbered spill files. */
	private int fileCounter;

	/** The number of bytes written since the last roll over. */
	private long bytesWritten;

	/** The IO Manager associated to current BufferSpiller. */
	private final IOManager ioManager;

	/**
	 * Creates a new buffer spiller, spilling to one of the I/O manager's temp directories.
	 *
	 * @param ioManager The I/O manager for access to the temp directories.
	 * @param pageSize The page size used to re-create spilled buffers.
	 * @throws IOException Thrown if the temp files for spilling cannot be initialized.
	 */
	public BufferSpiller(IOManager ioManager, int pageSize, int asyncLoadBufferCount) throws IOException {
		this.pageSize = pageSize;
		this.asyncLoadBufferCount = asyncLoadBufferCount;

		this.readBufferPool = new ByteBuffer[asyncLoadBufferCount];
		for (int i = 0; i < asyncLoadBufferCount; ++i) {
			this.readBufferPool[i] = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
			this.readBufferPool[i].order(ByteOrder.LITTLE_ENDIAN);
		}

		this.headBuffer = ByteBuffer.allocateDirect(16);
		this.headBuffer.order(ByteOrder.LITTLE_ENDIAN);

		this.ioManager = ioManager;
		File[] tempDirs = ioManager.getSpillingDirectories();
		this.tempDir = tempDirs[DIRECTORY_INDEX.getAndIncrement() % tempDirs.length];

		byte[] rndBytes = new byte[32];
		ThreadLocalRandom.current().nextBytes(rndBytes);
		this.spillFilePrefix = StringUtils.byteToHexString(rndBytes) + '.';

		// prepare for first contents
		createSpillingChannel();
	}

	/**
	 * Adds a buffer or event to the sequence of spilled buffers and events.
	 *
	 * @param boe The buffer or event to add and spill.
	 * @throws IOException Thrown, if the buffer of event could not be spilled.
	 */
	@Override
	public void add(BufferOrEvent boe) throws IOException {
		try {
			ByteBuffer contents;
			if (boe.isBuffer()) {
				Buffer buf = boe.getBuffer();
				contents = buf.getNioBufferReadable();
			}
			else {
				contents = EventSerializer.toSerializedEvent(boe.getEvent());
			}

			headBuffer.clear();
			writeBufferOrEventMeta(headBuffer, boe.getChannelIndex(), contents.remaining(), (byte) (boe.isBuffer() ? 0 : 1));
			headBuffer.flip();

			bytesWritten += (headBuffer.remaining() + contents.remaining());

			FileUtils.writeCompletely(currentChannel, headBuffer);
			FileUtils.writeCompletely(currentChannel, contents);
		}
		finally {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
	}

	/**
	 * NOTE: The BufferOrEventSequences created by this method all reuse the same reading memory
	 * (to reduce overhead) and can consequently not be read concurrently with each other.
	 *
	 * <p>To create a sequence that can be read concurrently with the previous BufferOrEventSequence,
	 * use the {@link #rollOverWithoutReusingResources()} ()} method.
	 *
	 * @return The readable sequence of spilled buffers and events, or 'null', if nothing was added.
	 * @throws IOException Thrown, if the readable sequence could not be created, or no new spill
	 *                     file could be created.
	 */
	@Override
	public BufferOrEventSequence rollOverReusingResources() throws IOException {
		return rollOver(false);
	}

	/**
	 * The BufferOrEventSequence returned by this method is safe for concurrent consumption with
	 * any previously returned sequence.
	 *
	 * @return The readable sequence of spilled buffers and events, or 'null', if nothing was added.
	 * @throws IOException Thrown, if the readable sequence could not be created, or no new spill
	 *                     file could be created.
	 */
	@Override
	public BufferOrEventSequence rollOverWithoutReusingResources() throws IOException {
		return rollOver(true);
	}

	private BufferOrEventSequence rollOver(boolean newBuffer) throws IOException {
		if (bytesWritten == 0) {
			return null;
		}

		ByteBuffer[] bufPool;
		if (newBuffer) {
			bufPool = new ByteBuffer[asyncLoadBufferCount];
			for (int i = 0; i < asyncLoadBufferCount; ++i) {
				bufPool[i] = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
				bufPool[i].order(ByteOrder.LITTLE_ENDIAN);
			}
		} else {
			bufPool = readBufferPool;
		}

		// create a reader for the spilled data
		currentChannel.position(0L);
		SpilledBufferOrEventSequence seq =
				new SpilledBufferOrEventSequence(ioManager, currentSpillFile, currentChannel, bufPool, pageSize);

		// create ourselves a new spill file
		createSpillingChannel();

		bytesWritten = 0L;
		return seq;
	}

	/**
	 * Cleans up the current spilling channel and file.
	 *
	 * <p>Does not clean up the SpilledBufferOrEventSequences generated by calls to
	 * {@link #rollOver(boolean false)}.
	 *
	 * @throws IOException Thrown if channel closing or file deletion fail.
	 */
	@Override
	public void close() throws IOException {
		currentChannel.close();
		if (!currentSpillFile.delete()) {
			throw new IOException("Cannot delete spill file");
		}
	}

	/**
	 * Gets the number of bytes written in the current spill file.
	 *
	 * @return the number of bytes written in the current spill file
	 */
	@Override
	public long getBytesBlocked() {
		return bytesWritten;
	}

	// ------------------------------------------------------------------------
	//  For testing
	// ------------------------------------------------------------------------

	File getCurrentSpillFile() {
		return currentSpillFile;
	}

	FileChannel getCurrentChannel() {
		return currentChannel;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@SuppressWarnings("resource")
	private void createSpillingChannel() throws IOException {
		currentSpillFile = new File(tempDir, spillFilePrefix + (fileCounter++) + ".buffer");
		currentChannel = new RandomAccessFile(currentSpillFile, "rw").getChannel();
	}

	// ------------------------------------------------------------------------

	/**
	 * This class represents a sequence of spilled buffers and events, created by the
	 * {@link BufferSpiller}.
	 */
	@Deprecated
	public static class SpilledBufferOrEventSequence implements BufferOrEventSequence {

		/** The file containing the data. */
		private final File file;

		/** The file channel to draw the data from. */
		private final FileChannel fileChannel;

		/** The byte buffer for bulk reading. */
		private ByteBuffer buffer;

		/** We store this size as a constant because it is crucial it never changes. */
		private final long size;

		/** The page size to instantiate properly sized memory segments. */
		private final int pageSize;

		/** Flag to track whether the sequence has been opened already. */
		private boolean opened = false;

		/** Buffer filer reader used to load the spilled data from file asynchronous. */
		private AsynchronousBufferOrEventFileReader fileReader;

		/** Buffers reused to asynchronous load the spilled data. */
		private ByteBuffer[] bufferPool;

		/** Queue used to contain buffers/Exception/EOF reading from the spilled file. */
		private BlockingQueue<BufferOrException<IOException>> queue;

		/** The index of bufferPool used to retrieve next BufferOrEvent. */
		private int nextIndex;

		/** The Exception when loading data from file. */
		private volatile IOException lastException = null;

		/**
		 * Create a reader that reads a sequence of spilled buffers and events.
		 *
		 * @param file The file with the data.
		 * @param fileChannel The file channel to read the data from.
		 * @param bufferPool The buffer pool used for bulk reading.
		 * @param pageSize The page size to use for the created memory segments.
		 */
		SpilledBufferOrEventSequence(IOManager ioManager, File file, FileChannel fileChannel, ByteBuffer[] bufferPool, int pageSize)
				throws IOException {
			this.file = file;
			this.fileChannel = fileChannel;
			this.bufferPool = bufferPool;
			this.pageSize = pageSize;
			// +1 for EOF.
			this.queue = new LinkedBlockingQueue<>(bufferPool.length + 1);
			this.size = fileChannel.size();
			this.nextIndex = 0;
			fileReader = (AsynchronousBufferOrEventFileReader) ioManager.createBufferOrEventFileReader(ioManager.createChannel(file), new RequestDoneCallback<Buffer>() {
				@Override
				public void requestSuccessful(Buffer request) {
					if (request.getSize() > 0) {
						queue.offer(new BufferOrException<>(request));
					}
					if (fileReader.hasReachedEndOfFile()) {
						// add an eof into the queue.
						queue.offer(new BufferOrException<>());
					}
				}

				@Override
				public void requestFailed(Buffer buffer, IOException e) {
					queue.offer(new BufferOrException<>(e));
					lastException = e;
				}
			});
		}

		/**
		 * This method needs to be called before the first call to {@link #getNext()}.
		 * Otherwise the results of {@link #getNext()} are not predictable.
		 */
		@Override
		public void open() {
			if (!opened) {
				opened = true;
				for (int i = 0; i < bufferPool.length; ++i) {
					if (!readOneBuffer(i)) {
						break;
					}
				}
			}
		}

		@Override
		public BufferOrEvent getNext() throws IOException {
			BufferOrException<IOException> bufferOrException;
			try {
				bufferOrException = queue.take();
				if (bufferOrException.isEof()) {
					return null;
				}

				if (bufferOrException.isException()) {
					throw bufferOrException.getException();
				}

				buffer = bufferOrException.getBuffer().getNioBufferReadable();
				buffer.order(ByteOrder.LITTLE_ENDIAN);
			} catch (InterruptedException e) {
				throw new IOException("Can not read the next buffer or event.");
			}

			final int channel = buffer.getInt();
			final int length = buffer.getInt();
			final boolean isBuffer = buffer.get() == 0;
			buffer.limit(HEADER_SIZE + length);

			if (isBuffer) {
				// deserialize buffer
				if (length > pageSize) {
					throw new IOException(String.format(
							"Spilled buffer (%d bytes) is larger than page size of (%d bytes)", length, pageSize));
				}

				MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

				int segPos = 0;
				int bytesRemaining = length;

				//we know we always have enough content
				while (bytesRemaining > 0) {
					int toCopy = Math.min(buffer.remaining(), bytesRemaining);
					seg.put(segPos, buffer, toCopy);
					segPos += toCopy;
					bytesRemaining -= toCopy;
				}

				Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
				buf.setSize(length);

				readOneBuffer(nextIndex);
				nextIndex = (nextIndex + 1) % bufferPool.length;
				return new BufferOrEvent(buf, channel, true);
			}
			else {
				// deserialize event
				AbstractEvent evt = EventSerializer.fromSerializedEvent(buffer, getClass().getClassLoader());
				readOneBuffer(nextIndex);
				nextIndex = (nextIndex + 1) % bufferPool.length;
				return new BufferOrEvent(evt, channel, true, length);
			}
		}

		@Override
		public void cleanup() throws IOException {
			fileChannel.close();
			if (!file.delete()) {
				throw new IOException("Cannot remove temp file for stream alignment writer");
			}
		}

		@Override
		public long size() {
			return size;
		}

		private boolean readOneBuffer(int index) {
			// reach the end of file, no need to add request.
			if (fileReader.hasReachedEndOfFile()) {
				return false;
			}

			// catch an exception previously, no need to add request.
			if (lastException != null) {
				return false;
			}

			try {
				Buffer buffer = new NetworkBuffer(MemorySegmentFactory.wrapOffHeapMemory(bufferPool[index]), FreeingBufferRecycler.INSTANCE);
				fileReader.readInto(buffer);
			} catch (IOException e) {
				lastException = e;
				return false;
			}
			return true;
		}
	}
}
