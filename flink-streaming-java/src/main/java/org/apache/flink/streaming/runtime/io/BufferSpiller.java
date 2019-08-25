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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link BufferSpiller} takes the buffers and events from a data stream and adds them to a spill file.
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
public class BufferSpiller extends AbstractBufferStorage {

	/** Size of header in bytes (see add method). */
	static final int HEADER_SIZE = 9;

	/** The counter that selects the next directory to spill into. */
	private static final AtomicInteger DIRECTORY_INDEX = new AtomicInteger(0);

	/** The size of the buffer with which data is read back in. */
	private static final int READ_BUFFER_SIZE = 1024 * 1024;

	/** The directories to spill to. */
	private final File tempDir;

	/** The name prefix for spill files. */
	private final String spillFilePrefix;

	/** The buffer used for bulk reading data (used in the SpilledBufferOrEventSequence). */
	private final ByteBuffer readBuffer;

	/** The buffer that encodes the spilled header. */
	private final ByteBuffer headBuffer;

	/** The file that we currently spill to. */
	private File currentSpillFile;

	/** The channel of the file we currently spill to. */
	private FileChannel currentChannel;

	/** The page size, to let this reader instantiate properly sized memory segments. */
	private final int pageSize;

	/** A counter, to created numbered spill files. */
	private int fileCounter;

	/** The number of bytes written since the last roll over. */
	private long bytesWritten;

	public BufferSpiller(IOManager ioManager, int pageSize) throws IOException {
		this(ioManager, pageSize, -1);
	}

	public BufferSpiller(IOManager ioManager, int pageSize, long maxBufferedBytes) throws IOException {
		this(ioManager, pageSize, maxBufferedBytes, "Unknown");
	}

	/**
	 * Creates a new {@link BufferSpiller}, spilling to one of the I/O manager's temp directories.
	 *
	 * @param ioManager The I/O manager for access to the temp directories.
	 * @param pageSize The page size used to re-create spilled buffers.
	 * @param maxBufferedBytes The maximum bytes to be buffered before the checkpoint aborts.
	 * @param taskName The task name for logging.
	 * @throws IOException Thrown if the temp files for spilling cannot be initialized.
	 */
	public BufferSpiller(IOManager ioManager, int pageSize, long maxBufferedBytes, String taskName) throws IOException {
		super(maxBufferedBytes, taskName);
		this.pageSize = pageSize;

		this.readBuffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
		this.readBuffer.order(ByteOrder.LITTLE_ENDIAN);

		this.headBuffer = ByteBuffer.allocateDirect(16);
		this.headBuffer.order(ByteOrder.LITTLE_ENDIAN);

		File[] tempDirs = ioManager.getSpillingDirectories();
		this.tempDir = tempDirs[DIRECTORY_INDEX.getAndIncrement() % tempDirs.length];

		byte[] rndBytes = new byte[32];
		ThreadLocalRandom.current().nextBytes(rndBytes);
		this.spillFilePrefix = StringUtils.byteToHexString(rndBytes) + '.';

		// prepare for first contents
		createSpillingChannel();
	}

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
			headBuffer.putInt(boe.getChannelIndex());
			headBuffer.putInt(contents.remaining());
			headBuffer.put((byte) (boe.isBuffer() ? 0 : 1));
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

		ByteBuffer buf;
		if (newBuffer) {
			buf = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf = readBuffer;
		}

		// create a reader for the spilled data
		currentChannel.position(0L);
		SpilledBufferOrEventSequence seq =
				new SpilledBufferOrEventSequence(currentSpillFile, currentChannel, buf, pageSize);

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
		super.close();
	}

	/**
	 * Gets the number of bytes written in the current spill file.
	 *
	 * @return the number of bytes written in the current spill file
	 */
	@Override
	public long getPendingBytes() {
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

		/** Header is "channel index" (4 bytes) + length (4 bytes) + buffer/event (1 byte). */
		private static final int HEADER_LENGTH = 9;

		/** The file containing the data. */
		private final File file;

		/** The file channel to draw the data from. */
		private final FileChannel fileChannel;

		/** The byte buffer for bulk reading. */
		private final ByteBuffer buffer;

		/** We store this size as a constant because it is crucial it never changes. */
		private final long size;

		/** The page size to instantiate properly sized memory segments. */
		private final int pageSize;

		/** Flag to track whether the sequence has been opened already. */
		private boolean opened = false;

		/**
		 * Create a reader that reads a sequence of spilled buffers and events.
		 *
		 * @param file The file with the data.
		 * @param fileChannel The file channel to read the data from.
		 * @param buffer The buffer used for bulk reading.
		 * @param pageSize The page size to use for the created memory segments.
		 */
		SpilledBufferOrEventSequence(File file, FileChannel fileChannel, ByteBuffer buffer, int pageSize)
				throws IOException {
			this.file = file;
			this.fileChannel = fileChannel;
			this.buffer = buffer;
			this.pageSize = pageSize;
			this.size = fileChannel.size();
		}

		/**
		 * This method needs to be called before the first call to {@link #getNext()}.
		 * Otherwise the results of {@link #getNext()} are not predictable.
		 */
		@Override
		public void open() {
			if (!opened) {
				opened = true;
				buffer.position(0);
				buffer.limit(0);
			}
		}

		@Override
		public BufferOrEvent getNext() throws IOException {
			if (buffer.remaining() < HEADER_LENGTH) {
				buffer.compact();

				while (buffer.position() < HEADER_LENGTH) {
					if (fileChannel.read(buffer) == -1) {
						if (buffer.position() == 0) {
							// no trailing data
							return null;
						} else {
							throw new IOException("Found trailing incomplete buffer or event");
						}
					}
				}

				buffer.flip();
			}

			final int channel = buffer.getInt();
			final int length = buffer.getInt();
			final boolean isBuffer = buffer.get() == 0;

			if (isBuffer) {
				// deserialize buffer
				if (length > pageSize) {
					throw new IOException(String.format(
							"Spilled buffer (%d bytes) is larger than page size of (%d bytes)", length, pageSize));
				}

				MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

				int segPos = 0;
				int bytesRemaining = length;

				while (true) {
					int toCopy = Math.min(buffer.remaining(), bytesRemaining);
					if (toCopy > 0) {
						seg.put(segPos, buffer, toCopy);
						segPos += toCopy;
						bytesRemaining -= toCopy;
					}

					if (bytesRemaining == 0) {
						break;
					}
					else {
						buffer.clear();
						if (fileChannel.read(buffer) == -1) {
							throw new IOException("Found trailing incomplete buffer");
						}
						buffer.flip();
					}
				}

				Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
				buf.setSize(length);

				return new BufferOrEvent(buf, channel, true);
			}
			else {
				// deserialize event
				if (length > buffer.capacity() - HEADER_LENGTH) {
					throw new IOException("Event is too large");
				}

				if (buffer.remaining() < length) {
					buffer.compact();

					while (buffer.position() < length) {
						if (fileChannel.read(buffer) == -1) {
							throw new IOException("Found trailing incomplete event");
						}
					}

					buffer.flip();
				}

				int oldLimit = buffer.limit();
				buffer.limit(buffer.position() + length);
				AbstractEvent evt = EventSerializer.fromSerializedEvent(buffer, getClass().getClassLoader());
				buffer.limit(oldLimit);

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
	}
}
