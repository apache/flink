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
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Putting and getting of a sequence of buffers to/from a FileChannel or a ByteBuffer.
 * This class handles the headers, length encoding, memory slicing.
 *
 * <p>The encoding is the same across FileChannel and ByteBuffer, so this class can
 * write to a file and read from the byte buffer that results from mapping this file to memory.
 */
final class BufferReaderWriterUtil {

	static final int HEADER_LENGTH = 8;

	static final int HEADER_VALUE_IS_BUFFER = 0;

	static final int HEADER_VALUE_IS_EVENT = 1;

	// ------------------------------------------------------------------------
	//  ByteBuffer read / write
	// ------------------------------------------------------------------------

	static boolean writeBuffer(Buffer buffer, ByteBuffer memory) {
		final int bufferSize = buffer.getSize();

		if (memory.remaining() < bufferSize + HEADER_LENGTH) {
			return false;
		}

		memory.putInt(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
		memory.putInt(bufferSize);
		memory.put(buffer.getNioBufferReadable());
		return true;
	}

	@Nullable
	static Buffer sliceNextBuffer(ByteBuffer memory) {
		final int remaining = memory.remaining();

		// we only check the correct case where data is exhausted
		// all other cases can only occur if our write logic is wrong and will already throw
		// buffer underflow exceptions which will cause the read to fail.
		if (remaining == 0) {
			return null;
		}

		final int header = memory.getInt();
		final int size = memory.getInt();

		memory.limit(memory.position() + size);
		ByteBuffer buf = memory.slice();
		memory.position(memory.limit());
		memory.limit(memory.capacity());

		MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(buf);

		return bufferFromMemorySegment(
				memorySegment,
				FreeingBufferRecycler.INSTANCE,
				size,
				header == HEADER_VALUE_IS_EVENT);
	}

	// ------------------------------------------------------------------------
	//  ByteChannel read / write
	// ------------------------------------------------------------------------

	static long writeToByteChannel(
			FileChannel channel,
			Buffer buffer,
			ByteBuffer[] arrayWithHeaderBuffer) throws IOException {

		final ByteBuffer headerBuffer = arrayWithHeaderBuffer[0];
		headerBuffer.clear();
		headerBuffer.putInt(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
		headerBuffer.putInt(buffer.getSize());
		headerBuffer.flip();

		final ByteBuffer dataBuffer = buffer.getNioBufferReadable();
		arrayWithHeaderBuffer[1] = dataBuffer;

		final long bytesExpected = HEADER_LENGTH + dataBuffer.remaining();

		// The file channel implementation guarantees that all bytes are written when invoked
		// because it is a blocking channel (the implementation mentioned it as guaranteed).
		// However, the api docs leaves it somewhat open, so it seems to be an undocumented contract in the JRE.
		// We build this safety net to be on the safe side.
		if (bytesExpected < channel.write(arrayWithHeaderBuffer)) {
			writeBuffers(channel, arrayWithHeaderBuffer);
		}
		return bytesExpected;
	}

	static long writeToByteChannelIfBelowSize(
			FileChannel channel,
			Buffer buffer,
			ByteBuffer[] arrayWithHeaderBuffer,
			long bytesLeft) throws IOException {

		if (bytesLeft >= HEADER_LENGTH + buffer.getSize()) {
			return writeToByteChannel(channel, buffer, arrayWithHeaderBuffer);
		}

		return -1L;
	}

	@Nullable
	static Buffer readFromByteChannel(
			FileChannel channel,
			ByteBuffer headerBuffer,
			MemorySegment memorySegment,
			BufferRecycler bufferRecycler) throws IOException {

		headerBuffer.clear();
		if (!tryReadByteBuffer(channel, headerBuffer)) {
			return null;
		}
		headerBuffer.flip();

		final ByteBuffer targetBuf;
		final int header;
		final int size;

		try {
			header = headerBuffer.getInt();
			size = headerBuffer.getInt();
			targetBuf = memorySegment.wrap(0, size);
		}
		catch (BufferUnderflowException | IllegalArgumentException e) {
			// buffer underflow if header buffer is undersized
			// IllegalArgumentException if size is outside memory segment size
			throwCorruptDataException();
			return null; // silence compiler
		}

		readByteBufferFully(channel, targetBuf);

		return bufferFromMemorySegment(memorySegment, bufferRecycler, size, header == HEADER_VALUE_IS_EVENT);
	}

	static ByteBuffer allocatedHeaderBuffer() {
		ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
		configureByteBuffer(bb);
		return bb;
	}

	static ByteBuffer[] allocatedWriteBufferArray() {
		return new ByteBuffer[] { allocatedHeaderBuffer(), null };
	}

	private static boolean tryReadByteBuffer(FileChannel channel, ByteBuffer b) throws IOException {
		if (channel.read(b) == -1) {
			return false;
		}
		else {
			while (b.hasRemaining()) {
				if (channel.read(b) == -1) {
					throwPrematureEndOfFile();
				}
			}
			return true;
		}
	}

	private static void readByteBufferFully(FileChannel channel, ByteBuffer b) throws IOException {
		// the post-checked loop here gets away with one less check in the normal case
		do {
			if (channel.read(b) == -1) {
				throwPrematureEndOfFile();
			}
		}
		while (b.hasRemaining());
	}

	private static void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
	}

	private static void writeBuffers(FileChannel channel, ByteBuffer... buffers) throws IOException {
		for (ByteBuffer buffer : buffers) {
			writeBuffer(channel, buffer);
		}
	}

	private static void throwPrematureEndOfFile() throws IOException {
		throw new IOException("The spill file is corrupt: premature end of file");
	}

	private static void throwCorruptDataException() throws IOException {
		throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	static Buffer bufferFromMemorySegment(
			MemorySegment memorySegment,
			BufferRecycler memorySegmentRecycler,
			int size,
			boolean isEvent) {

		final Buffer buffer = new NetworkBuffer(memorySegment, memorySegmentRecycler);
		buffer.setSize(size);

		if (isEvent) {
			buffer.tagAsEvent();
		}

		return buffer;
	}

	static void configureByteBuffer(ByteBuffer buffer) {
		buffer.order(ByteOrder.nativeOrder());
	}
}
