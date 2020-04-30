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

package org.apache.flink.table.runtime.io;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Compressed block channel writer provides a scenario where MemorySegment must be maintained.
 */
public class CompressedBlockChannelWriter
		implements BlockChannelWriter<MemorySegment>, BufferRecycler {

	private final LinkedBlockingQueue<MemorySegment> blockQueue;
	private final LinkedBlockingQueue<MemorySegment> compressedBuffers = new LinkedBlockingQueue<>();
	private final BufferFileWriter writer;
	private final boolean copyCompress;
	private final BlockCompressor compressor;

	private byte[] buf;
	private ByteBuffer bufWrapper;
	private int count;

	public CompressedBlockChannelWriter(
			IOManager ioManager, ID channel,
			LinkedBlockingQueue<MemorySegment> blockQueue,
			BlockCompressionFactory codecFactory, int preferBlockSize, int segmentSize) throws IOException {
		this.writer = ioManager.createBufferFileWriter(channel);
		this.blockQueue = blockQueue;
		copyCompress = preferBlockSize > segmentSize * 2;
		int blockSize = copyCompress ? preferBlockSize : segmentSize;
		this.compressor = codecFactory.getCompressor();

		if (copyCompress) {
			this.buf = new byte[blockSize];
			this.bufWrapper = ByteBuffer.wrap(buf);
		}

		for (int i = 0; i < 2; i++) {
			compressedBuffers.add(MemorySegmentFactory.wrap(
					new byte[compressor.getMaxCompressedSize(blockSize)]));
		}
	}

	@Override
	public void writeBlock(MemorySegment block) throws IOException {
		if (copyCompress) {
			int offset = 0;
			int len = block.size();

			while (len > 0) {
				int copy = Math.min(len, buf.length - count);
				if (copy == 0) {
					flushBuffer();
				} else {
					block.get(offset, buf, count, copy);
					count += copy;
					offset += copy;
					len -= copy;
				}
			}
		} else {
			compressBuffer(block.wrap(0, block.size()), block.size());
		}

		boolean add = blockQueue.add(block);
		Preconditions.checkState(add); // LinkedBlockingQueue never add fail.
	}

	private void flushBuffer() throws IOException {
		compressBuffer(bufWrapper, count);
		count = 0;
	}

	private void compressBuffer(ByteBuffer buffer, int len) throws IOException {
		MemorySegment compressedBuffer;
		try {
			compressedBuffer = compressedBuffers.take();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		int compressedLen = compressor.compress(
				buffer, 0, len,
				compressedBuffer.wrap(0, compressedBuffer.size()), 0);
		NetworkBuffer networkBuffer = new NetworkBuffer(compressedBuffer, this);
		networkBuffer.setSize(compressedLen);
		writer.writeBlock(networkBuffer);
	}

	@Override
	public ID getChannelID() {
		return writer.getChannelID();
	}

	@Override
	public long getSize() throws IOException {
		return writer.getSize();
	}

	@Override
	public boolean isClosed() {
		return writer.isClosed();
	}

	@Override
	public void close() throws IOException {
		if (!writer.isClosed()) {
			if (copyCompress) {
				flushBuffer();
			}
			this.writer.close();
		}
	}

	@Override
	public void deleteChannel() {
		writer.deleteChannel();
	}

	@Override
	public void closeAndDelete() throws IOException {
		writer.closeAndDelete();
	}

	@Override
	public FileChannel getNioFileChannel() {
		return writer.getNioFileChannel();
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		compressedBuffers.add(memorySegment);
	}

	@Override
	public MemorySegment getNextReturnedBlock() throws IOException {
		try {
			while (true) {
				final MemorySegment next = blockQueue.poll(1000, TimeUnit.MILLISECONDS);
				if (next != null) {
					return next;
				} else {
					if (writer.isClosed()) {
						throw new IOException("The writer has been closed.");
					}
				}
			}
		} catch (InterruptedException e) {
			throw new IOException("Writer was interrupted while waiting for the next returning segment.");
		}
	}

	@Override
	public LinkedBlockingQueue<MemorySegment> getReturnQueue() {
		return blockQueue;
	}
}
