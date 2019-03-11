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
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.table.runtime.compression.BlockCompressionFactory;
import org.apache.flink.table.runtime.compression.BlockCompressor;
import org.apache.flink.table.runtime.compression.BlockDecompressor;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Compressed block channel reader provides a scenario where MemorySegment must be maintained.
 */
public class CompressedBlockChannelReader
		implements BlockChannelReader<MemorySegment>, RequestDoneCallback<Buffer>, BufferRecycler {

	private final LinkedBlockingQueue<MemorySegment> blockQueue;
	private final boolean copyCompress;
	private final BlockDecompressor decompressor;
	private final BufferFileReader reader;
	private final AtomicReference<IOException> cause;
	private final LinkedBlockingQueue<Buffer> retBuffers = new LinkedBlockingQueue<>();

	private byte[] buf;
	private ByteBuffer bufWrapper;
	private int offset;
	private int len;

	public CompressedBlockChannelReader(
			IOManager ioManager,
			ID channel,
			LinkedBlockingQueue<MemorySegment> blockQueue,
			BlockCompressionFactory codecFactory,
			int preferBlockSize,
			int segmentSize) throws IOException {
		this.reader = ioManager.createBufferFileReader(channel, this);
		this.blockQueue = blockQueue;
		copyCompress = preferBlockSize > segmentSize * 2;
		int blockSize = copyCompress ? preferBlockSize : segmentSize;
		this.decompressor = codecFactory.getDecompressor();
		cause = new AtomicReference<>();

		if (copyCompress) {
			this.buf = new byte[blockSize];
			this.bufWrapper = ByteBuffer.wrap(buf);
		}

		BlockCompressor compressor = codecFactory.getCompressor();
		for (int i = 0; i < 2; i++) {
			MemorySegment segment = MemorySegmentFactory.wrap(new byte[compressor.getMaxCompressedSize(blockSize)]);
			reader.readInto(new NetworkBuffer(segment, this));
		}
	}

	@Override
	public void readBlock(MemorySegment segment) throws IOException {
		if (cause.get() != null) {
			throw cause.get();
		}

		if (copyCompress) {
			int readOffset = 0;
			int readLen = segment.size();

			while (readLen > 0) {
				int copy = Math.min(readLen, len - offset);
				if (copy == 0) {
					readBuffer();
				} else {
					segment.put(readOffset, buf, offset, copy);
					offset += copy;
					readOffset += copy;
					readLen -= copy;
				}
			}
		} else {
			int len = decompressBuffer(segment.wrap(0, segment.size()));
			Preconditions.checkState(len == segment.size());
		}

		boolean add = blockQueue.add(segment);
		Preconditions.checkState(add); // LinkedBlockingQueue never add fail.
	}

	private void readBuffer() throws IOException {
		len = decompressBuffer(bufWrapper);
	}

	private int decompressBuffer(ByteBuffer toRead) throws IOException {
		try {
			Buffer buffer;
			while ((buffer = retBuffers.poll(1000, TimeUnit.MILLISECONDS)) == null) {
				if (cause.get() != null) {
					throw cause.get();
				}
			}

			int readLen = decompressor.decompress(
					buffer.getMemorySegment().wrap(0, buffer.getSize()), 0, buffer.getSize(),
					toRead, 0);

			buffer.recycleBuffer();
			return readLen;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void seekToPosition(long position) throws IOException {
		throw new RuntimeException("Not support yet!");
	}

	@Override
	public MemorySegment getNextReturnedBlock() throws IOException {
		try {
			while (true) {
				final MemorySegment next = blockQueue.poll(1000, TimeUnit.MILLISECONDS);
				if (next != null) {
					return next;
				} else {
					if (reader.isClosed()) {
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

	@Override
	public ID getChannelID() {
		return reader.getChannelID();
	}

	@Override
	public long getSize() throws IOException {
		return reader.getSize();
	}

	@Override
	public boolean isClosed() {
		return reader.isClosed();
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public void deleteChannel() {
		reader.deleteChannel();
	}

	@Override
	public void closeAndDelete() throws IOException {
		reader.closeAndDelete();
	}

	@Override
	public FileChannel getNioFileChannel() {
		return reader.getNioFileChannel();
	}

	@Override
	public void requestSuccessful(Buffer request) {
		retBuffers.add(request);
	}

	@Override
	public void requestFailed(Buffer buffer, IOException e) {
		cause.compareAndSet(null, e);
		throw new RuntimeException(e);
	}

	@Override
	public void recycle(MemorySegment segment) {
		try {
			reader.readInto(new NetworkBuffer(segment, this));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
