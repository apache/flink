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
import org.apache.flink.runtime.io.compression.BlockDecompressor;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a
 * {@link BufferFileReader}, making it effectively a data input stream. The view reads it data
 * in blocks from the underlying channel and decompress it before returning to caller. The view
 * can only read data that has been written by {@link CompressedHeaderlessChannelWriterOutputView},
 * due to block formatting.
 */
public class CompressedHeaderlessChannelReaderInputView
		extends AbstractChannelReaderInputView
		implements RequestDoneCallback<Buffer>, BufferRecycler {

	private final BlockDecompressor decompressor;
	private final BufferFileReader reader;
	private final MemorySegment uncompressedBuffer;
	private final AtomicReference<IOException> cause;

	private final LinkedBlockingQueue<Buffer> retBuffers = new LinkedBlockingQueue<>();

	private int numBlocksRemaining;
	private int currentSegmentLimit;

	public CompressedHeaderlessChannelReaderInputView(
			FileIOChannel.ID id,
			IOManager ioManager,
			BlockCompressionFactory compressionCodecFactory,
			int compressionBlockSize,
			int numBlocks) throws IOException {
		super(0);
		this.numBlocksRemaining = numBlocks;
		this.reader = ioManager.createBufferFileReader(id, this);
		uncompressedBuffer = MemorySegmentFactory.wrap(new byte[compressionBlockSize]);
		decompressor = compressionCodecFactory.getDecompressor();
		cause = new AtomicReference<>();

		BlockCompressor compressor = compressionCodecFactory.getCompressor();
		for (int i = 0; i < 2; i++) {
			MemorySegment segment = MemorySegmentFactory.wrap(new byte[compressor.getMaxCompressedSize(
					compressionBlockSize)]);
			reader.readInto(new NetworkBuffer(segment, this));
		}
	}

	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		if (cause.get() != null) {
			throw cause.get();
		}

		// check for end-of-stream
		if (this.numBlocksRemaining <= 0) {
			this.reader.close();
			throw new EOFException();
		}

		try {
			Buffer buffer;
			while ((buffer = retBuffers.poll(1, TimeUnit.SECONDS)) == null) {
				if (cause.get() != null) {
					throw cause.get();
				}
			}
			this.currentSegmentLimit = decompressor.decompress(
					buffer.getMemorySegment().getArray(), 0, buffer.getSize(),
					uncompressedBuffer.getArray(), 0
			);

			buffer.recycleBuffer();
			this.numBlocksRemaining--;
			return uncompressedBuffer;
		}
		catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public BufferFileReader getReader() {
		return reader;
	}

	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return currentSegmentLimit;
	}

	@Override
	public List<MemorySegment> close() throws IOException {
		reader.close();
		return Collections.emptyList();
	}

	@Override
	public FileIOChannel getChannel() {
		return reader;
	}

	public boolean isClosed() {
		return reader.isClosed();
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
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
