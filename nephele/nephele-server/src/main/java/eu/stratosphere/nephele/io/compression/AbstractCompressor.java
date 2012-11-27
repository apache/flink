/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;

public abstract class AbstractCompressor implements Compressor {

	private final CompressionBufferProvider bufferProvider;

	protected MemoryBuffer uncompressedBuffer;

	private MemoryBuffer compressedBuffer;

	protected ByteBuffer uncompressedDataBuffer;

	protected ByteBuffer compressedDataBuffer;

	protected int uncompressedDataBufferLength;

	protected int compressedDataBufferLength;

	public final static int SIZE_LENGTH = 8;

	private int channelCounter = 1;

	protected AbstractCompressor(final CompressionBufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void increaseChannelCounter() {

		++this.channelCounter;
	}

	protected final void setCompressedDataBuffer(final MemoryBuffer buffer) {

		if (buffer == null) {
			this.compressedBuffer = null;
			this.compressedDataBuffer = null;
			this.compressedDataBufferLength = 0;
		} else {
			this.compressedDataBuffer = buffer.getByteBuffer();
			this.compressedDataBufferLength = this.compressedDataBuffer.limit();
			this.compressedBuffer = buffer;
		}
	}

	protected final void setUncompressedDataBuffer(final MemoryBuffer buffer) {

		if (buffer == null) {
			this.uncompressedBuffer = null;
			this.uncompressedDataBuffer = null;
			this.uncompressedDataBufferLength = 0;
		} else {
			this.uncompressedDataBuffer = buffer.getByteBuffer();
			this.uncompressedDataBufferLength = this.uncompressedDataBuffer.limit();
			this.uncompressedBuffer = buffer;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer compress(final Buffer uncompressedData) throws IOException {

		if (!uncompressedData.isBackedByMemory()) {
			throw new IllegalStateException("Uncompressed data buffer is not backed by memory");
		}

		setUncompressedDataBuffer((MemoryBuffer) uncompressedData);
		setCompressedDataBuffer(this.bufferProvider.lockCompressionBuffer());
		this.compressedDataBuffer.clear();
		this.uncompressedDataBufferLength = this.uncompressedDataBuffer.position();

		final int numberOfCompressedBytes = compressBytesDirect(0);

		// System.out.println("Compression library " + this.uncompressedDataBuffer.position() + " to " +
		// numberOfCompressedBytes + " bytes");

		this.compressedDataBuffer.position(numberOfCompressedBytes + SIZE_LENGTH);

		final Buffer compressedBuffer = this.compressedBuffer;
		this.bufferProvider.releaseCompressionBuffer(this.uncompressedBuffer);
		setUncompressedDataBuffer(null);
		setCompressedDataBuffer(null);

		return compressedBuffer;
	}

	protected abstract int compressBytesDirect(int offset);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final int getCurrentInternalCompressionLibraryIndex() {

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void shutdown() {

		--this.channelCounter;

		if (this.channelCounter == 0) {
			this.bufferProvider.shutdown();
			freeInternalResources();
		}
	}

	/**
	 * Frees the resources internally allocated by the compression library.
	 */
	protected void freeInternalResources() {

		// Default implementation does nothing
	}
}
