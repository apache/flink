/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.io.channels.MemoryBufferPoolConnector;
import eu.stratosphere.nephele.io.compression.Decompressor;

public abstract class AbstractDecompressor implements Decompressor {

	private final CompressionBufferProvider bufferProvider;

	private MemoryBuffer uncompressedBuffer;

	protected MemoryBuffer compressedBuffer;

	protected ByteBuffer uncompressedDataBuffer;

	protected ByteBuffer compressedDataBuffer;

	protected int uncompressedDataBufferLength;

	protected int compressedDataBufferLength;

	protected final static int SIZE_LENGTH = 8;

	private int channelCounter = 1;

	protected AbstractDecompressor(final CompressionBufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void increaseChannelCounter() {

		++this.channelCounter;
	}

	protected void setCompressedDataBuffer(final MemoryBuffer buffer) {

		if (buffer == null) {
			this.compressedBuffer = null;
			this.compressedDataBuffer = null;
			this.compressedDataBufferLength = 0;
		} else {
			this.compressedDataBuffer = buffer.getByteBuffer();
			this.compressedDataBufferLength = this.compressedDataBuffer.limit();
			this.compressedBuffer = buffer;

			// Extract length of uncompressed buffer from the compressed buffer
			this.uncompressedDataBufferLength = bufferToInt(this.compressedDataBuffer, 4);
		}
	}

	protected void setUncompressedDataBuffer(final MemoryBuffer buffer) {

		if (buffer == null) {
			this.uncompressedBuffer = null;
			this.uncompressedDataBuffer = null;
			this.uncompressedDataBufferLength = 0;
		} else {
			this.uncompressedDataBuffer = buffer.getByteBuffer();
			this.uncompressedBuffer = buffer;
			// Uncompressed buffer length is set the setCompressDataBuffer method
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer decompress(Buffer compressedData) throws IOException {

		boolean tmpBufferUsed = false;
		if (!compressedData.isBackedByMemory()) {
			tmpBufferUsed = true;
			final MemoryBuffer tmpBuffer = this.bufferProvider.lockTemporaryBuffer();
			tmpBuffer.reset(this.bufferProvider.getMaximumBufferSize());
			compressedData.copyToBuffer(tmpBuffer);
			compressedData.recycleBuffer();
			compressedData = tmpBuffer;
		}

		setCompressedDataBuffer((MemoryBuffer) compressedData);
		setUncompressedDataBuffer(this.bufferProvider.lockCompressionBuffer());

		if (this.uncompressedDataBuffer.position() > 0) {
			throw new IllegalStateException("Uncompressed data buffer is expected to be empty");
		}
		this.uncompressedDataBuffer.clear();

		final int result = decompressBytesDirect(SIZE_LENGTH);
		if (result < 0) {
			throw new IOException("Compression libary returned error-code: " + result);
		}

		if (this.uncompressedBuffer.isInWriteMode()) {
			this.uncompressedDataBuffer.position(result);
			this.uncompressedBuffer.finishWritePhase();
		} else {
			this.uncompressedDataBuffer.position(0);
			this.uncompressedDataBuffer.limit(result);
		}
		// System.out.println("UNCOMPRESSED SIZE: " + this.uncompressedBuffer.size());

		Buffer uncompressedBuffer = this.uncompressedBuffer;

		// Release the compression buffer again
		this.bufferProvider.releaseCompressionBuffer(this.compressedBuffer);

		setCompressedDataBuffer(null);
		setUncompressedDataBuffer(null);

		if (tmpBufferUsed) {

			final MemoryBuffer memBuffer = (MemoryBuffer) uncompressedBuffer;
			final ByteBuffer bb = memBuffer.getByteBuffer();

			uncompressedBuffer = BufferFactory.createFromMemory(bb.remaining(), bb, new MemoryBufferPoolConnector() {

				/**
				 * {@inheritDoc}
				 */
				@Override
				public void recycle(final ByteBuffer byteBuffer) {

					bufferProvider.releaseTemporaryBuffer(memBuffer);
				}
			});

			// Fake transition to read mode
			bb.position(bb.limit());
			bb.limit(bb.capacity());
			uncompressedBuffer.finishWritePhase();
		}

		return uncompressedBuffer;
	}

	protected int bufferToInt(ByteBuffer buffer, int offset) {

		int retVal = 0;

		retVal += 0xff000000 & (((int) buffer.get(offset)) << 24);
		retVal += 0x00ff0000 & (((int) buffer.get(offset + 1)) << 16);
		retVal += 0x0000ff00 & (((int) buffer.get(offset + 2)) << 8);
		retVal += 0x000000ff & ((int) buffer.get(offset + 3));

		return retVal;
	}

	protected abstract int decompressBytesDirect(int offset);

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setCurrentInternalDecompressionLibraryIndex(final int index) {

		throw new IllegalStateException(
			"setCurrentInternalDecompressionLibraryIndex called with wrong compression level activated");
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
