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
import eu.stratosphere.nephele.io.channels.InternalBuffer;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.io.compression.Decompressor;

public abstract class AbstractDecompressor implements Decompressor {

	private Buffer uncompressedBuffer;

	protected Buffer compressedBuffer;

	protected ByteBuffer uncompressedDataBuffer;

	protected ByteBuffer compressedDataBuffer;

	protected int uncompressedDataBufferLength;

	protected int compressedDataBufferLength;

	protected final static int SIZE_LENGTH = 8;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer getCompressedDataBuffer() {

		return this.compressedBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer getUncompresssedDataBuffer() {

		return this.uncompressedBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setCompressedDataBuffer(Buffer buffer) {

		if (buffer == null) {
			this.compressedBuffer = null;
			this.compressedDataBuffer = null;
			this.compressedDataBufferLength = 0;
		} else {
			this.compressedDataBuffer = getInternalByteBuffer(buffer);
			this.compressedDataBufferLength = this.compressedDataBuffer.limit();
			this.compressedBuffer = buffer;

			// Extract length of uncompressed buffer from the compressed buffer
			this.uncompressedDataBufferLength = bufferToInt(this.compressedDataBuffer, 4);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setUncompressedDataBuffer(Buffer buffer) {

		if (buffer == null) {
			this.uncompressedBuffer = null;
			this.uncompressedDataBuffer = null;
			this.uncompressedDataBufferLength = 0;
		} else {
			this.uncompressedDataBuffer = getInternalByteBuffer(buffer);
			this.uncompressedBuffer = buffer;
			// Uncompressed buffer length is set the setCompressDataBuffer method
		}
	}

	/**
	 * Checks if the provided buffer is backed by memory and
	 * returns the encapsulated {@link ByteBuffer} object.
	 * 
	 * @param buffer
	 *        the buffer to unwrap the {@link ByteBuffer} object from
	 * @return the unwrapped {@link ByteBuffer} object
	 */
	protected ByteBuffer getInternalByteBuffer(Buffer buffer) {

		final InternalBuffer internalBuffer = buffer.getInternalBuffer();
		if (!(internalBuffer instanceof MemoryBuffer)) {
			throw new RuntimeException("Provided buffer is not a memory buffer and cannot be used for compression");
		}

		final MemoryBuffer memoryBuffer = (MemoryBuffer) internalBuffer;

		return memoryBuffer.getByteBuffer();
	}

	@Override
	public void decompress() throws IOException {

		if (this.uncompressedDataBuffer.position() > 0) {
			throw new IllegalStateException("Uncompressed data buffer is expected to be empty");
		}
		this.uncompressedDataBuffer.clear();

		final int result = decompressBytesDirect(SIZE_LENGTH);
		if (result < 0) {
			throw new IOException("Compression libary returned error-code: " + result);
		}

		this.uncompressedDataBuffer.position(result);
		this.uncompressedBuffer.finishWritePhase();

		// Make sure the framework considers the buffer to be fully consumed
		this.compressedDataBuffer.position(this.compressedDataBuffer.limit());
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
	public void setCurrentInternalDecompressionLibraryIndex(int index) {

		throw new IllegalStateException(
			"setCurrentInternalDecompressionLibraryIndex called with wrong compression level activated");
	}
}