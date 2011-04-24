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
import eu.stratosphere.nephele.io.compression.Compressor;

public abstract class AbstractCompressor implements Compressor {

	protected Buffer uncompressedBuffer;

	private Buffer compressedBuffer;

	protected ByteBuffer uncompressedDataBuffer;

	protected ByteBuffer compressedDataBuffer;

	protected int uncompressedDataBufferLength;

	protected int compressedDataBufferLength;

	private final static int SIZE_LENGTH = 8;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer getCompressedDataBuffer() {

		return this.compressedBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Buffer getUncompresssedDataBuffer() {

		return this.uncompressedBuffer;
	}

	/**
	 * Checks if the provided buffer is backed by memory and
	 * returns the encapsulated {@link ByteBuffer} object.
	 * 
	 * @param buffer
	 *        the buffer to unwrap the {@link ByteBuffer} object from
	 * @return the unwrapped {@link ByteBuffer} object
	 */
	private ByteBuffer getInternalByteBuffer(Buffer buffer) {

		final InternalBuffer internalBuffer = buffer.getInternalBuffer();
		if (!(internalBuffer instanceof MemoryBuffer)) {
			throw new RuntimeException("Provided buffer is not a memory buffer and cannot be used for compression");
		}

		final MemoryBuffer memoryBuffer = (MemoryBuffer) internalBuffer;

		return memoryBuffer.getByteBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void setCompressedDataBuffer(Buffer buffer) {

		if (buffer == null) {
			this.compressedBuffer = null;
			this.compressedDataBuffer = null;
			this.compressedDataBufferLength = 0;
		} else {
			this.compressedDataBuffer = getInternalByteBuffer(buffer);
			this.compressedDataBufferLength = this.compressedDataBuffer.limit();
			this.compressedBuffer = buffer;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void setUncompressedDataBuffer(Buffer buffer) {

		if (buffer == null) {
			this.uncompressedBuffer = null;
			this.uncompressedDataBuffer = null;
			this.uncompressedDataBufferLength = 0;
		} else {
			this.uncompressedDataBuffer = getInternalByteBuffer(buffer);
			this.uncompressedDataBufferLength = this.uncompressedDataBuffer.limit();
			this.uncompressedBuffer = buffer;
		}
	}

	@Override
	public final void compress() throws IOException {
		this.compressedDataBuffer.clear();
		this.uncompressedDataBufferLength = this.uncompressedDataBuffer.position();

		final int numberOfCompressedBytes = compressBytesDirect(0);

		// System.out.println("Compression library " + this.uncompressedDataBuffer.position() + " to " +
		// numberOfCompressedBytes + " bytes");

		this.compressedDataBuffer.position(numberOfCompressedBytes + SIZE_LENGTH);

		// If everything went ok, prepare buffers for next run
		this.uncompressedBuffer.finishWritePhase();
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
	public void shutdown() {
	
		// The default implementation of this method does nothing
	}
}