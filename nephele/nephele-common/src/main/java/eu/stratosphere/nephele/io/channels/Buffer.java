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

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the general buffer abstraction that is used by Nephele
 * to transport data through the network or the file system.
 * <p>
 * Buffers may be backed by actual main memory or files.
 * <p>
 * Each buffer is expected to be written and read exactly once. Initially, the every buffer is in write mode. Before
 * reading from the buffer, it must be explicitly switched to read mode.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 */
public final class Buffer implements ReadableByteChannel, WritableByteChannel {

	/**
	 * The concrete buffer implementation to which all method calls on
	 * this object are delegated.
	 */
	private final InternalBuffer internalBuffer;

	/**
	 * Stores whether this buffer has already been recycled.
	 */
	private final AtomicBoolean isRecycled = new AtomicBoolean(false);

	/**
	 * Constructs a new buffer object.
	 * 
	 * @param internalBuffer
	 *        the concrete implementation which backs the buffer
	 */
	Buffer(InternalBuffer internalBuffer) {

		this.internalBuffer = internalBuffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(ByteBuffer arg0) throws IOException {

		return this.internalBuffer.read(arg0);
	}

	/**
	 * Reads data from the buffer and writes it to the
	 * given {@link WritableByteChannel} object.
	 * 
	 * @param writeByteChannel
	 *        the {@link WritableByteChannel} object to write the data to
	 * @return the number of bytes read from the buffer, potentially <code>0</code> or <code>-1</code to indicate the
	 *         end of the stream
	 * @throws IOException
	 *         thrown if an error occurs while writing to the {@link WritableByteChannel} object
	 */
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		return this.internalBuffer.read(writableByteChannel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.internalBuffer.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {

		return this.internalBuffer.isOpen();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(ByteBuffer arg0) throws IOException {

		return this.internalBuffer.write(arg0);
	}

	/**
	 * Reads data from the given {@link ReadableByteChannel} object and
	 * writes it to the buffer.
	 * 
	 * @param readableByteChannel
	 *        the {@link ReadableByteChannel} object to read data from
	 * @return the number of bytes written to the buffer, possibly <code>0</code>
	 * @throws IOException
	 *         thrown if an error occurs while writing data to the buffer
	 */
	public int write(ReadableByteChannel readableByteChannel) throws IOException {

		return this.internalBuffer.write(readableByteChannel);
	}

	/**
	 * Returns the number of bytes which can be either still written to or read from
	 * the buffer, depending whether the buffer is still in write mode or not.
	 * <p>
	 * If in write mode, the method returns the number of bytes which can be written to be buffer, before its capacity
	 * limit is reached. In read mode, the method returns the number of bytes which can be read from the number until
	 * all data previously written to the buffer is consumed.
	 * 
	 * @return the number of bytes which can be either written to or read from the buffer
	 */
	public int remaining() {

		return this.internalBuffer.remaining();
	}

	/**
	 * Checks whether data can still be written to or read from the buffer.
	 * 
	 * @return <code>true</code> if data can be still written to or read from
	 *         the buffer, <code>false</code> otherwise
	 */
	public boolean hasRemaining() {

		return (this.internalBuffer.remaining() > 0);
	}

	/**
	 * Returns the size of the buffer. In write mode, the size of the buffer is the initial capacity
	 * of the buffer. In read mode, the size of the buffer is number of bytes which have been
	 * previously written to the buffer.
	 * 
	 * @return the size of the buffer in bytes
	 */
	public int size() {

		return this.internalBuffer.size();
	}

	/**
	 * Returns the {@link InternalBuffer} object which contains
	 * the actual implementation of this buffer.
	 * 
	 * @return the {@link InternalBuffer} object which contains the actual implementation of this buffer
	 */
	public InternalBuffer getInternalBuffer() {
		return this.internalBuffer;
	}

	/**
	 * Recycles the buffer. In case of a memory backed buffer, the internal memory buffer
	 * is returned to a global buffer queue. In case of a file backed buffer, the temporary
	 * file created for this buffer is deleted. A buffer can only be recycled once. Calling this method more than once
	 * will therefore have no effect.
	 */
	public void recycleBuffer() {

		if (this.isRecycled.compareAndSet(false, true)) {
			this.internalBuffer.recycleBuffer();
		}
	}

	/**
	 * Switches the buffer from write mode into read mode. After being switched to read
	 * mode, the buffer will no longer accept write requests.
	 * 
	 * @throws IOException
	 *         throws if an error occurs while finishing writing mode
	 */
	public void finishWritePhase() throws IOException {

		this.internalBuffer.finishWritePhase();
	}

	/**
	 * Returns whether the buffer is backed by main memory or a file.
	 * 
	 * @return <code>true</code> if the buffer is backed by main memory
	 *         or <code>false</code> if it is backed by a file
	 */
	public boolean isBackedByMemory() {

		return this.internalBuffer.isBackedByMemory();
	}

	/**
	 * Copies the content of the buffer to the given destination buffer. The state of the source buffer is not modified
	 * by this operation.
	 * 
	 * @param destinationBuffer
	 *        the destination buffer to copy this buffer's content to
	 * @throws IOException
	 *         thrown if an error occurs while copying the data
	 */
	public void copyToBuffer(Buffer destinationBuffer) throws IOException {

		if (size() > destinationBuffer.size()) {
			throw new IllegalArgumentException("Destination buffer is too small to store content of source buffer: "
				+ size() + " vs. " + destinationBuffer.size());
		}

		this.internalBuffer.copyToBuffer(destinationBuffer);
	}

	/**
	 * Duplicates the buffer. This operation does not duplicate the actual
	 * content of the buffer, only the reading/writing state. As a result,
	 * modifications to the original buffer will affect the duplicate and vice-versa.
	 * 
	 * @return the duplicated buffer
	 */
	public Buffer duplicate() {

		return new Buffer(this.internalBuffer.duplicate());
	}

	public boolean isReadBuffer() {

		return this.internalBuffer.isReadBuffer();
	}
}
