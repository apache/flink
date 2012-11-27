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

package eu.stratosphere.nephele.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * This class is a special test implementation of a {@link ReadableByteChannel} and {@link WritableByteChannel}. Data is
 * first written into main memory through the {@link WritableByteChannel} interface. Afterwards, the data can be read
 * again through the {@link ReadableByteChannel} abstraction. The implementation is capable of simulating interruptions
 * in the byte stream.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public class InterruptibleByteChannel implements ReadableByteChannel, WritableByteChannel {

	/**
	 * The initial size of the internal memory buffer in bytes.
	 */
	private static final int INITIAL_BUFFER_SIZE = 8192;

	/**
	 * Stores the requested interruptions of the byte stream during write operations that still have to be processed.
	 */
	private final Queue<Integer> writeInterruptPositions = new ArrayDeque<Integer>();

	/**
	 * Stores the requested interruptions of the byte stream during read operations that still have to be processed.
	 */
	private final Queue<Integer> readInterruptPositions = new ArrayDeque<Integer>();

	/**
	 * The internal memory buffer used to hold the written data.
	 */
	private ByteBuffer buffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);

	/**
	 * Stores if the channel is still open.
	 */
	private boolean isOpen = true;

	/**
	 * Stores if the channel is still in write phase.
	 */
	private boolean isInWritePhase = true;

	/**
	 * Constructs a new interruptible byte channel.
	 * 
	 * @param writeInterruptPositions
	 *        the positions to interrupt the byte stream during write operations or <code>null</code> if no
	 *        interruptions are requested
	 * @param readInterruptPositions
	 *        the positions to interrupt the byte stream during read operations or <code>null</code> if no interruptions
	 *        are requested
	 */
	public InterruptibleByteChannel(final int[] writeInterruptPositions, final int[] readInterruptPositions) {

		if (writeInterruptPositions != null) {

			for (int i = 0; i < writeInterruptPositions.length - 1; ++i) {
				if (writeInterruptPositions[i] > writeInterruptPositions[i + 1]) {
					throw new IllegalArgumentException("Write interrupt positions must be ordered ascendingly");
				}
				this.writeInterruptPositions.add(Integer.valueOf(writeInterruptPositions[i]));
			}

			this.writeInterruptPositions.add(Integer
				.valueOf(writeInterruptPositions[writeInterruptPositions.length - 1]));
		}

		if (readInterruptPositions != null) {

			for (int i = 0; i < readInterruptPositions.length - 1; ++i) {
				if (readInterruptPositions[i] > readInterruptPositions[i + 1]) {
					throw new IllegalArgumentException("Read interrupt positions must be ordered ascendingly");
				}
				this.readInterruptPositions.add(Integer.valueOf(readInterruptPositions[i]));
			}

			this.readInterruptPositions.add(Integer
				.valueOf(readInterruptPositions[readInterruptPositions.length - 1]));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {

		return this.isOpen;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.isOpen = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src) throws IOException {

		if (!this.isOpen) {
			throw new ClosedChannelException();
		}

		if (!this.isInWritePhase) {
			throw new IllegalStateException("Channel is not in write phase anymore");
		}

		if (src.remaining() > this.buffer.remaining()) {
			increaseBufferSize();
		}

		int numberOfBytesToAccept = src.remaining();
		if (!this.writeInterruptPositions.isEmpty()
			&& (this.buffer.position() + numberOfBytesToAccept < this.writeInterruptPositions.peek().intValue())) {
			numberOfBytesToAccept = this.writeInterruptPositions.poll().intValue() - this.buffer.position();

			this.buffer.limit(this.buffer.position() + numberOfBytesToAccept);
			this.buffer.put(src);
			this.buffer.limit(this.buffer.capacity());

			return numberOfBytesToAccept;
		}

		this.buffer.put(src);

		return numberOfBytesToAccept;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst) throws IOException {

		if (!this.isOpen) {
			throw new ClosedChannelException();
		}

		if (this.isInWritePhase) {
			throw new IllegalStateException("Channel is still in write phase");
		}

		if (!this.buffer.hasRemaining()) {
			return -1;
		}

		int numberOfBytesToRetrieve = Math.min(this.buffer.remaining(), dst.remaining());
		if (!this.readInterruptPositions.isEmpty()
				&& (this.buffer.position() + numberOfBytesToRetrieve > this.readInterruptPositions.peek().intValue())) {
			numberOfBytesToRetrieve = this.readInterruptPositions.poll().intValue() - this.buffer.position();
		}

		final int oldLimit = this.buffer.limit();
		this.buffer.limit(this.buffer.position() + numberOfBytesToRetrieve);
		dst.put(this.buffer);
		this.buffer.limit(oldLimit);

		return numberOfBytesToRetrieve;
	}

	/**
	 * Switches the channel to read phase.
	 */
	public void switchToReadPhase() {

		if (!this.isInWritePhase) {
			throw new IllegalStateException("Channel is already in read phase");
		}

		this.isInWritePhase = false;
		this.buffer.flip();
	}

	/**
	 * Doubles the capacity of the internal byte buffer while preserving its content.
	 */
	private void increaseBufferSize() {

		final ByteBuffer newBuf = ByteBuffer.allocate(this.buffer.capacity() * 2);
		this.buffer.flip();
		newBuf.put(this.buffer);
		this.buffer = newBuf;
	}
}
