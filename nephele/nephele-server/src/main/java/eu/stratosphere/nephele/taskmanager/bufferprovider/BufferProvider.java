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

package eu.stratosphere.nephele.taskmanager.bufferprovider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;

/**
 * The buffer provider handles requests for buffers to temporarily store the serialized data of byte buffered channels.
 * To
 * avoid deadlocks a buffer provider has two different buffer pools. One pool contains buffers which are used by
 * {@link AbstractByteBufferedInputChannel} objects, the so-called read buffers. The second pool contains buffers which
 * are used by {@link AbstractByteBufferedOutputChannel}, the so-called write buffers.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class BufferProvider implements ReadBufferProvider, WriteBufferProvider {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(BufferProvider.class);

	/**
	 * The default setting for spilling.
	 */
	private static final boolean DEFAULT_ALLOW_SPILLING = false;

	/**
	 * The default number of read buffers to create at startup.
	 */
	private static final int DEFAULT_NUMBER_OF_READ_BUFFERS = 128;

	/**
	 * The default number of write buffers to create at startup.
	 */
	private static final int DEFAULT_NUMBER_OF_WRITE_BUFFERS = 128;

	/**
	 * The default buffer size in bytes.
	 */
	private static final int DEFAULT_BUFFER_SIZE_IN_BYTES = 128 * 1024; // 128k

	/**
	 * The singleton instance of the buffer provider.
	 */
	private static BufferProvider bufferProvider = null;

	/**
	 * Queue containing the empty read buffers.
	 */
	private final Deque<ByteBuffer> emptyReadBuffers = new ArrayDeque<ByteBuffer>();

	/**
	 * Queue containing the empty write buffers.
	 */
	private final Deque<ByteBuffer> emptyWriteBuffers = new ArrayDeque<ByteBuffer>();

	/**
	 * Sets of listeners which are notified in case we run out of empty write buffers.
	 */
	private final Set<OutOfByteBuffersListener> registeredOutOfWriteBuffersListeners = new HashSet<OutOfByteBuffersListener>();

	/**
	 * The file buffer provider which manages buffers backed by files.
	 */
	private final FileBufferManager fileBufferManager;

	/**
	 * The number of read buffers created at startup.
	 */
	private final int numberOfReadBuffers;

	/**
	 * The number of write buffers created at startup.
	 */
	private final int numberOfWriteBuffers;

	/**
	 * The size of read/write buffers in bytes.
	 */
	private final int bufferSizeInBytes;

	/**
	 * Indicates whether read buffers may also be written to disk.
	 */
	private final boolean isSpillingAllowed;

	/**
	 * Constructs a new buffer provider.
	 */
	private BufferProvider() {

		this.numberOfReadBuffers = GlobalConfiguration.getInteger("channel.network.numberOfReadBuffers",
			DEFAULT_NUMBER_OF_READ_BUFFERS);
		this.numberOfWriteBuffers = GlobalConfiguration.getInteger("channel.network.numberOfWriteBuffers",
			DEFAULT_NUMBER_OF_WRITE_BUFFERS);
		this.bufferSizeInBytes = GlobalConfiguration.getInteger("channel.network.bufferSizeInBytes",
			DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.isSpillingAllowed = GlobalConfiguration
			.getBoolean("channel.network.allowSpilling", DEFAULT_ALLOW_SPILLING);
		

		// Initialize buffers
		for (int i = 0; i < this.numberOfReadBuffers; i++) {
			final ByteBuffer readBuffer = ByteBuffer.allocateDirect(bufferSizeInBytes);
			this.emptyReadBuffers.add(readBuffer);
		}

		for (int i = 0; i < this.numberOfWriteBuffers; i++) {
			final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSizeInBytes);
			this.emptyWriteBuffers.add(writeBuffer);
		}

		this.fileBufferManager = FileBufferManager.getFileBufferManager();

		LOG.info("Initialized buffer provider with " + this.numberOfReadBuffers + " read buffers, "
			+ this.numberOfWriteBuffers + " write buffers (" + this.bufferSizeInBytes + " bytes) and spilling "
			+ (this.isSpillingAllowed ? "activated" : "deactivated"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerOutOfWriteBuffersListener(final OutOfByteBuffersListener listener) {

		synchronized (this.registeredOutOfWriteBuffersListeners) {
			this.registeredOutOfWriteBuffersListeners.add(listener);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterOutOfWriteBuffersLister(final OutOfByteBuffersListener listener) {

		synchronized (this.registeredOutOfWriteBuffersListeners) {
			this.registeredOutOfWriteBuffersListeners.remove(listener);
		}
	}

	/**
	 * Requests one buffer from the pool of write buffers. If a buffer is available, it will have at least the size of
	 * <code>bufferSize</code> bytes.
	 * 
	 * @param bufferSize
	 *        the minimum size of the requested buffer
	 * @return a buffer from the pool of write buffers with at least the size of <code>bufferSize</code> bytes or
	 *         <code>null</code> if no buffer is available
	 */
	public Buffer requestEmptyWriteBuffer(final int bufferSize) {

		if (bufferSize > this.bufferSizeInBytes) {
			LOG.error("buffer of " + bufferSize + " requested, but maximum buffer size is " + this.bufferSizeInBytes);
		}

		synchronized (this.emptyWriteBuffers) {

			if (this.emptyWriteBuffers.isEmpty()) {

				sendOutOfWriteBuffersNotification();
				return null;
			}

			return BufferFactory.createFromMemory(bufferSize, this.emptyWriteBuffers.poll(), this.emptyWriteBuffers,
				false);
		}
	}

	/**
	 * Sends out out of write buffers notifications to all registered listeners.
	 */
	private void sendOutOfWriteBuffersNotification() {

		synchronized (this.registeredOutOfWriteBuffersListeners) {
			if (!this.registeredOutOfWriteBuffersListeners.isEmpty()) {
				final Iterator<OutOfByteBuffersListener> it = this.registeredOutOfWriteBuffersListeners
					.iterator();
				while (it.hasNext()) {
					it.next().outOfByteBuffers();
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BufferPairResponse requestEmptyWriteBuffers(final BufferPairRequest bufferPairRequest)
			throws InterruptedException {

		synchronized (this.emptyWriteBuffers) {

			while (this.emptyWriteBuffers.size() < bufferPairRequest.getNumberOfRequestedByteBuffers()) {

				sendOutOfWriteBuffersNotification();

				this.emptyWriteBuffers.wait();
			}

			Buffer compressedDataBuffer = null;
			Buffer uncompressedDataBuffer = null;
			if (bufferPairRequest.getCompressedDataBufferSize() >= 0) {
				compressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest.getCompressedDataBufferSize(),
					this.emptyWriteBuffers.poll(), this.emptyWriteBuffers, false);
			}

			if (bufferPairRequest.getUncompressedDataBufferSize() >= 0) {
				uncompressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest
					.getUncompressedDataBufferSize(), this.emptyWriteBuffers.poll(), this.emptyWriteBuffers, false);
			}

			return new BufferPairResponse(compressedDataBuffer, uncompressedDataBuffer);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BufferPairResponse requestEmptyReadBuffers(final BufferPairRequest bufferPairRequest)
			throws InterruptedException {

		synchronized (this.emptyReadBuffers) {

			while (this.emptyReadBuffers.size() < bufferPairRequest.getNumberOfRequestedByteBuffers()) {
				this.emptyReadBuffers.wait();
			}

			Buffer compressedDataBuffer = null;
			Buffer uncompressedDataBuffer = null;
			if (bufferPairRequest.getCompressedDataBufferSize() >= 0) {
				compressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest.getCompressedDataBufferSize(),
					this.emptyReadBuffers.poll(), this.emptyReadBuffers, true);
			}

			if (bufferPairRequest.getUncompressedDataBufferSize() >= 0) {
				uncompressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest
					.getUncompressedDataBufferSize(), this.emptyReadBuffers.poll(), this.emptyReadBuffers, true);
			}
			return new BufferPairResponse(compressedDataBuffer, uncompressedDataBuffer);
		}
	}

	/**
	 * Requests an empty read buffer with a minimum size of <code>minimumSizeOfBuffer</code> for a specific
	 * {@link AbstractByteBufferedInputChannel}. The method will wait until the request can be fulfilled.
	 * 
	 * @param minimumSizeOfBuffer
	 *        the minimum size of the requested read buffer in bytes
	 * @param targetChannelID
	 *        the ID of the input channel this buffer is for
	 * @return the buffer with at least the requested size
	 * @throws IOException
	 *         thrown if an I/O error occurs while allocating the buffer
	 */
	public Buffer requestEmptyReadBufferAndWait(int minimumSizeOfBuffer, ChannelID targetChannelID) throws IOException,
			InterruptedException {

		return requestEmptyReadBufferInternal(minimumSizeOfBuffer, targetChannelID, true);
	}

	private Buffer requestEmptyReadBufferInternal(int minimumSizeOfBuffer, ChannelID targetChannelID, boolean wait)
			throws IOException, InterruptedException {

		if (minimumSizeOfBuffer > this.bufferSizeInBytes) {
			throw new IOException("Requested buffer size is " + minimumSizeOfBuffer + ", system can offer at most "
				+ this.bufferSizeInBytes);
		}

		synchronized (this.emptyReadBuffers) {

			if (wait) {

				while ((this.emptyReadBuffers.size() - 2) <= 0) { // -2 because there must be at least one buffer left
																	// if
					// compression is enabled

					if (this.isSpillingAllowed) {
						return BufferFactory.createFromFile(minimumSizeOfBuffer, targetChannelID,
							this.fileBufferManager);
					} else {
						this.emptyReadBuffers.wait();
					}
				}

				return BufferFactory.createFromMemory(minimumSizeOfBuffer, this.emptyReadBuffers.poll(),
					this.emptyReadBuffers, true);

			} else {

				if ((this.emptyReadBuffers.size() - 2) > 0) { // -2 because there must be at least one buffer left if
					// compression is enabled
					return BufferFactory.createFromMemory(minimumSizeOfBuffer, this.emptyReadBuffers.poll(),
						this.emptyReadBuffers, true);
				}

				if (this.isSpillingAllowed) {
					return BufferFactory.createFromFile(minimumSizeOfBuffer, targetChannelID, this.fileBufferManager);
				}
			}
		}

		return null;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyReadBuffer(final int minimumSizeOfBuffer, final ChannelID targetChannelID)
			throws IOException {

		try {
			return requestEmptyReadBufferInternal(minimumSizeOfBuffer, targetChannelID, false);
		} catch (InterruptedException e) {
			LOG.error("Caught InterruptedException although the method is not supposed to block");
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileBufferManager getFileBufferManager() {

		return this.fileBufferManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.bufferSizeInBytes;
	}

	/**
	 * Returns the singleton instance of the buffer provider. If the instance does not yet exist, it is created by this
	 * call.
	 */
	public static synchronized BufferProvider getBufferProvider() {

		if (bufferProvider == null) {
			bufferProvider = new BufferProvider();
		}

		return bufferProvider;
	}

	/**
	 * Shuts down the buffer provider.
	 */
	public void shutDown() {

		LOG.info("Shutting down buffer provider");

		// Shut down the file buffer manager
		this.fileBufferManager.shutDown();

		synchronized (this.emptyReadBuffers) {
			if (this.emptyReadBuffers.size() != this.numberOfReadBuffers) {
				LOG.error("Missing " + (this.numberOfReadBuffers - this.emptyReadBuffers.size())
					+ " read buffers during shutdown");
			}

			this.emptyReadBuffers.clear();
		}

		synchronized (this.emptyWriteBuffers) {
			if (this.emptyWriteBuffers.size() != this.numberOfWriteBuffers) {
				LOG.error("Missing " + (this.numberOfWriteBuffers - this.emptyWriteBuffers.size())
					+ " write buffers during shutdown");
			}

			this.emptyWriteBuffers.clear();
		}

		// Suggest triggering of garbage collection
		System.gc();
	}
}
