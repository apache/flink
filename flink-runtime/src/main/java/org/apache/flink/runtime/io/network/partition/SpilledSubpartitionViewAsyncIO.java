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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * View over a spilled subpartition.
 *
 * <p> Reads are triggered asynchronously in batches of configurable size.
 */
class SpilledSubpartitionViewAsyncIO implements ResultSubpartitionView {

	private final static int DEFAULT_READ_BATCH_SIZE = 2;

	private final Object lock = new Object();

	/** The subpartition this view belongs to. */
	private final ResultSubpartition parent;

	/** The buffer provider to get the buffer read everything into. */
	private final BufferProvider bufferProvider;

	/** The buffer availability listener to be notified on available buffers. */
	private final BufferProviderCallback bufferAvailabilityListener;

	/** The size of read batches. */
	private final int readBatchSize;

	/**
	 * The size of the current batch (>= 0 and <= the configured batch size). Reads are only
	 * triggered when the size of the current batch is 0.
	 */
	private final AtomicInteger currentBatchSize = new AtomicInteger();

	/** The asynchronous file reader to do the actual I/O. */
	private final BufferFileReader asyncFileReader;

	/** The buffers, which have been returned from the file reader. */
	private final ConcurrentLinkedQueue<Buffer> returnedBuffers = new ConcurrentLinkedQueue<Buffer>();

	/** A data availability listener. */
	private NotificationListener registeredListener;

	/** Error, which has occurred in the I/O thread. */
	private volatile IOException errorInIOThread;

	/** Flag indicating whether all resources have been released. */
	private volatile boolean isReleased;

	/** Flag indicating whether we reached EOF at the file reader. */
	private volatile boolean hasReachedEndOfFile;

	SpilledSubpartitionViewAsyncIO(
			ResultSubpartition parent,
			BufferProvider bufferProvider,
			IOManager ioManager,
			FileIOChannel.ID channelId,
			long initialSeekPosition) throws IOException {

		this(parent, bufferProvider, ioManager, channelId, initialSeekPosition, DEFAULT_READ_BATCH_SIZE);
	}

	SpilledSubpartitionViewAsyncIO(
			ResultSubpartition parent,
			BufferProvider bufferProvider,
			IOManager ioManager,
			FileIOChannel.ID channelId,
			long initialSeekPosition,
			int readBatchSize) throws IOException {

		checkArgument(initialSeekPosition >= 0, "Initial seek position is < 0.");
		checkArgument(readBatchSize >= 1, "Batch read size < 1.");

		this.parent = checkNotNull(parent);
		this.bufferProvider = checkNotNull(bufferProvider);
		this.bufferAvailabilityListener = new BufferProviderCallback(this);

		this.asyncFileReader = ioManager.createBufferFileReader(channelId, new IOThreadCallback(this));

		if (initialSeekPosition > 0) {
			asyncFileReader.seekToPosition(initialSeekPosition);
		}

		this.readBatchSize = readBatchSize;

		// Trigger the initial read requests
		readNextBatchAsync();
	}

	@Override
	public Buffer getNextBuffer() throws IOException {
		checkError();

		final Buffer buffer = returnedBuffers.poll();

		// No buffer returned from the I/O thread currently. Either the current batch is in progress
		// or we trigger the next one.
		if (buffer == null) {
			if (currentBatchSize.get() == 0) {
				readNextBatchAsync();
			}
		}
		else {
			currentBatchSize.decrementAndGet();
		}

		return buffer;
	}

	@Override
	public boolean registerListener(NotificationListener listener) throws IOException {
		checkNotNull(listener);

		checkError();

		synchronized (lock) {
			if (isReleased || !returnedBuffers.isEmpty()) {
				return false;
			}

			if (registeredListener == null) {
				registeredListener = listener;

				return true;
			}
		}

		throw new IllegalStateException("Already registered listener.");
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		parent.onConsumedSubpartition();
	}

	@Override
	public void releaseAllResources() throws IOException {
		try {
			synchronized (lock) {
				if (!isReleased) {
					// Recycle all buffers. Buffers, which are in flight are recycled as soon as
					// they return from the I/O thread.
					Buffer buffer;
					while ((buffer = returnedBuffers.poll()) != null) {
						buffer.recycle();
					}

					isReleased = true;
				}
			}
		}
		finally {
			asyncFileReader.close();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	/**
	 * Requests buffers from the buffer provider and triggers asynchronous read requests to fill
	 * them.
	 *
	 * <p> The number of requested buffers/triggered I/O read requests per call depends on the
	 * configured size of batch reads.
	 */
	private void readNextBatchAsync() throws IOException {
		// This does not need to be fully synchronized with actually reaching EOF as long as
		// we eventually notice it. In the worst case, we trigger some discarded reads and
		// notice it when the buffers are returned.
		//
		// We only trigger reads if the current batch size is 0.
		if (hasReachedEndOfFile || currentBatchSize.get() != 0) {
			return;
		}

		// Number of successful buffer requests or callback registrations. The call back will
		// trigger the read as soon as a buffer becomes available again.
		int i = 0;

		while (i < readBatchSize) {
			final Buffer buffer = bufferProvider.requestBuffer();

			if (buffer == null) {
				// Listen for buffer availability.
				currentBatchSize.incrementAndGet();

				if (bufferProvider.addListener(bufferAvailabilityListener)) {
					i++;
				}
				else if (bufferProvider.isDestroyed()) {
					currentBatchSize.decrementAndGet();
					return;
				}
				else {
					// Buffer available again
					currentBatchSize.decrementAndGet();
				}
			}
			else {
				currentBatchSize.incrementAndGet();

				asyncFileReader.readInto(buffer);
			}
		}
	}

	/**
	 * Returns a buffer from the buffer provider.
	 *
	 * <p> Note: This method is called from the thread recycling the available buffer.
	 */
	private void onAvailableBuffer(Buffer buffer) {
		try {
			asyncFileReader.readInto(buffer);
		}
		catch (IOException e) {
			notifyError(e);
		}
	}

	/**
	 * Returns a successful buffer read request.
	 *
	 * <p> Note: This method is always called from the same I/O thread.
	 */
	private void returnBufferFromIOThread(Buffer buffer) {
		final NotificationListener listener;

		synchronized (lock) {
			if (hasReachedEndOfFile || isReleased) {
				buffer.recycle();

				return;
			}

			returnedBuffers.add(buffer);

			listener = registeredListener;
			registeredListener = null;

			// If this was the last buffer before we reached EOF, set the corresponding flag to
			// ensure that further buffers are correctly recycled and eventually no further reads
			// are triggered.
			if (asyncFileReader.hasReachedEndOfFile()) {
				hasReachedEndOfFile = true;
			}
		}

		if (listener != null) {
			listener.onNotification();
		}
	}

	/**
	 * Notifies the view about an error.
	 */
	private void notifyError(IOException error) {
		if (errorInIOThread == null) {
			errorInIOThread = error;
		}

		final NotificationListener listener;

		synchronized (lock) {
			listener = registeredListener;
			registeredListener = null;
		}

		if (listener != null) {
			listener.onNotification();
		}
	}

	/**
	 * Checks whether an error has been reported and rethrow the respective Exception, if available.
	 */
	private void checkError() throws IOException {
		if (errorInIOThread != null) {
			throw errorInIOThread;
		}
	}

	/**
	 * Callback from the I/O thread.
	 *
	 * <p> Successful buffer read requests add the buffer to the subpartition view, and failed ones
	 * notify about the error.
	 */
	private static class IOThreadCallback implements RequestDoneCallback<Buffer> {

		private final SpilledSubpartitionViewAsyncIO subpartitionView;

		public IOThreadCallback(SpilledSubpartitionViewAsyncIO subpartitionView) {
			this.subpartitionView = subpartitionView;
		}

		@Override
		public void requestSuccessful(Buffer buffer) {
			subpartitionView.returnBufferFromIOThread(buffer);
		}

		@Override
		public void requestFailed(Buffer buffer, IOException error) {
			// Recycle the buffer and forward the error
			buffer.recycle();

			subpartitionView.notifyError(error);
		}
	}

	/**
	 * Callback from the buffer provider.
	 */
	private static class BufferProviderCallback implements EventListener<Buffer> {

		private final SpilledSubpartitionViewAsyncIO subpartitionView;

		private BufferProviderCallback(SpilledSubpartitionViewAsyncIO subpartitionView) {
			this.subpartitionView = subpartitionView;
		}

		@Override
		public void onEvent(Buffer buffer) {
			if (buffer == null) {
				return;
			}

			subpartitionView.onAvailableBuffer(buffer);
		}
	}
}
